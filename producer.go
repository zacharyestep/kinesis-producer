// Amazon kinesis producer
// A KPL-like batch producer for Amazon Kinesis built on top of the official Go AWS SDK
// and using the same aggregation format that KPL use.
//
// Note: this project start as a fork of `tj/go-kinesis`. if you are not intersting in the
// KPL aggregation logic, you probably want to check it out.
package producer

import (
	"sync"
	"time"
)

type Producer struct {
	sync.RWMutex
	*Config

	shardMap *ShardMap

	// semaphore controling size of Put backlog before blocking
	backlog semaphore

	pool *WorkerPool

	// stopped signals that the producer is no longer accepting Puts
	stopped chan struct{}

	// signal for the main loop that stop has been called and it should drain the backlog
	done chan struct{}

	failures chan error
}

func New(config *Config) *Producer {
	config.defaults()
	p := &Producer{
		Config:  config,
		backlog: make(chan struct{}, config.BacklogCount),
		pool:    NewWorkerPool(config),
		stopped: make(chan struct{}),
		done:    make(chan struct{}),
	}
	shards, _, err := p.GetShards(nil)
	if err != nil {
		// TODO: maybe just log and continue or fallback to default? if ShardRefreshInterval
		// 			 is set, it may succeed a later time
		panic(err)
	}
	p.shardMap = NewShardMap(shards, p.AggregateBatchCount)
	return p
}

// Put `data` using `partitionKey` asynchronously. This method is thread-safe.
//
// Under the covers, the Producer will automatically re-attempt puts in case of
// transient errors.
// When unrecoverable error has detected(e.g: trying to put to in a stream that
// doesn't exist), the message will returned by the Producer.
// Add a listener with `Producer.NotifyFailures` to handle undeliverable messages.
func (p *Producer) Put(data []byte, partitionKey string) error {
	return p.PutUserRecord(NewDataRecord(data, partitionKey))
}

func (p *Producer) PutUserRecord(userRecord UserRecord) error {
	select {
	case <-p.stopped:
		return userRecord.(*ErrStoppedProducer)
	// same as p.backlog.acquire() but using channel primative for select case
	case p.backlog <- struct{}{}:
	}

	var release = true
	defer func() {
		if release {
			p.backlog.release()
		}
	}()

	partitionKey := userRecord.PartitionKey()
	partitionKeySize := len(partitionKey)
	if partitionKeySize < 1 || partitionKeySize > 256 {
		return userRecord.(*ErrIllegalPartitionKey)
	}

	// Kinesis counts partition key size towards size limits
	recordSize := userRecord.Size() + partitionKeySize
	if recordSize > maxRecordSize {
		return userRecord.(*ErrRecordSizeExceeded)
	}

	var (
		record *AggregatedRecordRequest
		err    error
	)
	// if the record size is bigger than aggregation size
	// handle it as a simple kinesis record
	// TODO: this logic is not enforced when doing reaggreation after shard refresh
	if recordSize > p.AggregateBatchSize {
		record = NewAggregatedRecordRequest(userRecord.Data(), &partitionKey, nil, []UserRecord{userRecord})
	} else {
		record, err = p.shardMap.Put(userRecord)
	}

	if record != nil {
		// if we are going to send a record over the records channel
		// we hold the semaphore until that record has been sent
		// this way we can rely on p.backlog.wait() to mean all waiting puts complete and
		// future puts are blocked
		release = false
		go func() {
			p.pool.Add(record)
			p.backlog.release()
		}()
	}

	return err
}

func (p *Producer) Start() {
	poolErrs := p.pool.Errors()
	// listen for errors from the worker pool p.notify() will send on the failures
	// channel if p.NotifyFailures() has been called
	go func() {
		for err := range poolErrs {
			p.notify(err)
		}
		// we can close p.failure after pool error channel has closed
		// because
		p.Lock()
		if p.failures != nil {
			close(p.failures)
			p.failures = nil
		}
		p.Unlock()
	}()
	p.pool.Start()
	go p.loop()
}

func (p *Producer) Stop() {
	// signal to stop any future Puts
	close(p.stopped)
	// signal to main loop to begin cleanup process
	p.done <- struct{}{}
	// wait for the worker pool to complete
	p.pool.Wait()
	// send another signal to main loop to exit
	p.done <- struct{}{}
	<-p.done
}

// NotifyFailures registers and return listener to handle undeliverable messages.
// The incoming struct has a copy of the Data and the PartitionKey along with some
// error information about why the publishing failed.
func (p *Producer) NotifyFailures() <-chan error {
	p.Lock()
	defer p.Unlock()
	if p.failures == nil {
		p.failures = make(chan error, p.BacklogCount)
	}
	return p.failures
}

func (p *Producer) loop() {
	var (
		stop       chan struct{}
		done       chan struct{}    = p.done
		flushTick  *time.Ticker     = time.NewTicker(p.FlushInterval)
		flushTickC <-chan time.Time = flushTick.C
		shardTick  *time.Ticker
		shardTickC <-chan time.Time
	)

	if p.ShardRefreshInterval != 0 {
		shardTick = time.NewTicker(p.ShardRefreshInterval)
		shardTickC = shardTick.C
		defer shardTick.Stop()
	}

	defer flushTick.Stop()
	defer close(p.done)

	flush := func() {
		records := p.drain()
		for _, record := range records {
			p.pool.Add(record)
		}
		p.pool.Flush()
	}

	for {
		select {
		case <-flushTickC:
			flush()
		case <-shardTickC:
			err := p.updateShards(done == nil)
			if err != nil {
				p.Logger.Error("UpdateShards error", err)
				p.notify(err)
			}
		case <-done:
			// after waiting for the pool to finish, Stop() will send another signal to the done
			// channel, the second time signaling its safe to end this go routine
			stop, done = done, nil
			// once we are done we no longer need flush tick as we are already
			// flushing the backlog
			flushTickC = nil
			// block any more puts from happening
			p.backlog.wait(p.BacklogCount)
			// backlog is flushed and no more records are incomming
			// flush any remaining records in the aggregator
			flush()
			// with puts blocked and flush complete, we can close input channel safely
			p.pool.Close()
		case <-stop:
			return
		}
	}
}

func (p *Producer) updateShards(done bool) error {
	old := p.shardMap.Shards()
	shards, updated, err := p.GetShards(old)
	if err != nil {
		return err
	}
	if !updated {
		return nil
	}

	if !done {
		// if done signal has not been received yet, flush all backlogged puts into the worker
		// pool and block additional puts
		p.backlog.wait(p.BacklogCount)
	}

	// pause and drain the worker pool
	pending := p.pool.Pause()

	// update the shards and reaggregate pending records
	records, err := p.shardMap.UpdateShards(shards, pending)

	// resume the worker pool
	p.pool.Resume(records)

	if !done {
		// if done signal has not been received yet, re-open the backlog to accept more Puts
		p.backlog.open(p.BacklogCount)
	}

	return err
}

func (p *Producer) drain() []*AggregatedRecordRequest {
	if p.shardMap.Size() == 0 {
		return nil
	}
	records, errs := p.shardMap.Drain()
	if len(errs) > 0 {
		p.notify(errs...)
	}
	return records
}

func (p *Producer) notify(errs ...error) {
	p.RLock()
	if p.failures != nil {
		for _, err := range errs {
			p.failures <- err
		}
	}
	p.RUnlock()
}
