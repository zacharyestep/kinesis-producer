// Amazon kinesis producer
// A KPL-like batch producer for Amazon Kinesis built on top of the official Go AWS SDK
// and using the same aggregation format that KPL use.
//
// Note: this project start as a fork of `tj/go-kinesis`. if you are not intersting in the
// KPL aggregation logic, you probably want to check it out.
package producer

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/jpillora/backoff"
)

// Errors
var (
	ErrStoppedProducer     = errors.New("Unable to Put record. Producer is already stopped")
	ErrIllegalPartitionKey = errors.New("Invalid parition key. Length must be at least 1 and at most 256")
	ErrRecordSizeExceeded  = errors.New("Data must be less than or equal to 1MB in size")
)

// Producer batches records.
type Producer struct {
	sync.RWMutex
	*Config
	shardMap  *ShardMap
	semaphore semaphore
	records   chan *AggregatedRecordRequest
	failure   chan *FailureRecord
	done      chan struct{}

	// Current state of the Producer
	// notify set to true after calling to `NotifyFailures`
	notify bool
	// stopped set to true after `Stop`ing the Producer.
	// This will prevent from user to `Put` any new data.
	stopped bool
}

// New creates new producer with the given config.
func New(config *Config) *Producer {
	config.defaults()
	p := &Producer{
		Config:    config,
		done:      make(chan struct{}),
		records:   make(chan *AggregatedRecordRequest, config.BacklogCount),
		semaphore: make(chan struct{}, config.MaxConnections),
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
	p.RLock()
	stopped := p.stopped
	p.RUnlock()
	if stopped {
		return ErrStoppedProducer
	}

	recordSize := userRecord.Size()
	if userRecord.Size() > maxRecordSize {
		return ErrRecordSizeExceeded
	}

	partitionKey := userRecord.PartitionKey()
	if l := len(partitionKey); l < 1 || l > 256 {
		return ErrIllegalPartitionKey
	}

	nbytes := recordSize + len([]byte(partitionKey))
	// if the record size is bigger than aggregation size
	// handle it as a simple kinesis record
	if nbytes > p.AggregateBatchSize {
		p.records <- NewAggregatedRecordRequest(userRecord.Data(), &partitionKey, nil, []UserRecord{userRecord})
	} else {
		drained, err := p.shardMap.Put(userRecord)
		switch err.(type) {
		case nil:
		case ShardBucketError:
			return err
		default:
			p.Logger.Error("drain aggregator", err)
		}
		if drained != nil {
			p.records <- drained
		}
	}
	return nil
}

// Failure record type
type FailureRecord struct {
	Err error
	// The PartitionKey that was used in the kinesis.PutRecordsRequestEntry
	PartitionKey string
	// The ExplicitHashKey that was used in the kinesis.PutRecordsRequestEntry. Will be the
	// empty string if nil
	ExplicitHashKey string
	// UserRecords that were contained in the failed aggregated record request
	UserRecords []UserRecord
}

// NotifyFailures registers and return listener to handle undeliverable messages.
// The incoming struct has a copy of the Data and the PartitionKey along with some
// error information about why the publishing failed.
func (p *Producer) NotifyFailures() <-chan *FailureRecord {
	p.Lock()
	defer p.Unlock()
	if !p.notify {
		p.notify = true
		p.failure = make(chan *FailureRecord, p.BacklogCount)
	}
	return p.failure
}

// Start the producer
func (p *Producer) Start() {
	p.Logger.Info("starting producer", LogValue{"stream", p.StreamName})
	go p.loop()
}

// Stop the producer gracefully. Flushes any in-flight data.
func (p *Producer) Stop() {
	p.Lock()
	p.stopped = true
	p.Unlock()
	p.Logger.Info("stopping producer", LogValue{"backlog", len(p.records)})

	// drain
	records := p.drainIfNeed()
	for _, record := range records {
		p.records <- record
	}
	p.done <- struct{}{}
	close(p.records)

	// wait
	<-p.done
	p.semaphore.wait()

	// close the failures channel if we notify
	p.RLock()
	if p.notify {
		close(p.failure)
	}
	p.RUnlock()
	p.Logger.Info("stopped producer")
}

// loop and flush at the configured interval, or when the buffer is exceeded.
func (p *Producer) loop() {
	var (
		drain                       = false
		size                        = 0
		buf                         = make([]*AggregatedRecordRequest, 0, p.BatchCount)
		flushTick                   = time.NewTicker(p.FlushInterval)
		shardTick  *time.Ticker     = nil
		shardTickC <-chan time.Time = nil
	)

	if p.ShardRefreshInterval != 0 {
		shardTick = time.NewTicker(p.ShardRefreshInterval)
		shardTickC = shardTick.C
	}

	flush := func(msg string) {
		p.semaphore.acquire()
		go p.flush(buf, msg)
		size = 0
		buf = make([]*AggregatedRecordRequest, 0, p.BatchCount)
	}

	bufAppend := func(record *AggregatedRecordRequest) {
		// the record size limit applies to the total size of the
		// partition key and data blob.
		rsize := len(record.Entry.Data) + len([]byte(*record.Entry.PartitionKey))
		if size+rsize > p.BatchSize {
			flush("batch size")
		}
		size += rsize
		buf = append(buf, record)
		if len(buf) >= p.BatchCount {
			flush("batch length")
		}
	}

	defer flushTick.Stop()
	if shardTick != nil {
		defer shardTick.Stop()
	}
	defer close(p.done)

	for {
		select {
		case record, ok := <-p.records:
			if drain && !ok {
				if size > 0 {
					flush("drain")
				}
				p.Logger.Info("backlog drained")
				return
			}
			bufAppend(record)
		case <-flushTick.C:
			records := p.drainIfNeed()
			for _, record := range records {
				bufAppend(record)
			}
			// if the buffer is still containing records
			if size > 0 {
				flush("interval")
			}
		case <-shardTickC:
			records, err := p.shardMap.UpdateShards(p.GetShards)
			if err != nil {
				p.Logger.Error("UpdateShards error", err)
				continue
			}
			for _, record := range records {
				bufAppend(record)
			}
		case <-p.done:
			drain = true
		}
	}
}

func (p *Producer) drainIfNeed() []*AggregatedRecordRequest {
	if p.shardMap.Size() == 0 {
		return nil
	}
	records, errs := p.shardMap.Drain()
	for _, err := range errs {
		p.Logger.Error("drain aggregator", err)
	}
	return records
}

// flush records and retry failures if necessary.
// for example: when we get "ProvisionedThroughputExceededException"
func (p *Producer) flush(records []*AggregatedRecordRequest, reason string) {
	b := &backoff.Backoff{
		Jitter: true,
	}

	defer p.semaphore.release()

	for {
		count := len(records)
		p.Logger.Info("flushing records", LogValue{"reason", reason}, LogValue{"records", count})

		kinesisRecords := make([]*kinesis.PutRecordsRequestEntry, count)
		for i := 0; i < count; i++ {
			kinesisRecords[i] = records[i].Entry
		}

		out, err := p.Client.PutRecords(&kinesis.PutRecordsInput{
			StreamName: &p.StreamName,
			Records:    kinesisRecords,
		})

		if err != nil {
			p.Logger.Error("flush", err)
			p.RLock()
			notify := p.notify
			p.RUnlock()
			if notify {
				p.dispatchFailures(records, err)
			}
			return
		}

		if p.Verbose {
			for i, r := range out.Records {
				values := make([]LogValue, 2)
				if r.ErrorCode != nil {
					values[0] = LogValue{"ErrorCode", *r.ErrorCode}
					values[1] = LogValue{"ErrorMessage", *r.ErrorMessage}
				} else {
					values[0] = LogValue{"ShardId", *r.ShardId}
					values[1] = LogValue{"SequenceNumber", *r.SequenceNumber}
				}
				p.Logger.Info(fmt.Sprintf("Result[%d]", i), values...)
			}
		}

		failed := *out.FailedRecordCount
		if failed == 0 {
			return
		}

		duration := b.Duration()

		p.Logger.Info(
			"put failures",
			LogValue{"failures", failed},
			LogValue{"backoff", duration.String()},
		)
		time.Sleep(duration)

		// change the logging state for the next itertion
		reason = "retry"
		records = failures(records, out.Records, failed)
	}
}

// dispatchFailures gets batch of records, extract them, and push them
// into the failure channel
func (p *Producer) dispatchFailures(records []*AggregatedRecordRequest, err error) {
	for _, r := range records {
		failure := &FailureRecord{
			Err:          err,
			PartitionKey: *r.Entry.PartitionKey,
			UserRecords:  r.UserRecords,
		}
		if r.Entry.ExplicitHashKey != nil {
			failure.ExplicitHashKey = *r.Entry.ExplicitHashKey
		}
		p.failure <- failure
	}
}

// failures returns the failed records as indicated in the response.
func failures(
	records []*AggregatedRecordRequest,
	response []*kinesis.PutRecordsResultEntry,
	count int64,
) []*AggregatedRecordRequest {
	out := make([]*AggregatedRecordRequest, 0, count)
	for i, record := range response {
		if record.ErrorCode != nil {
			out = append(out, records[i])
		}
	}
	return out
}
