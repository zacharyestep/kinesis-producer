package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	k "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/jpillora/backoff"
)

type Work struct {
	records []*AggregatedRecordRequest
	size    int
	reason  string
	b       *backoff.Backoff
}

func NewWork(records []*AggregatedRecordRequest, size int, reason string) *Work {
	return &Work{
		records: records,
		size:    size,
		reason:  reason,
		b: &backoff.Backoff{
			Jitter: true,
		},
	}
}

type WorkerPool struct {
	*Config
	input      chan *AggregatedRecordRequest
	unfinished chan []*AggregatedRecordRequest
	flush      chan struct{}
	pause      chan struct{}
	done       chan struct{}
	errs       chan error
}

func NewWorkerPool(config *Config) *WorkerPool {
	return &WorkerPool{
		Config:     config,
		input:      make(chan *AggregatedRecordRequest),
		unfinished: make(chan []*AggregatedRecordRequest),
		flush:      make(chan struct{}),
		pause:      make(chan struct{}),
		done:       make(chan struct{}),
		errs:       make(chan error),
	}
}

func (wp *WorkerPool) Start() {
	go wp.loop()
}

func (wp *WorkerPool) Errors() chan error {
	return wp.errs
}

func (wp *WorkerPool) Add(record *AggregatedRecordRequest) {
	wp.input <- record
}

func (wp *WorkerPool) Pause() []*AggregatedRecordRequest {
	wp.pause <- struct{}{}
	return <-wp.unfinished
}

func (wp *WorkerPool) Resume(records []*AggregatedRecordRequest) {
	wp.unfinished <- records
	<-wp.pause
}

func (wp *WorkerPool) Wait() {
	<-wp.done
	close(wp.errs)
}

func (wp *WorkerPool) Flush() {
	wp.flush <- struct{}{}
}

func (wp *WorkerPool) Close() {
	close(wp.input)
}

func (wp *WorkerPool) loop() {
	var (
		buf                   = make([]*AggregatedRecordRequest, 0, wp.BatchCount)
		inflight    []*Work   = nil
		retry                 = make(chan *Work)
		size                  = 0
		connections semaphore = make(chan struct{}, wp.MaxConnections)
		closed      semaphore = make(chan struct{}, wp.MaxConnections)
	)

	// create new work item from buffer and append to inflight work
	flushBuf := func(reason string) {
		if size == 0 {
			return
		}
		work := NewWork(buf, size, reason)
		buf = make([]*AggregatedRecordRequest, 0, wp.BatchCount)
		size = 0
		inflight = append(inflight, work)
	}

	// Push aggregated record into the buffer. Flush buffer into new work item if push will
	// exceed size limits
	push := func(record *AggregatedRecordRequest) {
		rsize := len(record.Entry.Data) + len([]byte(*record.Entry.PartitionKey))
		if size+rsize > wp.BatchSize {
			// if this record would overflow the batch buffer, send it inflight
			flushBuf("batch size")
		}
		buf = append(buf, record)
		size += rsize
		if len(buf) >= wp.BatchCount {
			flushBuf("batch length")
		}
	}

	// prepend work item to start of inflight buffer. Work that needs to be retried is
	// prepended for prioritization over new work
	prepend := func(work *Work) {
		inflight = append([]*Work{work}, inflight...)
	}

	do := func(work *Work) {
		failed := wp.send(work)
		if failed != nil {
			retry <- failed
		}
		connections.release()
	}

	var (
		flush     chan struct{}                 = wp.flush
		pause     chan struct{}                 = wp.pause
		input     chan *AggregatedRecordRequest = wp.input
		completed int
	)

	// fill up the closed connection semaphore before starting the loop so that when
	// connections are closed after stopping, the loop can exit when all have closed
	closed.wait(wp.MaxConnections)

	defer close(wp.done)

	for {
		select {
		case record, ok := <-input:
			if !ok {
				input = nil
				flushBuf("drain")
			} else {
				push(record)
			}
		case <-flush:
			flushBuf("flush interval")
		case connections <- struct{}{}:
			// acquired an open connection
			// check to see if there is any work in flight that needs to be sent
			var work *Work
			if len(inflight) > 0 {
				work, inflight = inflight[0], inflight[1:]
			}

			if work != nil {
				go do(work)
			} else if input == nil {
				// If input is nil, no more work will be coming so close the connection for good
				closed.release()
			} else {
				// otherwise release it
				connections.release()
			}
		case closed <- struct{}{}:
			// this case will block until the connections case releases the closed semaphore
			completed++
			if completed == wp.MaxConnections {
				return
			}
		case failed := <-retry:
			// prioritize work that needs to be resent due to throttling
			prepend(failed)
		case <-pause:
			// collect failed records that need retry from open connections
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for failed := range retry {
					prepend(failed)
				}
			}()
			// wait for open connections to finish
			connections.wait(wp.MaxConnections - completed)
			// safe to close retry channel now that no connections are open
			close(retry)
			// wait to finish collecting all failed requests
			wg.Wait()
			// flush out anything remaining in the buffer
			flushBuf("pause")
			// capture the inflight requests that did not get finished
			var drained []*AggregatedRecordRequest
			for _, work := range inflight {
				drained = append(drained, work.records...)
			}
			// reset state
			retry = make(chan *Work)
			inflight = nil
			// send the drained records
			wp.unfinished <- drained
			// reset closed connections
			closed.wait(completed)
			completed = 0
			// reopen connections
			connections.open(wp.MaxConnections)
			// collect records to push after resuming
			// this will block the pool until Resume() is called
			records := <-wp.unfinished
			for _, record := range records {
				push(record)
			}
			if input == nil {
				// if the pool was paused after Close(), then we want to flush any remaining buffer
				flushBuf("drain")
			}
			wp.pause <- struct{}{}
		}
	}
}

func (wp *WorkerPool) send(work *Work) *Work {
	count := len(work.records)
	wp.Logger.Info("flushing records", LogValue{"reason", work.reason}, LogValue{"records", count})

	kinesisRecords := make([]types.PutRecordsRequestEntry, count)
	for i := 0; i < count; i++ {
		kinesisRecords[i] = work.records[i].Entry
	}

	out, err := wp.Client.PutRecords(context.Background(), &k.PutRecordsInput{
		StreamName: &wp.StreamName,
		Records:    kinesisRecords,
	})

	if err != nil {
		wp.Logger.Error("send", err)
		for _, r := range work.records {
			failure := &FailureRecord{
				Err:          err,
				PartitionKey: *r.Entry.PartitionKey,
				UserRecords:  r.UserRecords,
			}
			if r.Entry.ExplicitHashKey != nil {
				failure.ExplicitHashKey = *r.Entry.ExplicitHashKey
			}
			wp.errs <- failure
		}
		return nil
	}

	if wp.Verbose {
		for i, r := range out.Records {
			values := make([]LogValue, 2)
			if r.ErrorCode != nil {
				values[0] = LogValue{"ErrorCode", *r.ErrorCode}
				values[1] = LogValue{"ErrorMessage", *r.ErrorMessage}
			} else {
				values[0] = LogValue{"ShardId", *r.ShardId}
				values[1] = LogValue{"SequenceNumber", *r.SequenceNumber}
			}
			wp.Logger.Info(fmt.Sprintf("Result[%d]", i), values...)
		}
	}

	failed := *out.FailedRecordCount
	if failed == 0 {
		return nil
	}

	duration := work.b.Duration()

	wp.Logger.Info(
		"put failures",
		LogValue{"failures", failed},
		LogValue{"backoff", duration.String()},
	)
	time.Sleep(duration)

	// change the logging state for the next itertion
	work.reason = "retry"
	work.records = failures(work.records, out.Records, failed)
	return work
}

// failures returns the failed records as indicated in the response.
func failures(
	records []*AggregatedRecordRequest,
	response []types.PutRecordsResultEntry,
	count int32,
) []*AggregatedRecordRequest {
	out := make([]*AggregatedRecordRequest, 0, count)
	for i, record := range response {
		if record.ErrorCode != nil {
			out = append(out, records[i])
		}
	}
	return out
}
