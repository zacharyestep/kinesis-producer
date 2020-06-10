package producer

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	k "github.com/aws/aws-sdk-go/service/kinesis"
)

type responseMock struct {
	Response *k.PutRecordsOutput
	Error    error
}

type clientMock struct {
	calls     int
	responses []responseMock
	incoming  map[int][]string
}

func (c *clientMock) PutRecords(input *k.PutRecordsInput) (*k.PutRecordsOutput, error) {
	res := c.responses[c.calls]
	for _, r := range input.Records {
		c.incoming[c.calls] = append(c.incoming[c.calls], *r.PartitionKey)
	}
	c.calls++
	if res.Error != nil {
		return nil, res.Error
	}
	return res.Response, nil
}

type testCase struct {
	// configuration
	name    string      // test name
	config  *Config     // test config
	records []string    // all outgoing records(partition keys and data too)
	putter  *clientMock // mocked client

	// expectations
	outgoing map[int][]string // [call number][partition keys]
}

func genBulk(n int, s string) (ret []string) {
	for i := 0; i < n; i++ {
		ret = append(ret, s)
	}
	return
}

var testCases = []testCase{
	{
		"one record with batch count 1",
		&Config{BatchCount: 1},
		[]string{"hello"},
		&clientMock{
			incoming: make(map[int][]string),
			responses: []responseMock{
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
					},
				},
			}},
		map[int][]string{
			0: []string{"hello"},
		},
	},
	{
		"two records with batch count 1",
		&Config{BatchCount: 1, AggregateBatchCount: 1},
		[]string{"hello", "world"},
		&clientMock{
			incoming: make(map[int][]string),
			responses: []responseMock{
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
					},
				},
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
					},
				},
			}},
		map[int][]string{
			0: []string{"hello"},
			1: []string{"world"},
		},
	},
	{
		"two records with batch count 2, simulating retries",
		&Config{BatchCount: 2, AggregateBatchCount: 1},
		[]string{"hello", "world"},
		&clientMock{
			incoming: make(map[int][]string),
			responses: []responseMock{
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(1),
						Records: []*k.PutRecordsResultEntry{
							{SequenceNumber: aws.String("3"), ShardId: aws.String("1")},
							{ErrorCode: aws.String("400")},
						},
					},
				},
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
					},
				},
			}},
		map[int][]string{
			0: []string{"hello", "world"},
			1: []string{"world"},
		},
	},
	{
		"2 bulks of 10 records",
		&Config{BatchCount: 10, AggregateBatchCount: 1, BacklogCount: 1},
		genBulk(20, "foo"),
		&clientMock{
			incoming: make(map[int][]string),
			responses: []responseMock{
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
					},
				},
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
					},
				},
			}},
		map[int][]string{
			0: genBulk(10, "foo"),
			1: genBulk(10, "foo"),
		},
	},
}

func TestProducer(t *testing.T) {
	for _, test := range testCases {
		test.config.StreamName = test.name
		test.config.MaxConnections = 1
		test.config.Client = test.putter
		p := New(test.config)
		p.Start()
		var wg sync.WaitGroup
		wg.Add(len(test.records))
		for _, r := range test.records {
			go func(s string) {
				p.Put([]byte(s), s)
				wg.Done()
			}(r)
		}
		wg.Wait()
		p.Stop()
		for k, v := range test.putter.incoming {
			if len(v) != len(test.outgoing[k]) {
				t.Errorf("failed test: %s\n\texcpeted:%v\n\tactual:  %v", test.name,
					test.outgoing, test.putter.incoming)
			}
		}
	}
}

func TestNotify(t *testing.T) {
	kError := errors.New("ResourceNotFoundException: Stream foo under account X not found")
	p := New(&Config{
		StreamName:          "foo",
		MaxConnections:      1,
		BatchCount:          1,
		AggregateBatchCount: 10,
		Client: &clientMock{
			incoming:  make(map[int][]string),
			responses: []responseMock{{Error: kError}},
		},
	})
	p.Start()
	records := genBulk(10, "bar")
	failures := p.NotifyFailures()
	failed := 0
	done := make(chan struct{})
	// timeout test after 10 seconds
	timeout := time.After(10 * time.Second)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case failure, ok := <-failures:
				if !ok {
					// expect producer close the failures channel
					close(done)
					return
				}
				failed += len(failure.UserRecords)
			case <-timeout:
				return
			}
		}
	}()
	for _, r := range records {
		p.Put([]byte(r), r)
	}
	p.Stop()
	wg.Wait()

	if failed != len(records) {
		t.Errorf("failed test: NotifyFailure\n\texcpeted:%v\n\tactual:%v", failed, len(records))
	}

	select {
	case <-done:
	default:
		t.Error("failed test: NotifyFailure\n\texpect failures channel to be closed")
	}
}

type mockBenchmarkClient struct {
	b *testing.B
}

func (c *mockBenchmarkClient) PutRecords(*k.PutRecordsInput) (*k.PutRecordsOutput, error) {
	failed := int64(0)
	return &k.PutRecordsOutput{
		FailedRecordCount: &failed,
	}, nil
}

func BenchmarkProducer(b *testing.B) {
	testCases := []struct {
		name   string
		config *Config
	}{
		{
			name: "default producer",
			config: &Config{
				StreamName: "default producer",
			},
		},
		{
			name: "10 shard count",
			config: &Config{
				StreamName: "10 shard count",
				GetShards:  StaticGetShardsFunc(10),
			},
		},
		{
			name: "500 shard count",
			config: &Config{
				StreamName: "500 shard count",
				GetShards:  StaticGetShardsFunc(500),
			},
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			tc.config.Client = &mockBenchmarkClient{
				b: b,
			}
			tc.config.Logger = &NopLogger{}

			p := New(tc.config)

			failures := p.NotifyFailures()
			failuresDone := make(chan struct{})
			go func() {
				defer close(failuresDone)
				for f := range failures {
					b.Fatal(f.Err)
				}
			}()

			workerWG := new(sync.WaitGroup)
			workers := runtime.NumCPU()
			each := b.N / workers
			workerWG.Add(workers)

			records := make([]UserRecord, b.N)
			for i := 0; i < b.N; i++ {
				r, err := newMyExampleUserRecord("foo", "bar")
				if err != nil {
					b.Fatal(err)
				}
				records[i] = r
			}

			p.Start()

			b.ResetTimer()

			for i := 0; i < workers; i++ {
				go func(index int) {
					for j := 0; j < each; j++ {
						record := records[index*each+j]
						err := p.PutUserRecord(record)
						if err != nil {
							b.Fatal(err)
						}
					}
					workerWG.Done()
				}(i)
			}
			workerWG.Wait()
			p.Stop()
			<-failuresDone
		})
	}
}
