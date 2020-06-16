package producer

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	k "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
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
				failed += len(failure.(*FailureRecord).UserRecords)
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

func mockGetShards(startingShards, shards []*k.Shard, updated bool, err error) GetShardsFunc {
	calls := 0
	return func(_ []*k.Shard) ([]*k.Shard, bool, error) {
		calls++
		switch calls {
		case 1:
			// first call happens on producer creation
			return startingShards, true, nil
		case 2:
			// second call on first refresh
			return shards, updated, err
		default:
			// any subsequent calls, return no update
			return nil, false, nil
		}
	}
}

type mockThrottleClient struct {
	done chan struct{}
}

// Error all records to mock in flight requests retry due to throttles
func (c *mockThrottleClient) PutRecords(input *k.PutRecordsInput) (*k.PutRecordsOutput, error) {
	select {
	case <-c.done:
		failed := int64(0)
		return &k.PutRecordsOutput{
			FailedRecordCount: &failed,
		}, nil
	default:
		fmt.Println("put records throttle")
		failed := int64(len(input.Records))
		code := "errorcode"
		var records []*kinesis.PutRecordsResultEntry
		for range input.Records {
			records = append(records, &kinesis.PutRecordsResultEntry{
				ErrorCode: &code,
			})
		}
		return &k.PutRecordsOutput{
			FailedRecordCount: &failed,
			Records:           records,
		}, nil
	}
}

func TestProducerUpdateShards(t *testing.T) {
	testCases := []struct {
		name                string
		startingShards      string
		aggregateBatchCount int
		records             []UserRecord
		getShardsShards     string
		getShardsUpdated    bool
		getShardsError      string
		updateDrained       []*AggregatedRecordRequest
		expectedError       string
	}{
		{
			name:                "returns error from GetShardsFunc",
			startingShards:      "testdata/TestProducerUpdateShards/error/startingShards.json",
			aggregateBatchCount: 2,
			records: []UserRecord{
				newTestUserRecord("foo", "100141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("bar", "200141183460469231731687303715884105727", []byte("world")),
				newTestUserRecord("foo", "110141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("bar", "210141183460469231731687303715884105727", []byte("world")),
			},
			getShardsError: "getShards error",
			expectedError:  "getShards error",
		},
		{
			name:                "does not drain in flight records",
			startingShards:      "testdata/TestProducerUpdateShards/no_update/startingShards.json",
			aggregateBatchCount: 2,
			records: []UserRecord{
				newTestUserRecord("foo", "100141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("bar", "200141183460469231731687303715884105727", []byte("world")),
				newTestUserRecord("foo", "110141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("bar", "210141183460469231731687303715884105727", []byte("world")),
			},
			getShardsUpdated: false,
		},
		{
			name:                "updates shards and redistributes inflight records",
			startingShards:      "testdata/TestProducerUpdateShards/update/startingShards.json",
			aggregateBatchCount: 1,
			records: []UserRecord{
				newTestUserRecord("foo", "100141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("bar", "200141183460469231731687303715884105727", []byte("world")),
				newTestUserRecord("fuzz", "110141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("buzz", "210141183460469231731687303715884105727", []byte("world")),
			},
			getShardsShards:  "testdata/TestProducerUpdateShards/update/getShardsShards.json",
			getShardsUpdated: true,
			updateDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: &k.PutRecordsRequestEntry{
						// StartingHashKey of first shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("foo", "", []byte("hello")),
					},
				},
				&AggregatedRecordRequest{
					Entry: &k.PutRecordsRequestEntry{
						// StartingHashKey of second shard
						ExplicitHashKey: aws.String("170141183460469231731687303715884105728"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("bar", "", []byte("world")),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				startingShards  []*k.Shard
				getShardsShards []*k.Shard
				getShardsError  error
			)
			if tc.startingShards != "" {
				startingShards = make([]*k.Shard, 0)
				loadJSONFromFile(t, tc.startingShards, &startingShards)
			}
			if tc.getShardsShards != "" {
				getShardsShards = make([]*k.Shard, 0)
				loadJSONFromFile(t, tc.getShardsShards, &getShardsShards)
			}
			if tc.getShardsError != "" {
				getShardsError = errors.New(tc.getShardsError)
			}
			getShards := mockGetShards(startingShards, getShardsShards, tc.getShardsUpdated, getShardsError)

			client := &mockThrottleClient{
				done: make(chan struct{}),
			}

			producer := New(&Config{
				AggregateBatchCount: tc.aggregateBatchCount,
				GetShards:           getShards,
				FlushInterval:       time.Duration(1) * time.Second,
				StreamName:          tc.name,
				Client:              client,
			})

			var (
				failures = producer.NotifyFailures()
				done     = make(chan struct{})
				timeout  = time.After(10 * time.Second)
				wg       sync.WaitGroup
			)
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
						t.Fatal(failure.Error())
					case <-timeout:
						return
					}
				}
			}()

			producer.Start()

			// populate initial records
			// this will persist in flight due to throttle client
			for _, record := range tc.records {
				err := producer.PutUserRecord(record)
				require.NoError(t, err)
			}

			// wait long enough for flush tick to occur
			time.Sleep(time.Duration(2) * time.Second)

			gotUpdateDrained, gotError := producer.updateShards()

			if tc.expectedError != "" {
				require.EqualError(t, gotError, tc.expectedError)
			} else if !tc.getShardsUpdated {
				require.Nil(t, gotUpdateDrained)
				require.Nil(t, gotError)
			} else {
				require.Nil(t, gotError)
			}

			compareAggregatedRecordRequests(t, tc.updateDrained, gotUpdateDrained)
			close(client.done)
			producer.Stop()
			wg.Wait()
			select {
			case <-done:
			default:
				t.Error("failed test: \n\texpect failures channel to be closed")
			}
		})
	}
}

type mockBenchmarkClient struct {
	b *testing.B
}

func (_ *mockBenchmarkClient) PutRecords(_ *k.PutRecordsInput) (*k.PutRecordsOutput, error) {
	failed := int64(0)
	return &k.PutRecordsOutput{
		FailedRecordCount: &failed,
	}, nil
}

func simpleUUIDRecords(dataSize int) func(int) ([]UserRecord, error) {
	return func(count int) ([]UserRecord, error) {
		records := make([]UserRecord, count)
		for i := 0; i < count; i++ {
			records[i] = newTestUserRecord(uuid.New().String(), "", mockData("foobar", dataSize))
		}
		return records, nil
	}
}

func explicitHashKeyRecords(getShards GetShardsFunc, dataSize int) func(int) ([]UserRecord, error) {
	return func(count int) ([]UserRecord, error) {
		shards, _, err := getShards(nil)
		if err != nil {
			return nil, err
		}

		shardCount := len(shards)
		records := make([]UserRecord, count)
		for i := 0; i < count; i++ {
			bucket := i % shardCount
			shard := shards[bucket]
			records[i] = newTestUserRecord(
				uuid.New().String(),
				*shard.HashKeyRange.StartingHashKey,
				mockData("foobar", dataSize))
		}
		return records, nil
	}
}

func BenchmarkProducer(b *testing.B) {
	testCases := []struct {
		name    string
		config  *Config
		records func(count int) ([]UserRecord, error)
	}{
		{
			name: "default producer",
			config: &Config{
				StreamName:   "default producer",
				BacklogCount: 10000,
			},
			records: simpleUUIDRecords(1024),
		},
		{
			name: "10 shard count",
			config: &Config{
				StreamName:   "10 shard count",
				GetShards:    StaticGetShardsFunc(10),
				BacklogCount: 10000,
			},
			records: simpleUUIDRecords(1024),
		},
		{
			name: "500 shard count",
			config: &Config{
				StreamName:   "500 shard count",
				GetShards:    StaticGetShardsFunc(500),
				BacklogCount: 10000,
			},
			records: simpleUUIDRecords(1024),
		},
		{
			name: "10 shard count using explicit hash key",
			config: &Config{
				StreamName:   "10 shard count",
				GetShards:    StaticGetShardsFunc(10),
				BacklogCount: 10000,
			},
			records: explicitHashKeyRecords(StaticGetShardsFunc(10), 1024),
		},
		{
			name: "500 shard count using explicit hash key",
			config: &Config{
				StreamName:   "500 shard count",
				GetShards:    StaticGetShardsFunc(500),
				BacklogCount: 10000,
			},
			records: explicitHashKeyRecords(StaticGetShardsFunc(500), 1024),
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
					b.Fatal(f.Error())
				}
			}()

			workerWG := new(sync.WaitGroup)
			workers := runtime.NumCPU()
			each := b.N / workers
			workerWG.Add(workers)

			records, err := tc.records(b.N)
			if err != nil {
				b.Fatal(err)
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
