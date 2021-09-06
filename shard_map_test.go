package producer

import (
	"context"
	"errors"
	"math/big"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	k "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/require"
)

type testUserRecord struct {
	partitionKey    string
	explicitHashKey *big.Int
	data            []byte
}

func newTestUserRecord(partitionKey string, explicitHashKey string, data []byte) *testUserRecord {
	var ehk *big.Int
	if explicitHashKey != "" {
		ehk = big.NewInt(int64(0))
		ehk, _ = ehk.SetString(explicitHashKey, 10)
	}
	return &testUserRecord{
		partitionKey:    partitionKey,
		explicitHashKey: ehk,
		data:            data,
	}
}

func (r *testUserRecord) PartitionKey() string      { return r.partitionKey }
func (r *testUserRecord) ExplicitHashKey() *big.Int { return r.explicitHashKey }
func (r *testUserRecord) Data() []byte              { return r.data }
func (r *testUserRecord) Size() int                 { return len(r.data) }

// make arbitrarly long data slice with data
func mockData(val string, length int) []byte {
	ret := make([]byte, length)
	copy(ret, val)
	return ret
}

func TestNewShardMap(t *testing.T) {
	testCases := []struct {
		name   string
		shards GetShardsFunc
	}{
		{
			name:   "Nil shards creates one aggregator with nil ExplicitHashKey",
			shards: StaticGetShardsFunc(0),
		},
		{
			name:   "Creates aggregator for each shard using StartingHashKey for ExplicitHashKey",
			shards: StaticGetShardsFunc(3),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			shards, _, _ := tc.shards(nil)
			batchCount := rand.Intn(10)
			shardMap := NewShardMap(shards, batchCount)

			require.Equal(t, batchCount, shardMap.aggregateBatchCount)

			if len(shards) == 0 {
				require.Equal(t, 1, len(shardMap.aggregators))
				require.Nil(t, shardMap.aggregators[0].explicitHashKey)
				return
			}

			require.Equal(t, len(shards), len(shardMap.aggregators))
			for i, agg := range shardMap.aggregators {
				require.Equal(t, shards[i].HashKeyRange.StartingHashKey, agg.explicitHashKey)
			}
		})
	}
}

func TestShardMapPut(t *testing.T) {
	testCases := []struct {
		name                string
		shards              GetShardsFunc
		aggregateBatchCount int
		records             []UserRecord
		putDrained          []*AggregatedRecordRequest
		postDrained         []*AggregatedRecordRequest
	}{
		{
			name:                "Aggregates all records together with no shards",
			shards:              StaticGetShardsFunc(0),
			aggregateBatchCount: 2,
			records: []UserRecord{
				newTestUserRecord("foo", "", []byte("hello")),
				newTestUserRecord("foo", "", []byte("hello")),
			},
			postDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{},
					UserRecords: []UserRecord{
						newTestUserRecord("foo", "", []byte("hello")),
						newTestUserRecord("foo", "", []byte("hello")),
					},
				},
			},
		},
		{
			name:                "Drains record when AggregateBatchCount exceeded",
			shards:              StaticGetShardsFunc(0),
			aggregateBatchCount: 1,
			records: []UserRecord{
				newTestUserRecord("foo", "", []byte("hello")),
				newTestUserRecord("foo", "", []byte("hello")),
			},
			putDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{},
					UserRecords: []UserRecord{
						newTestUserRecord("foo", "", []byte("hello")),
					},
				},
			},
			postDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{},
					UserRecords: []UserRecord{
						newTestUserRecord("foo", "", []byte("hello")),
					},
				},
			},
		},
		{
			name:                "Drains record when maxRecordSize exceeded",
			shards:              StaticGetShardsFunc(0),
			aggregateBatchCount: 4,
			records: []UserRecord{
				newTestUserRecord("foo", "", mockData("hello", (maxRecordSize-100)/3)),
				newTestUserRecord("foo", "", mockData("hello", (maxRecordSize-100)/3)),
				newTestUserRecord("foo", "", mockData("hello", (maxRecordSize-100)/3)),
				newTestUserRecord("foo", "", mockData("hello", (maxRecordSize-100)/3)),
			},
			putDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{},
					UserRecords: []UserRecord{
						newTestUserRecord("foo", "", mockData("hello", (maxRecordSize-100)/3)),
						newTestUserRecord("foo", "", mockData("hello", (maxRecordSize-100)/3)),
						newTestUserRecord("foo", "", mockData("hello", (maxRecordSize-100)/3)),
					},
				},
			},
			postDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{},
					UserRecords: []UserRecord{
						newTestUserRecord("foo", "", mockData("hello", (maxRecordSize-100)/3)),
					},
				},
			},
		},
		{
			name:                "Buckets UserRecords by ExplicitHashKey for aggregation",
			shards:              StaticGetShardsFunc(2),
			aggregateBatchCount: 2,
			records: []UserRecord{
				newTestUserRecord("foo", "100141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("bar", "200141183460469231731687303715884105727", []byte("world")),
				newTestUserRecord("foo", "110141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("foo", "120141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("bar", "210141183460469231731687303715884105727", []byte("world")),
			},
			putDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of first shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("foo", "", []byte("hello")),
						newTestUserRecord("foo", "", []byte("hello")),
					},
				},
			},
			postDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of first shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("foo", "", []byte("hello")),
					},
				},
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of second shard
						ExplicitHashKey: aws.String("170141183460469231731687303715884105727"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("bar", "", []byte("world")),
						newTestUserRecord("bar", "", []byte("world")),
					},
				},
			},
		},
		{
			name:                "Buckets UserRecords by md5 hash of PartitionKey for aggregation",
			shards:              StaticGetShardsFunc(2),
			aggregateBatchCount: 2,
			records: []UserRecord{
				// md5 hash of a4c35... is 119180311785658537254883594002133722733
				newTestUserRecord("a4c35bc0b0474f3ca691bdbd526f1f50", "", []byte("hello")),
				// md5 hash pf 4459a... is 269473573115900060929316357216383275764
				newTestUserRecord("4459a807a2ef401690812235bad5cbf5", "", []byte("world")),
				newTestUserRecord("a4c35bc0b0474f3ca691bdbd526f1f50", "", []byte("hello")),
				newTestUserRecord("a4c35bc0b0474f3ca691bdbd526f1f50", "", []byte("hello")),
				newTestUserRecord("4459a807a2ef401690812235bad5cbf5", "", []byte("world")),
			},
			putDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of first shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("a4c35bc0b0474f3ca691bdbd526f1f50", "", []byte("hello")),
						newTestUserRecord("a4c35bc0b0474f3ca691bdbd526f1f50", "", []byte("hello")),
					},
				},
			},
			postDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of first shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("a4c35bc0b0474f3ca691bdbd526f1f50", "", []byte("hello")),
					},
				},
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of second shard
						ExplicitHashKey: aws.String("170141183460469231731687303715884105727"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("4459a807a2ef401690812235bad5cbf5", "", []byte("world")),
						newTestUserRecord("4459a807a2ef401690812235bad5cbf5", "", []byte("world")),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			shards, _, _ := tc.shards(nil)
			shardMap := NewShardMap(shards, tc.aggregateBatchCount)

			var (
				putWg        sync.WaitGroup
				putDrainedWg sync.WaitGroup
				putDrained   []*AggregatedRecordRequest
				putDrainedC  = make(chan *AggregatedRecordRequest)
			)
			putWg.Add(len(tc.records))

			for _, r := range tc.records {
				go func(record UserRecord) {
					defer putWg.Done()
					d, err := shardMap.Put(record)
					require.NoError(t, err)
					if d != nil {
						putDrainedC <- d
					}
				}(r)
			}

			// collect any drained records during Put
			putDrainedWg.Add(1)
			go func() {
				for d := range putDrainedC {
					putDrained = append(putDrained, d)
				}
				putDrainedWg.Done()
			}()

			putWg.Wait()
			close(putDrainedC)
			putDrainedWg.Wait()

			compareAggregatedRecordRequests(t, tc.putDrained, putDrained)

			postDrained, errs := shardMap.Drain()
			require.Nil(t, errs)

			compareAggregatedRecordRequests(t, tc.postDrained, postDrained)
		})
	}
}

type byExplicitHashKey []*AggregatedRecordRequest

func (a byExplicitHashKey) Len() int      { return len(a) }
func (a byExplicitHashKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byExplicitHashKey) Less(i, j int) bool {
	return *a[i].Entry.ExplicitHashKey < *a[j].Entry.ExplicitHashKey
}

func compareAggregatedRecordRequests(t *testing.T, expected, actual []*AggregatedRecordRequest) {
	require.Equal(t, len(expected), len(actual))
	if actual == nil {
		return
	}

	if actual[0].Entry.ExplicitHashKey != nil {
		sort.Sort(byExplicitHashKey(actual))
	}

	for i, e := range expected {
		a := actual[i]

		// We do not check PartitionKey or Data as they depend on order which we don't have
		// control over
		if e.Entry.ExplicitHashKey == nil {
			require.Nil(t, a.Entry.ExplicitHashKey)
		} else {
			require.NotNil(t, a.Entry.ExplicitHashKey)
			require.Equal(t, *e.Entry.ExplicitHashKey, *a.Entry.ExplicitHashKey)
		}

		require.Equal(t, len(e.UserRecords), len(a.UserRecords))
		// Again because we cannot rely on order, we just compare Data.
		// It is assumed for these tests that records expected to be aggregated together have
		// the same data values
		for j, expectedRecord := range e.UserRecords {
			actualRecord := a.UserRecords[j]
			require.Equal(t, expectedRecord.Data(), actualRecord.Data())
		}
	}
}

func TestShardMapSize(t *testing.T) {
	shards, _, _ := StaticGetShardsFunc(2)(nil)
	shardMap := NewShardMap(shards, 1)

	require.Equal(t, 0, shardMap.Size())
	record := newTestUserRecord("foo", "100141183460469231731687303715884105727", mockData("", 10))
	drained, err := shardMap.Put(record)
	require.Nil(t, drained)
	require.Nil(t, err)

	expectedSize := calculateRecordFieldSize(0, record.Data()) + calculateStringFieldSize("foo")
	require.Equal(t, expectedSize, shardMap.Size())

	record2 := newTestUserRecord("bar", "210141183460469231731687303715884105727", mockData("", 20))
	drained, err = shardMap.Put(record2)
	require.Nil(t, drained)
	require.Nil(t, err)

	{
		expectedSize = calculateRecordFieldSize(0, record.Data()) + calculateStringFieldSize("foo")
		expectedSize += calculateRecordFieldSize(0, record2.Data()) + calculateStringFieldSize("bar")
	}
	require.Equal(t, expectedSize, shardMap.Size())

	record3 := newTestUserRecord("foo", "100141183460469231731687303715884105727", mockData("", 20))
	drained, err = shardMap.Put(record3)
	require.NotNil(t, drained)
	require.Nil(t, err)

	{
		// record1 drained on put of record3 so don't include in size
		expectedSize = calculateRecordFieldSize(0, record2.Data()) + calculateStringFieldSize("bar")
		expectedSize += calculateRecordFieldSize(0, record3.Data()) + calculateStringFieldSize("foo")
	}
	require.Equal(t, expectedSize, shardMap.Size())

	all, errs := shardMap.Drain()
	require.Nil(t, errs)
	require.Equal(t, 2, len(all))
	require.Equal(t, 0, shardMap.Size())

}

func TestStaticGetShardsFunc(t *testing.T) {
	testCases := []struct {
		name  string
		count int
	}{
		{
			name:  "Returns nil for count 0",
			count: 0,
		},
		{
			name:  "Creates evenly distributed shard list of size count",
			count: 1,
		},
		{
			name:  "Creates evenly distributed shard list of size count",
			count: 10,
		},
		{
			name:  "Creates evenly distributed shard list of size count",
			count: 100,
		},
		{
			name:  "Creates evenly distributed shard list of size count",
			count: 1000,
		},
		{
			name:  "Creates evenly distributed shard list of size count",
			count: 10000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			getShards := StaticGetShardsFunc(tc.count)
			shards, updated, err := getShards(nil)

			require.False(t, updated)
			require.Nil(t, err)

			if tc.count == 0 {
				require.Nil(t, shards)
				return
			}

			require.Equal(t, tc.count, len(shards))
			require.Equal(t, maxHashKeyRange, *shards[len(shards)-1].HashKeyRange.EndingHashKey)

			var count, step, tolerance *big.Int
			{
				count = big.NewInt(int64(tc.count))
				step = big.NewInt(int64(0))
				step, _ = step.SetString(maxHashKeyRange, 10)
				step = step.Div(step, count)

				// tolerate to account for small rounding errors
				tolerance = big.NewInt(int64(tc.count))
			}

			for _, shard := range shards {
				start := big.NewInt(int64(0))
				start, _ = start.SetString(*shard.HashKeyRange.StartingHashKey, 10)
				end := big.NewInt(int64(0))
				end, _ = end.SetString(*shard.HashKeyRange.EndingHashKey, 10)

				diff := big.NewInt(int64(0))
				diff = diff.Sub(end, start)

				delta := big.NewInt(int64(0))
				delta = delta.Sub(step, diff)
				delta = delta.Abs(delta)
				delta = delta.Sub(tolerance, delta)

				// tolerance >= |(step - (end - start))|
				require.True(t, delta.Sign() >= 0)
			}
		})
	}
}

type mockShardLister struct {
	t          *testing.T
	responses  []*k.ListShardsOutput
	callCount  int
	streamName string
	next       *string
}

func (m *mockShardLister) ListShards(ctx context.Context, input *k.ListShardsInput, optFns ...func(*k.Options)) (*k.ListShardsOutput, error) {
	m.callCount++
	if m.callCount > len(m.responses) {
		return nil, errors.New("ListShards error")
	}

	call := m.callCount - 1

	if call == 0 {
		// First call should include StreamName and no NextToken
		require.NotNil(m.t, input.StreamName)
		require.Equal(m.t, m.streamName, *input.StreamName)
		require.Nil(m.t, input.NextToken)
	} else {
		// Subsequent calls should include NextToken and no StreamName
		require.Nil(m.t, input.StreamName)
		require.NotNil(m.t, input.NextToken)
		require.NotNil(m.t, m.next)
		require.Equal(m.t, *m.next, *input.NextToken)
	}

	response := m.responses[call]
	m.next = response.NextToken

	return response, nil
}

func TestGetKinesisShardsFunc(t *testing.T) {
	testCases := []struct {
		name                string
		oldShards           string
		listShardsResponses string
		expectedShards      string
		expectedUpdated     bool
		expectedError       string
	}{
		{
			name:          "returns error from ShardLister",
			expectedError: "ListShards error",
		},
		{
			name:                "returns list of new shards",
			oldShards:           "testdata/TestGetKinesisShardsFunc/new_shards/oldShards.json",
			listShardsResponses: "testdata/TestGetKinesisShardsFunc/new_shards/listShardsResponses.json",
			expectedShards:      "testdata/TestGetKinesisShardsFunc/new_shards/expectedShards.json",
			expectedUpdated:     true,
		},
		{
			name:                "handles unsorted response",
			oldShards:           "testdata/TestGetKinesisShardsFunc/unsorted/oldShards.json",
			listShardsResponses: "testdata/TestGetKinesisShardsFunc/unsorted/listShardsResponses.json",
			expectedShards:      "testdata/TestGetKinesisShardsFunc/unsorted/expectedShards.json",
			expectedUpdated:     true,
		},
		{
			name:                "returns false for equal shards",
			oldShards:           "testdata/TestGetKinesisShardsFunc/equal_shards/oldShards.json",
			listShardsResponses: "testdata/TestGetKinesisShardsFunc/equal_shards/listShardsResponses.json",
			expectedUpdated:     false,
		},
		{
			name:                "returns error from subsequent ListShards calls",
			listShardsResponses: "testdata/TestGetKinesisShardsFunc/NextToken_error/listShardsResponses.json",
			expectedError:       "ListShards error",
		},
		{
			name:                "calls ListShards with NextToken and returns new shards",
			oldShards:           "testdata/TestGetKinesisShardsFunc/NextToken_new_shards/oldShards.json",
			listShardsResponses: "testdata/TestGetKinesisShardsFunc/NextToken_new_shards/listShardsResponses.json",
			expectedShards:      "testdata/TestGetKinesisShardsFunc/NextToken_new_shards/expectedShards.json",
			expectedUpdated:     true,
		},
		{
			name:                "calls ListShards with NextToken and returns false for equal shards",
			oldShards:           "testdata/TestGetKinesisShardsFunc/NextToken_equal_shards/oldShards.json",
			listShardsResponses: "testdata/TestGetKinesisShardsFunc/NextToken_equal_shards/listShardsResponses.json",
			expectedUpdated:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				listShardsResponses []*k.ListShardsOutput
				oldShards           []types.Shard
				expectedShards      []types.Shard
			)

			if tc.listShardsResponses != "" {
				listShardsResponses = make([]*k.ListShardsOutput, 0)
				loadJSONFromFile(t, tc.listShardsResponses, &listShardsResponses)
			}

			if tc.oldShards != "" {
				oldShards = make([]types.Shard, 0)
				loadJSONFromFile(t, tc.oldShards, &oldShards)
			}

			if tc.expectedShards != "" {
				expectedShards = make([]types.Shard, 0)
				loadJSONFromFile(t, tc.expectedShards, &expectedShards)
			}

			client := &mockShardLister{
				t:          t,
				responses:  listShardsResponses,
				streamName: tc.name,
			}

			undertest := GetKinesisShardsFunc(client, tc.name)
			gotShards, gotUpdated, gotError := undertest(oldShards)

			if tc.expectedError != "" {
				require.Nil(t, gotShards)
				require.False(t, gotUpdated)
				require.EqualError(t, gotError, tc.expectedError)
			} else {
				require.Equal(t, expectedShards, gotShards)
				require.Equal(t, tc.expectedUpdated, gotUpdated)
				require.Nil(t, gotError)
			}
		})
	}
}

func TestShardMapUpdateShards(t *testing.T) {
	testCases := []struct {
		name                string
		startingShards      string
		aggregateBatchCount int
		records             []UserRecord
		newShards           string
		pendingRecords      []*AggregatedRecordRequest
		updateDrained       []*AggregatedRecordRequest
		postDrained         []*AggregatedRecordRequest
		expectedError       string
	}{
		{
			name:                "returns error from pending record put",
			startingShards:      "testdata/TestShardMapUpdateShards/error_pending_put/startingShards.json",
			aggregateBatchCount: 2,
			records: []UserRecord{
				newTestUserRecord("foo", "100141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("bar", "230141183460469231731687303715884105727", []byte("world")),
				newTestUserRecord("foo", "110141183460469231731687303715884105727", []byte("hello")),
			},
			newShards: "testdata/TestShardMapUpdateShards/error_pending_put/newShards.json",
			pendingRecords: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of second shard
						ExplicitHashKey: aws.String("170141183460469231731687303715884105728"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("baz", "200141183460469231731687303715884105727", []byte("world")),
						newTestUserRecord("baz", "210141183460469231731687303715884105727", []byte("world")),
					},
				},
			},
			updateDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of second shard
						ExplicitHashKey: aws.String("170141183460469231731687303715884105728"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("baz", "200141183460469231731687303715884105727", []byte("world")),
						newTestUserRecord("baz", "210141183460469231731687303715884105727", []byte("world")),
					},
				},
			},
			postDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of first shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("foo", "", []byte("hello")),
						newTestUserRecord("foo", "", []byte("hello")),
					},
				},
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of second shard
						ExplicitHashKey: aws.String("170141183460469231731687303715884105728"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("bar", "", []byte("world")),
					},
				},
			},
			expectedError: "ExplicitHashKey outside shard key range: 200141183460469231731687303715884105727",
		},
		{
			name:                "returns error from aggregator record put",
			startingShards:      "testdata/TestShardMapUpdateShards/error_agg_put/startingShards.json",
			aggregateBatchCount: 2,
			records: []UserRecord{
				newTestUserRecord("foo", "100141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("bar", "230141183460469231731687303715884105727", []byte("world")),
				newTestUserRecord("foo", "110141183460469231731687303715884105727", []byte("hello")),
			},
			newShards: "testdata/TestShardMapUpdateShards/error_agg_put/newShards.json",
			pendingRecords: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of second shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("baz", "120141183460469231731687303715884105727", []byte("world")),
						newTestUserRecord("baz", "130141183460469231731687303715884105727", []byte("world")),
					},
				},
			},
			updateDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of second shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("baz", "120141183460469231731687303715884105727", []byte("world")),
						newTestUserRecord("baz", "130141183460469231731687303715884105727", []byte("world")),
					},
				},
			},
			postDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of first shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("foo", "", []byte("hello")),
						newTestUserRecord("foo", "", []byte("hello")),
					},
				},
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of second shard
						ExplicitHashKey: aws.String("170141183460469231731687303715884105728"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("bar", "", []byte("world")),
					},
				},
			},
			expectedError: "ExplicitHashKey outside shard key range: 230141183460469231731687303715884105727",
		},
		// {
		// 	name:                "does not update shards if updated false",
		// 	startingShards:      "testdata/TestShardMapUpdateShards/no_update/startingShards.json",
		// 	aggregateBatchCount: 2,
		// 	records: []UserRecord{
		// 		newTestUserRecord("foo", "100141183460469231731687303715884105727", []byte("hello")),
		// 		newTestUserRecord("bar", "200141183460469231731687303715884105727", []byte("world")),
		// 		newTestUserRecord("foo", "110141183460469231731687303715884105727", []byte("hello")),
		// 		newTestUserRecord("bar", "210141183460469231731687303715884105727", []byte("world")),
		// 	},
		// 	getShardsUpdated: false,
		// 	postDrained: []*AggregatedRecordRequest{
		// 		&AggregatedRecordRequest{
		// 			Entry: types.PutRecordsRequestEntry{
		// 				// StartingHashKey of first shard
		// 				ExplicitHashKey: aws.String("0"),
		// 			},
		// 			UserRecords: []UserRecord{
		// 				newTestUserRecord("foo", "", []byte("hello")),
		// 				newTestUserRecord("foo", "", []byte("hello")),
		// 			},
		// 		},
		// 		&AggregatedRecordRequest{
		// 			Entry: types.PutRecordsRequestEntry{
		// 				// StartingHashKey of second shard
		// 				ExplicitHashKey: aws.String("170141183460469231731687303715884105728"),
		// 			},
		// 			UserRecords: []UserRecord{
		// 				newTestUserRecord("bar", "", []byte("world")),
		// 				newTestUserRecord("bar", "", []byte("world")),
		// 			},
		// 		},
		// 	},
		// },
		{
			name:                "updates shards and redistributes records",
			startingShards:      "testdata/TestShardMapUpdateShards/update/startingShards.json",
			aggregateBatchCount: 4,
			records: []UserRecord{
				newTestUserRecord("foo", "100141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("bar", "200141183460469231731687303715884105727", []byte("world")),
				newTestUserRecord("foo", "110141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("bar", "210141183460469231731687303715884105727", []byte("world")),
			},
			newShards: "testdata/TestShardMapUpdateShards/update/newShards.json",
			pendingRecords: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of first shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("foo", "120141183460469231731687303715884105727", []byte("hello")),
						newTestUserRecord("foo", "130141183460469231731687303715884105727", []byte("hello")),
					},
				},
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of second shard
						ExplicitHashKey: aws.String("170141183460469231731687303715884105728"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("bar", "220141183460469231731687303715884105727", []byte("world")),
						newTestUserRecord("bar", "230141183460469231731687303715884105727", []byte("world")),
					},
				},
			},
			postDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of first shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("foo", "", []byte("hello")),
						newTestUserRecord("foo", "", []byte("hello")),
						newTestUserRecord("foo", "", []byte("hello")),
						newTestUserRecord("foo", "", []byte("hello")),
					},
				},
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of second shard
						ExplicitHashKey: aws.String("170141183460469231731687303715884105728"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("bar", "", []byte("world")),
						newTestUserRecord("bar", "", []byte("world")),
						newTestUserRecord("bar", "", []byte("world")),
						newTestUserRecord("bar", "", []byte("world")),
					},
				},
			},
		},
		{
			name:                "updates shards and redistributes records, returning drained records from the process",
			startingShards:      "testdata/TestShardMapUpdateShards/update_drained/startingShards.json",
			aggregateBatchCount: 2,
			records: []UserRecord{
				newTestUserRecord("foo", "100141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("bar", "200141183460469231731687303715884105727", []byte("world")),
				newTestUserRecord("foo", "110141183460469231731687303715884105727", []byte("hello")),
				newTestUserRecord("bar", "210141183460469231731687303715884105727", []byte("world")),
			},
			newShards: "testdata/TestShardMapUpdateShards/update_drained/newShards.json",
			pendingRecords: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of first shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("fuzz", "120141183460469231731687303715884105727", []byte("hello")),
					},
				},
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of second shard
						ExplicitHashKey: aws.String("170141183460469231731687303715884105728"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("buzz", "220141183460469231731687303715884105727", []byte("world")),
					},
				},
			},
			updateDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of first shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("fuzz", "", []byte("hello")),
						newTestUserRecord("buzz", "", []byte("world")),
					},
				},
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of first shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("foo", "", []byte("hello")),
						newTestUserRecord("foo", "", []byte("hello")),
					},
				},
			},
			postDrained: []*AggregatedRecordRequest{
				&AggregatedRecordRequest{
					Entry: types.PutRecordsRequestEntry{
						// StartingHashKey of first shard
						ExplicitHashKey: aws.String("0"),
					},
					UserRecords: []UserRecord{
						newTestUserRecord("bar", "", []byte("world")),
						newTestUserRecord("bar", "", []byte("world")),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				startingShards []types.Shard
			)
			if tc.startingShards != "" {
				startingShards = make([]types.Shard, 0)
				loadJSONFromFile(t, tc.startingShards, &startingShards)
			}

			shardMap := NewShardMap(startingShards, tc.aggregateBatchCount)

			for _, record := range tc.records {
				// assume test config to not drain on setup Puts
				_, err := shardMap.Put(record)
				require.NoError(t, err)
			}

			var newShards []types.Shard
			if tc.newShards != "" {
				newShards = make([]types.Shard, 0)
				loadJSONFromFile(t, tc.newShards, &newShards)
			}

			gotUpdateDrained, gotError := shardMap.UpdateShards(newShards, tc.pendingRecords)
			if tc.expectedError != "" {
				require.EqualError(t, gotError, tc.expectedError)
				require.Equal(t, tc.pendingRecords, gotUpdateDrained)
				require.Equal(t, startingShards, shardMap.shards)
			} else {
				require.Nil(t, gotError)
				require.Equal(t, newShards, shardMap.shards)
			}

			compareAggregatedRecordRequests(t, tc.updateDrained, gotUpdateDrained)

			gotPostDrained, gotErrors := shardMap.Drain()
			require.Nil(t, gotErrors)

			compareAggregatedRecordRequests(t, tc.postDrained, gotPostDrained)
		})
	}
}
