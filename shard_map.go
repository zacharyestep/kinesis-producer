package producer

import (
	"context"
	"crypto/md5"
	"math/big"
	"sort"
	"sync"

	k "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// 2^128 exclusive upper bound
// Hash key ranges are 0 indexed, so true max is 2^128 - 1
var maxHashKeyRange = "340282366920938463463374607431768211455"

// ShardLister is the interface that wraps the KinesisAPI.ListShards method.
type ShardLister interface {
	ListShards(ctx context.Context, params *k.ListShardsInput, optFns ...func(*k.Options)) (*k.ListShardsOutput, error)
}

// GetKinesisShardsFunc gets the active list of shards from Kinesis.ListShards API
func GetKinesisShardsFunc(client ShardLister, streamName string) GetShardsFunc {
	return func(old []types.Shard) ([]types.Shard, bool, error) {
		var (
			shards []types.Shard
			next   *string
		)

		for {
			input := &k.ListShardsInput{}
			if next != nil {
				input.NextToken = next
			} else {
				input.StreamName = &streamName
			}

			resp, err := client.ListShards(context.Background(), input)
			if err != nil {
				return nil, false, err
			}

			for _, shard := range resp.Shards {
				// There may be many shards with overlapping HashKeyRanges due to prior merge and
				// split operations. The currently open shards are the ones that do not have a
				// SequenceNumberRange.EndingSequenceNumber.
				if shard.SequenceNumberRange.EndingSequenceNumber == nil {
					shards = append(shards, shard)
				}
			}

			next = resp.NextToken
			if next == nil {
				break
			}
		}

		sort.Sort(ShardSlice(shards))

		if shardsEqual(old, shards) {
			return nil, false, nil
		}
		return shards, true, nil
	}
}

// StaticGetShardsFunc returns a GetShardsFunc that when called, will generate a static
// list of shards with length count whos HashKeyRanges are evenly distributed
func StaticGetShardsFunc(count int) GetShardsFunc {
	return func(old []types.Shard) ([]types.Shard, bool, error) {
		if count == 0 {
			return nil, false, nil
		}

		step := big.NewInt(int64(0))
		step, _ = step.SetString(maxHashKeyRange, 10)
		bCount := big.NewInt(int64(count))
		step = step.Div(step, bCount)
		b1 := big.NewInt(int64(1))

		shards := make([]types.Shard, count)
		key := big.NewInt(int64(0))
		for i := 0; i < count; i++ {
			bI := big.NewInt(int64(i))
			// starting key range (step * i)
			key = key.Mul(bI, step)
			startingHashKey := key.String()

			// ending key range ((step * (i + 1)) - 1)
			bINext := big.NewInt(int64(i + 1))
			key = key.Mul(bINext, step)
			key = key.Sub(key, b1)
			endingHashKey := key.String()

			shards[i].HashKeyRange = &types.HashKeyRange{
				StartingHashKey: &startingHashKey,
				EndingHashKey:   &endingHashKey,
			}
		}
		// Set last shard end range to max to account for small rounding errors
		shards[len(shards)-1].HashKeyRange.EndingHashKey = &maxHashKeyRange
		return shards, false, nil
	}
}

type ShardSlice []types.Shard

func (p ShardSlice) Len() int { return len(p) }
func (p ShardSlice) Less(i, j int) bool {
	a, _ := new(big.Int).SetString(*p[i].HashKeyRange.StartingHashKey, 10)
	b, _ := new(big.Int).SetString(*p[j].HashKeyRange.StartingHashKey, 10)
	// a < b
	return a.Cmp(b) == -1
}
func (p ShardSlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// Checks to see if the shards have the same hash key ranges
func shardsEqual(a, b []types.Shard) bool {
	if len(a) != len(b) {
		return false
	}
	for i, ashard := range a {
		bshard := b[i]
		if *ashard.HashKeyRange.StartingHashKey != *bshard.HashKeyRange.StartingHashKey ||
			*ashard.HashKeyRange.EndingHashKey != *bshard.HashKeyRange.EndingHashKey {
			return false
		}
	}
	return true
}

type ShardMap struct {
	sync.RWMutex
	shards      []types.Shard
	aggregators []*Aggregator
	// aggregateBatchCount determine the maximum number of items to pack into an aggregated record.
	aggregateBatchCount int
}

// NewShardMap initializes an aggregator for each shard.
// UserRecords that map to the same shard based on MD5 hash of their partition
// key (Same method used by Kinesis) will be aggregated together. Aggregators will use an
// ExplicitHashKey from their assigned shards when creating kinesis.PutRecordsRequestEntry.
// A ShardMap with an empty shards slice will return to unsharded behavior with a single
// aggregator. The aggregator will instead use the PartitionKey of the first UserRecord and
// no ExplicitHashKey.
func NewShardMap(shards []types.Shard, aggregateBatchCount int) *ShardMap {
	return &ShardMap{
		shards:              shards,
		aggregators:         makeAggregators(shards),
		aggregateBatchCount: aggregateBatchCount,
	}
}

// Put puts a UserRecord into the aggregator that maps to its partition key.
func (m *ShardMap) Put(userRecord UserRecord) (*AggregatedRecordRequest, error) {
	m.RLock()
	drained, err := m.put(userRecord)
	// Not using defer to avoid runtime overhead
	m.RUnlock()
	return drained, err
}

// Size return how many bytes stored in all the aggregators.
// including partition keys.
func (m *ShardMap) Size() int {
	m.RLock()
	size := 0
	for _, a := range m.aggregators {
		a.RLock()
		size += a.Size()
		a.RUnlock()
	}
	m.RUnlock()
	return size
}

// Drain drains all the aggregators and returns a list of the results
func (m *ShardMap) Drain() ([]*AggregatedRecordRequest, []error) {
	m.RLock()
	var (
		requests []*AggregatedRecordRequest
		errs     []error
	)
	for _, a := range m.aggregators {
		a.Lock()
		req, err := a.Drain()
		a.Unlock()
		if err != nil {
			errs = append(errs, err)
		} else if req != nil {
			requests = append(requests, req)
		}
	}
	m.RUnlock()
	return requests, errs
}

// Shards returns the list of shards
func (m *ShardMap) Shards() []types.Shard {
	m.RLock()
	shards := m.shards
	m.RUnlock()
	return shards
}

// Update the list of shards and redistribute buffered user records.
// Returns any records that were drained due to redistribution.
// Shards are not updated if an error occurs during redistribution.
// TODO: Can we optimize this?
// TODO: How to handle shard splitting? If a shard splits but we don't remap before sending
//
//	records to the new shards, once we do update our mapping, user records may end up
//	in a new shard and we would lose the shard ordering. Consumer can probably figure
//	it out since we retain original partition keys (but not explicit hash keys)
//	Shard merging should not be an issue since records from both shards should fall
//	into the merged hash key range.
func (m *ShardMap) UpdateShards(shards []types.Shard, pendingRecords []*AggregatedRecordRequest) ([]*AggregatedRecordRequest, error) {
	m.Lock()
	defer m.Unlock()

	update := NewShardMap(shards, m.aggregateBatchCount)
	var drained []*AggregatedRecordRequest

	// first put any pending UserRecords from inflight requests
	for _, record := range pendingRecords {
		for _, userRecord := range record.UserRecords {
			req, err := update.put(userRecord)
			if err != nil {
				// if we encounter an error trying to redistribute the records, return the pending
				// records to the Producer tries to send them again. They won't be redistributed
				// across new shards, but at least they won't be lost.
				return pendingRecords, err
			}
			if req != nil {
				drained = append(drained, req)
			}
		}
	}
	// then redistribute the records still being aggregated
	for _, agg := range m.aggregators {
		// We don't need to get the aggregator lock because we have the shard map write lock
		for _, userRecord := range agg.buf {
			req, err := update.put(userRecord)
			if err != nil {
				return pendingRecords, err
			}
			if req != nil {
				drained = append(drained, req)
			}
		}
	}
	// Only update m if we successfully redistributed all the user records
	m.shards = update.shards
	m.aggregators = update.aggregators
	return drained, nil
}

// puts a UserRecord into the aggregator that maps to its partition key.
// Not thread safe. acquire lock before calling.
func (m *ShardMap) put(userRecord UserRecord) (*AggregatedRecordRequest, error) {
	bucket := m.bucket(userRecord)
	if bucket == -1 {
		return nil, &ShardBucketError{UserRecord: userRecord}
	}
	a := m.aggregators[bucket]
	a.Lock()
	var (
		needToDrain = a.WillOverflow(userRecord) || a.Count() >= m.aggregateBatchCount

		drained *AggregatedRecordRequest
		err     error
	)
	if needToDrain {
		drained, err = a.Drain()
	}
	a.Put(userRecord)
	a.Unlock()
	return drained, err
}

// bucket returns the index of the shard the given partition key maps to.
// Returns -1 if partition key is outside shard range.
// Assumes shards is ordered by  contiguous HaskKeyRange ascending. If there are gaps in
// shard hash key ranges and the partition key falls into one of the gaps, it will be placed
// in the shard with the larger starting HashKeyRange
// Not thread safe. acquire lock before calling.
// TODO: Can we optimize this? Cache for pk -> bucket?
func (m *ShardMap) bucket(userRecord UserRecord) int {
	if len(m.shards) == 0 {
		return 0
	}

	hk := userRecord.ExplicitHashKey()
	if hk == nil {
		hk = hashKey(userRecord.PartitionKey())
	}
	sortFunc := func(i int) bool {
		shard := m.shards[i]
		end := big.NewInt(int64(0))
		end, _ = end.SetString(*shard.HashKeyRange.EndingHashKey, 10)
		// end >= hk
		return end.Cmp(hk) > -1
	}

	// Search uses binary search to find and return the smallest index i in [0, n)
	// at which f(i) is true
	// See https://golang.org/pkg/sort/#Search
	bucket := sort.Search(len(m.shards), sortFunc)
	if bucket == len(m.shards) {
		return -1
	}
	return bucket
}

// Calculate a new explicit hash key based on the given partition key.
// (following the algorithm from the original KPL).
// Copied from: https://github.com/a8m/kinesis-producer/issues/1#issuecomment-524620994
func hashKey(pk string) *big.Int {
	h := md5.New()
	h.Write([]byte(pk))
	sum := h.Sum(nil)
	hk := big.NewInt(int64(0))
	for i := 0; i < md5.Size; i++ {
		p := big.NewInt(int64(sum[i]))
		p = p.Lsh(p, uint((16-i-1)*8))
		hk = hk.Add(hk, p)
	}
	return hk
}

func makeAggregators(shards []types.Shard) []*Aggregator {
	count := len(shards)
	if count == 0 {
		return []*Aggregator{NewAggregator(nil)}
	}

	aggregators := make([]*Aggregator, count)
	for i := 0; i < count; i++ {
		shard := shards[i]
		// Is using the StartingHashKey sufficient?
		aggregators[i] = NewAggregator(shard.HashKeyRange.StartingHashKey)
	}
	return aggregators
}
