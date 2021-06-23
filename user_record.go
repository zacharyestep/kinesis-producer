package producer

import "math/big"

// UserRecord represents an individual record that is meant for aggregation
type UserRecord interface {
	// PartitionKey returns the partition key of the record
	PartitionKey() string
	// ExplicitHashKey returns an optional explicit hash key that will be used for shard
	// mapping. Should return nil if there is none.
	ExplicitHashKey() *big.Int
	// The raw data payload of the record that should be added to the record
	Data() []byte
	// Size is the size of the record's data. Do not include the size of the partition key
	// in this result. The partition key's size is calculated separately by the aggregator.
	Size() int
}

type DataRecord struct {
	partitionKey string
	data         []byte
}

func NewDataRecord(data []byte, partitionKey string) *DataRecord {
	return &DataRecord{
		partitionKey: partitionKey,
		data:         data,
	}
}

func (r *DataRecord) PartitionKey() string      { return r.partitionKey }
func (r *DataRecord) ExplicitHashKey() *big.Int { return nil }
func (r *DataRecord) Data() []byte              { return r.data }
func (r *DataRecord) Size() int                 { return len(r.data) }
