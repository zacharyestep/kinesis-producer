package producer

import (
	"crypto/md5"
	"sync"

	k "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/golang/protobuf/proto"
)

var (
	magicNumber = []byte{0xF3, 0x89, 0x9A, 0xC2}
)

// Contains the AWS Kinesis PutRecordsRequestEntry and UserRecords that are aggregated into
// the request. UserRecords are provided for more control over failure notifications
type AggregatedRecordRequest struct {
	Entry       *k.PutRecordsRequestEntry
	UserRecords []UserRecord
}

func NewAggregatedRecordRequest(data []byte, partitionKey, explicitHashKey *string, userRecords []UserRecord) *AggregatedRecordRequest {
	return &AggregatedRecordRequest{
		Entry: &k.PutRecordsRequestEntry{
			Data:            data,
			PartitionKey:    partitionKey,
			ExplicitHashKey: explicitHashKey,
		},
		UserRecords: userRecords,
	}
}

type Aggregator struct {
	// Aggregator holds onto its own RWMutex, but the caller of Aggregator methods is expected
	// to call Lock/Unlock
	sync.RWMutex
	// explicitHashKey will be used for aggregated PutRecordsRequestEntry
	explicitHashKey *string
	buf             []UserRecord
	pkeys           []string
	pkeysIndex      map[string]int
	nbytes          int
}

// NewAggregator initializes a new Aggregator with the given partitionKey
func NewAggregator(explicitHashKey *string) *Aggregator {
	a := new(Aggregator)
	a.explicitHashKey = explicitHashKey
	a.pkeysIndex = make(map[string]int)
	return a
}

// Size return how many bytes stored in the aggregator.
// including partition keys.
func (a *Aggregator) Size() int {
	return a.nbytes
}

// Count return how many records stored in the aggregator.
func (a *Aggregator) Count() int {
	return len(a.buf)
}

// Put record using `data` and `partitionKey`. This method is thread-safe.
func (a *Aggregator) Put(userRecord UserRecord) {
	partitionKey := userRecord.PartitionKey()
	// The protobuf message allows more efficient partition and explicit hash key packing
	// by allowing multiple records to point to the same key in a table.
	if _, ok := a.pkeysIndex[partitionKey]; !ok {
		a.pkeys = append(a.pkeys, partitionKey)
		a.nbytes += len([]byte(partitionKey))
		a.pkeysIndex[partitionKey] = len(a.pkeys) - 1
	}

	a.nbytes++ // protobuf message index and wire type
	a.nbytes += partitionKeyIndexSize
	a.buf = append(a.buf, userRecord)
	a.nbytes += userRecord.Size()
}

// Drain create an aggregated `kinesis.PutRecordsRequestEntry`
// that compatible with the KCL's deaggregation logic.
//
// If you interested to know more about it. see: aggregation-format.md
func (a *Aggregator) Drain() (*AggregatedRecordRequest, error) {
	if a.nbytes == 0 {
		return nil, nil
	}

	data, err := proto.Marshal(&AggregatedRecord{
		PartitionKeyTable: a.pkeys,
		Records:           a.aggregateUserRecords(),
	})
	if err != nil {
		return nil, err
	}

	h := md5.New()
	h.Write(data)
	checkSum := h.Sum(nil)
	aggData := append(magicNumber, data...)
	aggData = append(aggData, checkSum...)

	request := NewAggregatedRecordRequest(aggData, &a.pkeys[0], a.explicitHashKey, a.buf)
	a.clear()
	return request, nil
}

func (a *Aggregator) aggregateUserRecords() []*Record {
	count := len(a.buf)
	records := make([]*Record, count)
	for i := 0; i < count; i++ {
		userRecord := a.buf[i]
		keyIndex := uint64(a.pkeysIndex[userRecord.PartitionKey()])
		records[i] = &Record{
			Data:              userRecord.Data(),
			PartitionKeyIndex: &keyIndex,
		}
	}
	return records
}

func (a *Aggregator) clear() {
	a.buf = make([]UserRecord, 0)
	a.pkeys = make([]string, 0)
	a.pkeysIndex = make(map[string]int, 0)
	a.nbytes = 0
}
