package producer

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	k "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/golang/protobuf/proto"
)

func assert(t *testing.T, val bool, msg string) {
	if !val {
		t.Error(msg)
	}
}

func TestSizeAndCount(t *testing.T) {
	a := NewAggregator(nil)
	assert(t, a.Size()+a.Count() == 0, "size and count should equal to 0 at the beginning")
	data := []byte("hello")
	pkey := "world"
	n := rand.Intn(100)
	for i := 0; i < n; i++ {
		a.Put(NewDataRecord(data, pkey))
	}

	var expectedSize int
	{
		// plus protobuf message index and wire type per UserRecord
		expectedSize += n
		// plus size of partition keys. Since same key is used in test, it's only stored once
		expectedSize += 5
		// plus size of data per UserRecord
		expectedSize += 5 * n
		// plus size of partition key index per UserRecord
		expectedSize += 8 * n
	}

	assert(t, a.Size() == expectedSize, "size should equal to size of data, partition-keys, partition key indexes, and protobuf wire type")
	assert(t, a.Count() == n, "count should be equal to the number of Put calls")
}

func TestAggregation(t *testing.T) {
	a := NewAggregator(nil)
	n := 50
	userRecords := make([]UserRecord, n)
	for i := 0; i < n; i++ {
		c := strconv.Itoa(i)
		data := []byte("hello-" + c)
		ur := NewDataRecord(data, c)
		userRecords[i] = ur
		a.Put(ur)
	}
	record, err := a.Drain()
	if err != nil {
		t.Error(err)
	}
	assert(t, isAggregated(record.Entry), "should return an agregated record")
	records := extractRecords(record.Entry)
	assert(t, len(records) == n, "AggregatedRecord count does not match")
	for i := 0; i < n; i++ {
		var (
			expected = fmt.Sprintf("hello-%d", i)
			rdata    = string(records[i].Data)
			urdata   = string(userRecords[i].Data())
		)
		assert(t, rdata == expected, "`Data` field contains invalid value")
		assert(t, rdata == urdata, "Record does not match UserRecord")
	}
}

func TestDrainEmptyAggregator(t *testing.T) {
	a := NewAggregator(nil)
	_, err := a.Drain()
	assert(t, err == nil, "should not return an error")
}

// Test if a given entry is aggregated record.
func isAggregated(entry *k.PutRecordsRequestEntry) bool {
	return bytes.HasPrefix(entry.Data, magicNumber)
}

func extractRecords(entry *k.PutRecordsRequestEntry) (out []*k.PutRecordsRequestEntry) {
	src := entry.Data[len(magicNumber) : len(entry.Data)-md5.Size]
	dest := new(AggregatedRecord)
	err := proto.Unmarshal(src, dest)
	if err != nil {
		return
	}
	for i := range dest.Records {
		r := dest.Records[i]
		out = append(out, &k.PutRecordsRequestEntry{
			Data:         r.GetData(),
			PartitionKey: &dest.PartitionKeyTable[r.GetPartitionKeyIndex()],
		})
	}
	return
}
