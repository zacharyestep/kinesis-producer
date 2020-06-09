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
	"github.com/stretchr/testify/require"
)

func TestSizeAndCount(t *testing.T) {
	a := NewAggregator(nil)
	require.Equal(t, 0, a.Size()+a.Count(), "size and count should equal to 0 at the beginning")

	var (
		data             = []byte("hello")
		keyCount         = rand.Intn(10) + 1
		keys             = make([]string, keyCount)
		totalKeySize     = 0
		n                = rand.Intn(100) + 1
		totalRecordCount = n * keyCount
	)

	// setup multiple keys
	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("world-%d", i)
		totalKeySize += len([]byte(keys[i]))
	}

	for i := 0; i < n; i++ {
		for k := 0; k < keyCount; k++ {
			a.Put(NewDataRecord(data, keys[k]))
		}
	}

	var expectedSize int
	{
		// plus protobuf message index and wire type per UserRecord
		expectedSize += totalRecordCount
		// plus size of partition keys.
		expectedSize += totalKeySize
		// plus size of data per UserRecord
		expectedSize += len(data) * totalRecordCount
		// plus size of partition key index per UserRecord
		expectedSize += 8 * totalRecordCount
	}

	require.Equal(t, expectedSize, a.Size(), "size should equal to size of data, partition-keys, partition key indexes, and protobuf wire type")
	require.Equal(t, totalRecordCount, a.Count(), "count should be equal to the number of Put calls")
}

func TestAggregation(t *testing.T) {
	testCases := []struct {
		name            string
		userRecordCount int
		explicitHashKey string
	}{
		{
			name:            "Drain empty aggregator causes no error",
			userRecordCount: 0,
		},
		{
			name:            "Aggregates user records",
			userRecordCount: 50,
		},
		{
			name:            "Aggregates user records with explicitHashKey",
			userRecordCount: 50,
			explicitHashKey: "123",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var explicitHashKey *string = nil
			if tc.explicitHashKey != "" {
				explicitHashKey = &tc.explicitHashKey
			}
			a := NewAggregator(explicitHashKey)

			userRecords := make([]UserRecord, tc.userRecordCount)
			for i := 0; i < tc.userRecordCount; i++ {
				pk := strconv.Itoa(i)
				data := []byte("hello-" + pk)
				ur := NewDataRecord(data, pk)
				userRecords[i] = ur
				a.Put(ur)
			}

			record, err := a.Drain()
			require.NoError(t, err)
			if tc.userRecordCount == 0 {
				require.Nil(t, record)
				return
			}

			require.Equal(t, 0, a.Size()+a.Count(), "size and count should be cleared on drain")
			require.True(t, isAggregated(record.Entry), "should return an agregated record")
			require.Equal(t, "0", *record.Entry.PartitionKey, "Entry should user first PartitionKey")
			if explicitHashKey == nil {
				require.Nil(t, record.Entry.ExplicitHashKey)
			} else {
				require.NotNil(t, record.Entry.ExplicitHashKey)
				require.Equal(t, *explicitHashKey, *record.Entry.ExplicitHashKey, "Entry should contain ExplicitHashKey")
			}
			records := extractRecords(record.Entry)
			require.Equal(t, tc.userRecordCount, len(records), "AggregatedRecord count does not match")

			for i := 0; i < tc.userRecordCount; i++ {
				var (
					expectedPartitionKey = strconv.Itoa(i)
					expectedData         = fmt.Sprintf("hello-%d", i)
					rdata                = string(records[i].Data)
					urdata               = string(userRecords[i].Data())
					rpartitionKey        = *records[i].PartitionKey
					urpartitionKey       = userRecords[i].PartitionKey()
				)
				require.Equal(t, expectedData, rdata, "`Data` field contains invalid value")
				require.Equal(t, urdata, rdata, "Record data does not match UserRecord data")
				require.Equal(t, expectedPartitionKey, rpartitionKey, "`PartitionKey` field contains invalid value")
				require.Equal(t, urpartitionKey, rpartitionKey, "Record partition key does not match UserRecord partition key")
			}
		})
	}
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

func TestAggregatorWillOverflow(t *testing.T) {
	a := NewAggregator(nil)

	record := NewDataRecord(mockData("", maxRecordSize/2), "foo")
	require.False(t, a.WillOverflow(record))

	a.Put(record)
	record = NewDataRecord(mockData("", maxRecordSize/2), "foo")
	require.True(t, a.WillOverflow(record))
}

func TestAggregatorUserRecordNBytes(t *testing.T) {
	a := NewAggregator(nil)

	record := NewDataRecord(mockData("", 10), "foo")
	nbytes, includesPk := a.userRecordNBytes(record)
	expectedNBytes := 1 + partitionKeyIndexSize + 10 + 3
	require.Equal(t, expectedNBytes, nbytes)
	require.True(t, includesPk)

	a.Put(record)

	record = NewDataRecord(mockData("", 20), "foo")
	nbytes, includesPk = a.userRecordNBytes(record)
	expectedNBytes = 1 + partitionKeyIndexSize + 20
	require.Equal(t, expectedNBytes, nbytes)
	require.False(t, includesPk)
}
