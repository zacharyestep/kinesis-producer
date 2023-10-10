package producer

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	k "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/stretchr/testify/require"
	"github.com/zacharyestep/kinesis-producer/deaggregation"
)

func TestSizeAndCount(t *testing.T) {
	a := NewAggregator(nil)
	require.Equal(t, 0, a.Size()+a.Count(), "size and count should equal to 0 at the beginning")

	var (
		keyCount      = 100
		recordsPerKey = 10

		keySize = 32
		// message wire/index type + varint of keysize + keysize
		keySizeProto = 1 + 1 + keySize

		keyIndexSizeProto = 1 + 1
		dataSize          = 512
		// message wire/index type + varint of datasize + datasize
		dataSizeProto = 1 + 2 + dataSize

		recordSizeProto = 1 + 2 + keyIndexSizeProto + dataSizeProto

		expectedCount = keyCount * recordsPerKey
		expectedSize  = (keySizeProto * keyCount) + (recordSizeProto * expectedCount)
	)

	for k := 0; k < keyCount; k++ {
		key := fmt.Sprintf("%0[2]*[1]d", k, keySize)
		for i := 0; i < recordsPerKey; i++ {
			a.Put(NewDataRecord(make([]byte, dataSize), key))
		}
	}

	require.Equal(t, expectedCount, a.Count(), "count should be equal to the number of Put calls")
	require.Equal(t, expectedSize, a.Size(), "size should equal to size of data, partition-keys, partition key indexes, and protobuf wire type")
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
			require.True(t, deaggregation.IsAggregatedRecord(record.Entry.Data), "should return an agregated record")
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

func extractRecords(entry types.PutRecordsRequestEntry) (out []*k.PutRecordsRequestEntry) {
	dest, err := deaggregation.Unmarshal(entry.Data)
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
