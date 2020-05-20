package producer

type UserRecord interface {
	PartitionKey() string
	Data() []byte
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

func (r *DataRecord) PartitionKey() string { return r.partitionKey }
func (r *DataRecord) Data() []byte         { return r.data }
func (r *DataRecord) Size() int            { return len(r.data) }
