package producer

import (
	"fmt"
)

type ErrStoppedProducer struct {
	UserRecord
}

func (e *ErrStoppedProducer) Error() string {
	return "Unable to Put record. Producer is already stopped"
}

type ErrIllegalPartitionKey struct {
	UserRecord
}

func (e *ErrIllegalPartitionKey) Error() string {
	return fmt.Sprintf("Invalid parition key. Length must be at least 1 and at most 256: %s", e.PartitionKey())
}

type ErrRecordSizeExceeded struct {
	UserRecord
}

func (e *ErrRecordSizeExceeded) Error() string {
	return fmt.Sprintf("Data must be less than or equal to 1MB in size: %d", e.Size())
}

// Failure record type for failures from Kinesis PutRecords request
type FailureRecord struct {
	Err error
	// The PartitionKey that was used in the kinesis.PutRecordsRequestEntry
	PartitionKey string
	// The ExplicitHashKey that was used in the kinesis.PutRecordsRequestEntry. Will be the
	// empty string if nil
	ExplicitHashKey string
	// UserRecords that were contained in the failed aggregated record request
	UserRecords []UserRecord
}

func (e *FailureRecord) Error() string {
	return e.Err.Error()
}

type DrainError struct {
	Err error
	// UserRecords in the buffer when drain attempt was made
	UserRecords []UserRecord
}

func (e *DrainError) Error() string {
	return e.Err.Error()
}

type ShardBucketError struct {
	UserRecord
}

func (s *ShardBucketError) Error() string {
	if hk := s.ExplicitHashKey(); hk != nil {
		return fmt.Sprintf("ExplicitHashKey outside shard key range: %s", hk.String())
	}
	return fmt.Sprintf("PartitionKey outside shard key range: %s", s.PartitionKey())
}

type ShardRefreshError struct {
	Err error
}

func (s *ShardRefreshError) Error() string {
	return fmt.Sprintf("ShardRefreshError: %v", s.Err)
}
