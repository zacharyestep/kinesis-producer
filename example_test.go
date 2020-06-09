package producer

import (
	"encoding/json"
	"log"
	"math/big"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/google/uuid"
)

func ExampleSimple() {
	logger := &StdLogger{log.New(os.Stdout, "", log.LstdFlags)}
	client := kinesis.New(session.New(aws.NewConfig()))
	pr := New(&Config{
		StreamName:   "test",
		BacklogCount: 2000,
		Client:       client,
		Logger:       logger,
	})

	pr.Start()

	failures := pr.NotifyFailures()

	// Handle failures
	go func() {
		for r := range failures {
			logger.Error("detected put failure", r.Err)
		}
	}()

	go func() {
		for i := 0; i < 5000; i++ {
			err := pr.Put([]byte("foo"), "bar")
			if err != nil {
				logger.Error("error producing", err)
			}
		}
	}()

	time.Sleep(3 * time.Second)
	pr.Stop()
}

func ExampleShardMap() {
	logger := &StdLogger{log.New(os.Stdout, "", log.LstdFlags)}
	client := kinesis.New(session.New(aws.NewConfig()))
	pr := New(&Config{
		StreamName:           "test",
		BacklogCount:         2000,
		Client:               client,
		GetShards:            GetKinesisShardsFunc(client, "test"),
		ShardRefreshInterval: 5 * time.Second,
		Logger:               logger,
	})

	pr.Start()

	failures := pr.NotifyFailures()

	// Handle failures
	go func() {
		for r := range failures {
			logger.Error("detected put failure", r.Err)
		}
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			pk := uuid.New().String()
			for j := 0; j < 5; j++ {
				err := pr.Put([]byte("foo"), pk)
				if err != nil {
					logger.Error("error producing", err)
				}
			}
		}
	}()

	time.Sleep(3 * time.Second)
	pr.Stop()
}

type myExampleUserRecord struct {
	Id   string `json:"id"`
	Key  string `json:"key"`
	Val  string `json:"val"`
	data []byte `json:"-"`
}

func (r *myExampleUserRecord) PartitionKey() string      { return r.Id }
func (r *myExampleUserRecord) ExplicitHashKey() *big.Int { return nil }
func (r *myExampleUserRecord) Data() []byte              { return r.data }
func (r *myExampleUserRecord) Size() int                 { return len(r.data) }

func newMyExampleUserRecord(key, val string) (*myExampleUserRecord, error) {
	r := &myExampleUserRecord{
		Id:  uuid.New().String(),
		Key: key,
		Val: val,
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	r.data = data
	return r, nil
}

func ExampleUserRecord() {
	logger := &StdLogger{log.New(os.Stdout, "", log.LstdFlags)}
	client := kinesis.New(session.New(aws.NewConfig()))
	pr := New(&Config{
		StreamName:           "test",
		BacklogCount:         2000,
		Client:               client,
		GetShards:            GetKinesisShardsFunc(client, "test"),
		ShardRefreshInterval: 5 * time.Second,
		Logger:               logger,
	})

	pr.Start()

	failures := pr.NotifyFailures()

	// Handle failures
	go func() {
		for r := range failures {
			logger.Error("detected put failure", r.Err)
		}
	}()

	go func() {
		for i := 0; i < 5000; i++ {
			record, err := newMyExampleUserRecord("foo", "bar")
			if err != nil {
				logger.Error("error creating user record", err)
			}
			err = pr.PutUserRecord(record)
			if err != nil {
				logger.Error("error producing", err)
			}
		}
	}()

	time.Sleep(3 * time.Second)
	pr.Stop()
}
