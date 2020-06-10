# Amazon kinesis producer [![Build status][travis-image]][travis-url] [![License][license-image]][license-url] [![GoDoc][godoc-img]][godoc-url]
> A KPL-like batch producer for Amazon Kinesis built on top of the official Go AWS SDK
and using the same aggregation format that [KPL][kpl-url] use.

### Useful links
- [Documentation][godoc-url]
- [Aggregation format][aggregation-format-url]
- [Considerations When Using KPL Aggregation][kpl-aggregation]
- [Consumer De-aggregation][de-aggregation]

### Example
```go
package main

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/a8m/kinesis-producer"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func main() {
	client := kinesis.New(session.New(aws.NewConfig()))
	pr := producer.New(&producer.Config{
		StreamName:   "test",
		BacklogCount: 2000,
		Client:       client
	})

	pr.Start()

	failures := pr.NotifyFailures()

	// Handle failures
	go func() {
		for r := range failures {
			log.Error(r.Err)
		}
	}()

	go func() {
		for i := 0; i < 5000; i++ {
			err := pr.Put([]byte("foo"), "bar")
			if err != nil {
				log.WithError(err).Fatal("error producing")
			}
		}
	}()

	time.Sleep(3 * time.Second)
	pr.Stop()
}
```

### Shard Mapping

The `Producer` supports aggregation based on a shard map. UserRecords get mapped to a shard using the md5 hash of the Partition Key or a provided Explicit Hash Key. Records mapped to the same shard are aggregated together.

By default, shard mapping is disabled. To use the shard mapping feature, you need to set `Config.GetShards`. This function will be called on producer initialization to populate the shard map. You can optionally provide a refresh interval `Config.ShardRefreshInterval` to update the map.

This package provides a GetShards function `GetKinesisShardsFunc` that uses an AWS client to call the `ListShards` API to get the shard list.

**Note** At the time of writing, using the shard map feature adds significant overhead. Depending on the configuration and your record set, this can be more than 2x slower. Providing an explicit hash key for user records can help reduce this by quite a bit. Take a look at the benchmarks in `producer_test.go` for examples.

#### Example
```go
package main

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/google/uuid"
)

func main() {
	client := kinesis.New(session.New(aws.NewConfig()))
	pr := New(&Config{
		StreamName:           "test",
		BacklogCount:         2000,
		Client:               client,
		GetShards:            GetKinesisShardsFunc(client, "test"),
		ShardRefreshInterval: 5 * time.Second,
	})

	pr.Start()

	failures := pr.NotifyFailures()

	// Handle failures
	go func() {
		for r := range failures {
			log.Error(r.Err)
		}
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			pk := uuid.New().String()
			for j := 0; j < 5; j++ {
				err := pr.Put([]byte("foo"), pk)
				if err != nil {
					log.WithError(err).Fatal("error producing")
				}
			}
		}
	}()

	time.Sleep(3 * time.Second)
	pr.Stop()
}

```

### UserRecord interface

You can optionally define a custom struct that implements the `UserRecord` interface and put using `Producer.PutUserRecord`. The producer will hold onto the reference in case of any failures. Do not attempt to modify or use the reference after passing it to the producer until you receive it back in a failure record, otherwise thread issues may occur.

#### Example
```go
package main

import (
	"encoding/json"
	"math/big"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/google/uuid"
)

type myExampleUserRecord struct {
	Id   string `json:"id"`
	Key  string `json:"key"`
	Val  string `json:"val"`
	data []byte `json:"-"`
}

func (r *myExampleUserRecord) PartitionKey() string      { return r.id }
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

func main() {
	client := kinesis.New(session.New(aws.NewConfig()))
	pr := New(&Config{
		StreamName:           "test",
		BacklogCount:         2000,
		Client:               client,
		GetShards:            GetKinesisShardsFunc(client, "test"),
		ShardRefreshInterval: 5 * time.Second,
	})

	pr.Start()

	failures := pr.NotifyFailures()

	// Handle failures
	go func() {
		for r := range failures {
			log.Error(r.Err)
		}
	}()

	go func() {
		for i := 0; i < 5000; i++ {
			record, err := newMyExampleUserRecord("foo", "bar")
			if err != nil {
				log.WithError(err).Fatal("error creating user record")
			}
			err = pr.PutUserRecord(record)
			if err != nil {
				log.WithError(err).Fatal("error producing")
			}
		}
	}()

	time.Sleep(3 * time.Second)
	pr.Stop()
}
```

### Specifying logger implementation
`producer.Config` takes an optional `logging.Logger` implementation.

#### Using a custom logger
```go
customLogger := &CustomLogger{}

&producer.Config{
  StreamName:   "test",
  BacklogCount: 2000,
  Client:       client,
  Logger:       customLogger,
}
```

#### Using logrus

```go
import (
	"github.com/sirupsen/logrus"
	producer "github.com/a8m/kinesis-producer"
	"github.com/a8m/kinesis-producer/loggers"
)

log := logrus.New()

&producer.Config{
  StreamName:   "test",
  BacklogCount: 2000,
  Client:       client,
  Logger:       loggers.Logrus(log),
}
```

kinesis-producer ships with three logger implementations.

- `producer.Standard` used the standard library logger
- `loggers.Logrus` uses logrus logger
- `loggers.Zap` uses zap logger

### License
MIT

[godoc-url]: https://godoc.org/github.com/a8m/kinesis-producer
[godoc-img]: https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square
[kpl-url]: https://github.com/awslabs/amazon-kinesis-producer
[de-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-kpl-consumer-deaggregation.html
[kpl-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-producer-adv-aggregation.html
[aggregation-format-url]: https://github.com/a8m/kinesis-producer/blob/master/aggregation-format.md
[license-image]: https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square
[license-url]: LICENSE
[travis-image]: https://img.shields.io/travis/a8m/kinesis-producer.svg?style=flat-square
[travis-url]: https://travis-ci.org/a8m/kinesis-producer

