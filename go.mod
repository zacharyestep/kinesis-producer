module github.com/achunariov/kinesis-producer

go 1.17

require (
	github.com/aws/aws-sdk-go v1.40.37
	github.com/aws/aws-sdk-go-v2 v1.9.0
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.1.1
	github.com/jpillora/backoff v1.0.0
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.2.2
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0
	google.golang.org/protobuf v1.27.1
)
