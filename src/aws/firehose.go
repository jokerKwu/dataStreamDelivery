package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	AwsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"golang.org/x/sync/errgroup"
	"time"
)

var awsClientFirehose *firehose.Client

type firehoseStream uint8

const (
	firehoseStreamAnalytics = firehoseStream(0)
)

type firehoseStreamType struct {
	name string
}

var firehoseStreamTypes map[firehoseStream]firehoseStreamType

type firehoseRecord struct {
	Record types.Record
	Stream firehoseStream
}

type AnalyticsLogAccess struct {
	UrlMethod string `json:"url"`
	Msg       string `json:"msg"`
}

func (d AnalyticsLogAccess) Put() {
	m, err := json.Marshal(d)
	if err != nil {
		fmt.Sprintf("err marshal log message - %+v", d)
		return
	}
	if chanFirehose != nil {
		chanFirehose <- firehoseRecord{
			Record: types.Record{Data: m},
			Stream: firehoseStreamAnalytics,
		}
	}
}

func putRecords(size int, channel chan firehoseRecord) {
	recordsRaw := make([]firehoseRecord, 0, size)
	for i := 0; i < size; i++ {
		recordsRaw = append(recordsRaw, <-channel)
	}
	recordsMap := make(map[firehoseStream][]types.Record)
	for _, recordRaw := range recordsRaw {
		_, exists := recordsMap[recordRaw.Stream]
		if exists {
			recordsMap[recordRaw.Stream] = append(recordsMap[recordRaw.Stream], recordRaw.Record)
		} else {
			recordsMap[recordRaw.Stream] = []types.Record{recordRaw.Record}
		}
	}

	for streamType, records := range recordsMap {
		stream, exists := firehoseStreamTypes[streamType]
		if !exists {
			fmt.Sprintf("firehose stream not existed - %d", uint8(streamType))
			continue
		}
		_, err := awsClientFirehose.PutRecordBatch(context.TODO(), &firehose.PutRecordBatchInput{
			DeliveryStreamName: aws.String(stream.name),
			Records:            records,
		})
		if err != nil {
			fmt.Sprintf("fail to send records to %s", stream.name)
		}
	}
	return
}

const (
	workerPeriod          = time.Millisecond * 500
	workerRotate          = time.Second * 10
	workerRecordLimit     = 500
	workerRecordThreshold = 1000
	chanSize              = 1500
)

func firehoseWorkerStart(channel chan firehoseRecord) {
	timeStart := time.Now()
	for {
		chanLength := len(channel)
		if chanLength >= workerRecordThreshold || time.Since(timeStart) >= workerRotate {
			if chanLength > 0 {
				g, _ := errgroup.WithContext(context.TODO())
				for i := 0; true; i++ {
					size := chanLength - (i * workerRecordLimit)
					if size < 1 {
						break
					}
					if size > workerRecordLimit {
						size = workerRecordLimit
					}
					g.Go(func() error {
						putRecords(size, channel)
						return nil
					})
				}
				_ = g.Wait()
			}
			timeStart = time.Now()
		}
		time.Sleep(workerPeriod)
	}
}

var chanFirehose chan firehoseRecord

func InitFirehose() error {

	endpointUrl, err := AwsGetParam(fmt.Sprintf("firehose_endpoint_%s", "dev"))
	if err != nil {
		return err
	}
	fmt.Println(endpointUrl)
	awsConfigFirebase, err := AwsConfig.LoadDefaultConfig(context.TODO(), AwsConfig.WithEndpointResolverWithOptions(
		aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				if service != firehose.ServiceID {
					return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
				}
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           endpointUrl,
					SigningRegion: "us-east-2",
				}, nil
			})),
	)
	if err != nil {
		return err
	}
	awsClientFirehose = firehose.NewFromConfig(awsConfigFirebase)

	firehoseStreamTypes = map[firehoseStream]firehoseStreamType{
		firehoseStreamAnalytics: {
			name: "local-analytics",
		},
	}

	chanFirehose = make(chan firehoseRecord, chanSize)
	go firehoseWorkerStart(chanFirehose)
	return nil
}
