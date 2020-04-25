package main

import (
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"os"
	"time"
)

const DatetimeFormat = "2006-01-02T15:04:05"
const LogSliceMaxCapacity = 500

func parseDateTime(t0 *string) (*time.Time, error) {
	parsed, err := time.Parse(DatetimeFormat, *t0)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func main() {
	region := flag.String("region", os.Getenv("AWS_REGION"), "AWS region where logs are stored")
	logGroup := flag.String("log-group", "/eks/EKS-Cluster/containers", "AWS CloudWatch Log Group where logs are stored")
	sTime := flag.String("start-time", "", "Starting time for the time window from which the logs are replayed")
	eTime := flag.String("end-time", "", "Ending time for the time window from which the logs are replayed")
	kinesisStream := flag.String("kinesis-stream", "LogDataStream", "Name of Kinesis stream the logs are transferred to")
	filterPattern := flag.String("filter-pattern", "\"*\"", "CloudWatch logs filter pattern to select exported logs")
	flag.Parse()

	if *sTime == "" {
		fmt.Println("Start time must be set with command line argument")
		flag.Usage()
		return
	}

	var startTime *int64 = nil
	e0, err := parseDateTime(sTime)
	if err != nil {
		fmt.Println(err)
		return
	}
	startTime = aws.Int64(e0.Unix() * 1000)

	var endTime *int64 = nil
	if *eTime != "" {
		e0, err := parseDateTime(eTime)
		if err != nil {
			fmt.Println(err)
			return
		}
		endTime = aws.Int64(e0.Unix() * 1000)
	}

	fmt.Println("Filtering log group and forwarding filtered events to kinesis stream")
	fmt.Printf("\tLog group: %s\n", *logGroup)
	fmt.Printf("\tKinesis stream name: %s\n", *kinesisStream)
	fmt.Printf("\tFilter pattern: %s\n", *filterPattern)
	fmt.Printf("\tRegion: %s\n", *region)
	fmt.Printf("\tStart time: %s (%d)\n", *sTime, *startTime)

	if endTime != nil {
		fmt.Printf("\tEnd time: %s (%d) \n", *eTime, *endTime)
	}

	mySession := session.Must(session.NewSession())
	config := aws.NewConfig().WithRegion(aws.StringValue(region))

	cloudwatchSvc := cloudwatchlogs.New(mySession, config)
	kinesisSvc := kinesis.New(mySession, config)


	fmt.Println("################ Starting replay ################")
	err = cloudwatchSvc.FilterLogEventsPages(&cloudwatchlogs.FilterLogEventsInput{
		LogGroupName:  logGroup,
		FilterPattern: filterPattern,
		StartTime:     startTime,
		EndTime:       endTime,
	}, func(filteredEvents *cloudwatchlogs.FilterLogEventsOutput, lastPage bool) bool {
		events := filteredEvents.Events
		eventsLen := len(events)
		if eventsLen == 0 {
			return true
		}
		for j := 0; j <= eventsLen/LogSliceMaxCapacity; j++ {
			var records []*kinesis.PutRecordsRequestEntry
			var sliceEnd = j*LogSliceMaxCapacity + LogSliceMaxCapacity
			if sliceEnd > eventsLen {
				sliceEnd = eventsLen
			}
			var buf bytes.Buffer
			for i, event := range filteredEvents.Events[j*LogSliceMaxCapacity : sliceEnd] {
				buf.Reset()
				com := gzip.NewWriter(&buf)
				_, err := com.Write([]byte(*event.Message))
				if err != nil {
					fmt.Println(err)
					return false
				}
				err = com.Close()
				if err != nil {
					fmt.Println(err)
					return false
				}
				if i == 0 {
					fmt.Println("Processing messages from", time.Unix(*event.Timestamp/1000, 0))
				}
				records = append(records, &kinesis.PutRecordsRequestEntry{
					Data:         buf.Bytes(),
					PartitionKey: event.LogStreamName,
				})
			}

			_, err := kinesisSvc.PutRecords(&kinesis.PutRecordsInput{
				Records:    records,
				StreamName: kinesisStream,
			})
			if err != nil {
				fmt.Printf("Failed to publish logs to Kinesis: %v\n", err)
				return false
			}
		}
		return true
	})

	if err != nil {
		fmt.Printf("Log replay failed: %s\n", err)
	}
}
