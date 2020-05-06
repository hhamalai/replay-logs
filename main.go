package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/sts"
	"os"
	"time"
)

const datetimeFormat = "2006-01-02 15:04:05"

func parseDateTime(t0 *string) (time.Time, error) {
	t, _ := time.LoadLocation("UTC")
	return time.ParseInLocation(datetimeFormat, *t0, t)
}

// Cloudwatch Log event
type LogEvent struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
	Message   string `json:"message"`
}

// CloudWatch Log group subscription filtered format to Kinesis
type Record struct {
	Owner               string      `json:"owner"`
	LogGroup            string      `json:"logGroup"`
	LogStream           string      `json:"logStream"`
	SubscriptionFilters []string    `json:"subscriptionFilters"`
	MessageType         string      `json:"messageType"`
	LogEvents           []LogEvent `json:"logEvents"`
}

func sendRecords(kinesisSvc *kinesis.Kinesis, records []*kinesis.PutRecordsRequestEntry, kinesisStream *string, tm *TimedMetric) error {
	var bytesSent = 0
	for _, record := range records {
		bytesSent += len(record.Data)
	}
	tm.Add(bytesSent)
	bs, output := tm.Value()
	if bs >= 650000 {
		time.Sleep(time.Millisecond*100)
	}
	results, err := kinesisSvc.PutRecords(&kinesis.PutRecordsInput{
		Records:    records,
		StreamName: kinesisStream,
	})

	if err != nil {
		return err
	}

	var backOff = time.Second
	for ; *results.FailedRecordCount != 0 ; {
		_, output = tm.Value()
		fmt.Printf("Some records (%d/%d) failed: %s retrying in %f seconds (%s)\n", *results.FailedRecordCount, len(records), err, backOff.Seconds(), output)
		time.Sleep(backOff)
		bytesSent = 0
		if backOff < 128 * time.Second {
			backOff *= 2
		}
		var toRetry []*kinesis.PutRecordsRequestEntry

		for i, result := range results.Records {
			if result.ErrorCode != nil {
				toRetry = append(toRetry, records[i])
			}
		}

		for _, record := range records {
			bytesSent += len(record.Data)
		}

		tm.Add(bytesSent)
		results, err = kinesisSvc.PutRecords(&kinesis.PutRecordsInput{
			Records:    records,
			StreamName: kinesisStream,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Partition list of log events into lists of of logevents, each holding at maximum bytes
func splitLogEvents(logEvents []LogEvent, bytesPerSlice int) [][]LogEvent {
	var bts = 0
	result := make([][]LogEvent, 0)
	var prevSliceStart = 0
	if len(logEvents) == 0 {
		return nil
	}
	for i, event := range logEvents {
		curEventSize := len(event.ID) + len(event.Message) + 8 // len(event.ID) + len(event.Message) + len(event.Timestamp)
		if bts + curEventSize >= bytesPerSlice {
			result = append(result, logEvents[prevSliceStart:i])
			prevSliceStart = i
			bts = 0
		}
		bts += curEventSize
	}
	result = append(result, logEvents[prevSliceStart:])
	return result
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
		os.Exit(1)
	}

	var startTime *int64 = nil
	e0, err := parseDateTime(sTime)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	startTime = aws.Int64(e0.Unix() * 1000)

	var endTime *int64 = nil
	if *eTime != "" {
		e0, err := parseDateTime(eTime)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
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
	stsSvc := sts.New(mySession, config)

	result, err := stsSvc.GetCallerIdentity(&sts.GetCallerIdentityInput{})

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	accountID := *result.Account

	tm := TimedMetric{
		measurements: make([]TimedBytes, 0),
		windowSize:   time.Second,
	}

	fmt.Println("################ Starting replay ################")
	err = cloudwatchSvc.FilterLogEventsPages(&cloudwatchlogs.FilterLogEventsInput{
		LogGroupName:  logGroup,
		FilterPattern: filterPattern,
		StartTime:     startTime,
		EndTime:       endTime,
	}, func(filteredEvents *cloudwatchlogs.FilterLogEventsOutput, lastPage bool) bool {
		eventsByStream := make(map[string][]LogEvent, 0)
		events := filteredEvents.Events
		eventsLen := len(events)

		// There may be outputs without any events when no log events are produced within the CW "index period"
		if eventsLen == 0 {
			return true
		}


		// Sort messages by log stream, so that they can be published into Kinesis stream in a single batch by stream
		for i, event := range filteredEvents.Events {
			if i == 0 {
				_, value := tm.Value()
				fmt.Println("Processing messages from", time.Unix(*event.Timestamp/1000, 0).UTC(), value)
			}
			eventsByStream[*event.LogStreamName] = append(eventsByStream[*event.LogStreamName], LogEvent{
				ID:        *event.EventId,
				Timestamp: *event.Timestamp,
				Message:   *event.Message,
			})
		}

		for stream, streamEvents := range eventsByStream {
			for i, events := range splitLogEvents(streamEvents, 600000) {
				var records []*kinesis.PutRecordsRequestEntry
				fmt.Println("Publishing to stream", stream, "batch #", i, "items", len(events))
				record := Record{
					Owner:               accountID,
					LogGroup:            *logGroup,
					LogStream:           stream,
					SubscriptionFilters: []string{"kinesis_logfilter_0"},
					MessageType:         "DATA_MESSAGE",
					LogEvents:           events,
				}
				encodedRecords, err := json.Marshal(record)
				if err != nil {
					fmt.Println(err)
					return false
				}
				var buf bytes.Buffer

				com := gzip.NewWriter(&buf)

				_, err = com.Write(encodedRecords); if err != nil {
					fmt.Println(err)
					return false
				}

				err = com.Close();
				if err != nil {
					fmt.Println(err)
					return false
				}

				records = append(records, &kinesis.PutRecordsRequestEntry{
					Data:         buf.Bytes(),
					PartitionKey: &stream,
				})

				err = sendRecords(kinesisSvc, records, kinesisStream, &tm)
				if err != nil {
					fmt.Println(err)
					return false
				}
			}
		}
		return true
	})

	if err != nil {
		fmt.Printf("Log replay failed: %s\n", err)
		os.Exit(1)
	}
}
