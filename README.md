# replay-logs

To transfer AWS CloudWatch log streams to AWS Kinesis Data Streams one can create AWS CloudWatch logs subscription, that
will take care of exporting the log streams into Kinesis. It might happen that, e.g. due to malfunction of the system
reading the Kinesis Data Streams, some log streams need to be migrated later again. If the defined Kinesis Data Stream
retention period is expired, the logs must be resent to Kinesis from CloudWatch logs manually.

This application will do exactly that. Given log event selectors like start/end timestamps and log filter pattern, the 
application query the CloudWatch logs and puts all returned log events as Kinesis Data Stream records into defined
stream, as they would have published by CloudWatch logs subscription.

## Build

```
$ git clone https://github.com/hhamalai/replay-logs.git
$ cd replay-logs
$ go build
```

## Usage
```
# You need to provide your AWS Credentials e.g. as environment variables. 
# You can use tool like aws-vault to provide this easily https://github.com/99designs/aws-vault

# Transfer logs from a time window into Kinesis stream called "LogDataStream"
$ ./replay-logs -start-time "2020-04-24 16:35:05" -end-time "2020-04-24 23:59:59" -kinesis-stream LogDataStream

# Transfer logs from starting from start-time timestamp and matching to filter-pattern
# into Kinesis Stream called "LogDataStream"
$ ./replay-logs \
  -start-time "2020-04-24 16:35:05" \
  -end-time "2020-04-24 23:59:59" \
  -filter-pattern '{ $.kubernetes.labels.app = "my-application" }' \ 
  -kinesis-stream LogDataStream
```

