package main

import (
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const (
	streamName    = "hackathon"
	flushInterval = 5 * time.Second
	bufsize       = 1000
)

type kinesisWriter struct {
	session      *kinesis.Kinesis
	partitionKey string
	messages     chan []byte
	flushTicker  <-chan time.Time
	lasterr      error
}

func dialKinesis() *kinesis.Kinesis {
	sess := session.Must(session.NewSession())
	creds := credentials.NewEnvCredentials()
	region := os.Getenv("AWS_DEFAULT_REGION")
	return kinesis.New(sess, &aws.Config{Credentials: creds, Region: &region})
}

func newKinesisWriter(partitionKey string) *kinesisWriter {
	session := dialKinesis()
	w := &kinesisWriter{
		session:      session,
		partitionKey: partitionKey,
		messages:     make(chan []byte, bufsize),
		flushTicker:  time.Tick(flushInterval),
	}

	go func() {
		for range w.flushTicker {
			w.flush()
		}
	}()

	return w
}

func (w *kinesisWriter) Write(payload []byte) {
	w.messages <- payload
	if len(w.messages) == bufsize {
		w.flush()
	}
}

func (w *kinesisWriter) flush() error {
	flushLen := len(w.messages)
	log.Printf("flush: flushing %d messages", flushLen)
	records := make([]*kinesis.PutRecordsRequestEntry, flushLen)
	for i := range records {
		records[i] = new(kinesis.PutRecordsRequestEntry)
		records[i].SetData(<-w.messages)
		records[i].SetPartitionKey(w.partitionKey)
	}
	input := new(kinesis.PutRecordsInput)
	input.SetStreamName(streamName)
	input.SetRecords(records)
	out, err := w.session.PutRecords(input)
	if err != nil {
		return err
	}
	log.Printf("flush: flushed %d messages", len(out.Records))

	return nil
}
