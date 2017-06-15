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
	full         chan bool
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
		full:         make(chan bool),
	}

	// periodically flush to kinesis
	go func() {
		for {
			select {
			case <-w.flushTicker:
				if err := w.flush(); err != nil {
					w.lasterr = err
				}
			case <-w.full:
				if err := w.flush(); err != nil {
					w.lasterr = err
				}
			}
		}
	}()

	return w
}

func (w *kinesisWriter) Write(payload []byte) error {
	if w.lasterr != nil {
		err := w.lasterr
		w.lasterr = nil
		return err
	}

	w.messages <- payload

	// if the buffer is full, don't wait for next flush
	if len(w.messages) == bufsize {
		w.full <- true
	}
	return nil
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
