package main

import (
	"bytes"
	"fmt"
	"time"
)

func readAmqp(buf []byte) error {
	time.Sleep(100 * time.Millisecond)
	bytes.NewBuffer(buf).WriteString(fmt.Sprintf("hello kinesis! the time now is %v", time.Now()))
	return nil
}
