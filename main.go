package main

import "log"

const inputChannelSize = 1000

func main() {
	writeBuf := newKinesisWriter("partition1")

	buf := make([]byte, 8192)
	for {
		if err := readAmqp(buf); err != nil {
			log.Fatal(err)
		}
		writeBuf.Write(buf)
	}
}
