package connector

import (
	"context"
	"log"
	"net"
	"bufio"
	"strings"

	"github.com/nutanix/kps-connector-go-sdk/transport"
)

type streamMetadata struct {
	ListenPort	string
}

// mapToStreamMetadata translates the stream metadata into the corresponding streamMetadata struct
func mapToStreamMetadata(metadata map[string]interface{}) *streamMetadata {
	ListenPort := metadata["ListenPort"].(string)
	return &streamMetadata{
		ListenPort: ListenPort,
	}
}

type consumer struct {
	transport transport.Client
	metadata  *streamMetadata
	msgCh     chan string
}

// producer consumes the data from the relevant client or service and publishes them to KPS data pipelines
func newConsumer() *consumer {
	// TODO: Add the relevant clients and fields
	return &consumer{}
}

// nextMsg wraps the logic for consuming iteratively a transport.Message
// from the relevant client or service
func (c *consumer) nextMsg() ([]byte, error) {
	msg := <-c.msgCh
	return []byte(msg), nil}

func handleConnection(c net.Conn) {
	log.Printf("Serving %s\n", c.RemoteAddr().String())
	for {
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			log.Printf("error on NewReader")
			return
		}

		temp := strings.TrimSpace(string(netData))
		
		c.Write([]byte(string(temp)))
	}
	c.Close()
}

// subscribe wraps the logic to connect or subscribe to the corresponding stream
// from the relevant client or service
func (c *consumer) subscribe(ctx context.Context, metadata *streamMetadata) error {
	PORT := ":" + metadata.ListenPort
	l, err := net.Listen("tcp4", PORT)
	if err != nil {
		log.Printf("cannot listen on port")
		return nil
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			log.Printf("error on accepting message")
			return nil
		}
		go handleConnection(c)
	}
	return  nil
}

// producer produces data received from KPS data pipelines to the relevant client
type producer struct {
	// TODO: Add the relevant client and fields
}

func newProducer() *producer {
	// TODO: Add the relevant client and fields
	return &producer{}
}

func (p *producer) connect(ctx context.Context, metadata *streamMetadata) error {
	// TODO: Add the code for connecting to the relevant client or service for publishing to it
	return nil
}

// subscribeMsgHandler is a callback function that wraps the logic for producing a transport.Message
// from the data pipelines into the relevant client or service
func (*producer) subscribeMsgHandler(message *transport.Message) {
	// TODO: Add code for producing data into the relevant client
	log.Printf("msg received: %s", string(message.Payload))
}
