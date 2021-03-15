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
	return &consumer{
		msgCh:     make(chan string ),
	}
}

// nextMsg wraps the logic for consuming iteratively a transport.Message
// from the relevant client or service
func (c *consumer) nextMsg() ([]byte, error) {
	msg := <-c.msgCh
	return []byte(msg), nil}

	func handleConnection(conn net.Conn) (string, error) {
		log.Printf("Serving %s\n", conn.RemoteAddr().String())
		netData, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			log.Printf("error on NewReader")
			return "", err
		}
		_ ,err = conn.Write([]byte("Message received."))
		if err != nil {
			log.Printf("error on closing")
			return "", err
		}
		// Send a response back to person contacting us.
		// Close the connection when you're done with it.
		temp := strings.TrimSpace(string(netData))
		log.Printf(temp)
		err = conn.Close()
		if err != nil {
			log.Printf("error on closing")
			return "", err
		}
		log.Printf("Handle Connection closed")
		return temp, nil
	}
	

// subscribe wraps the logic to connect or subscribe to the corresponding stream
// from the relevant client or service
func (c *consumer) subscribe(ctx context.Context, metadata *streamMetadata) error {
	log.Printf("Subcribe")
	port := ":" + metadata.ListenPort
	go func() {
		l, err := net.Listen("tcp4", port)
		if err != nil {
			log.Printf("cannot listen on port")
			return
		}
		
		for {
			log.Printf("Accept")
			con, err := l.Accept()
			log.Printf("Accepted")
			
			if err != nil {
				log.Printf("error on accepting message")
				// wait before retry?
				continue
			}
			res, err := handleConnection(con)
			log.Printf("after Handler")
			log.Printf(res)
			if err != nil {
				// do something
				continue
			}
			c.msgCh <- res
		}
	}()
	return nil
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
