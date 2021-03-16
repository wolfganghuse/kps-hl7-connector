package connector

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
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
	netData, err := bufio.NewReader(conn).ReadString(0x10)
	if err != nil {
		log.Printf("error on NewReader")
		return "", err
	}
	_ ,err = conn.Write([]byte("Message received."))
	if err != nil {
		log.Printf("error on closing")
		return "", err
	}
	
	//temp := strings.TrimSpace(string(netData))
	temp, err := ReadMessage(netData)
	if err != nil {
		log.Printf("error on readmessage")
		log.Fatal(err)
	}
	log.Printf(string(temp))
	err = conn.Close()
	if err != nil {
		log.Printf("error on closing")
		return "", err
	}
	log.Printf("Handle Connection closed")
	return string(temp), nil
}

// Processing Message MLLP
func ReadMessage(message string) ([]byte, error) {
	log.Printf("1")
	r := bufio.NewReader(strings.NewReader(message))
	log.Printf("2")
	c, err := r.ReadByte()
	log.Printf("3")
	if err != nil {
	  return nil, err
	}
    log.Printf("4")
	if c != byte(0x0b) {
		return nil, fmt.Errorf("invalid header found; expected 0x0b but got %02x", c)
	  }
	  log.Printf("5")
	d, err := r.ReadBytes(byte(0x1c))
	if err != nil {
	  return nil, err
	}
	log.Printf("6")
	if len(d) < 2 {
	  return nil, fmt.Errorf("content including boundary should be at least two bytes long; instead was %02x", len(d))
	}
	log.Printf("7")
	if d[len(d)-2] != 0x0d {
	  return nil, fmt.Errorf("content should end with 0x0d; instead was %02x", d[len(d)-2])
	}
	log.Printf("8")
	t, err := r.ReadByte()
	if err != nil {
	  return nil, err
	}
	log.Printf("9")
	if t != byte(0x0d) {
	  return nil, fmt.Errorf("invalid trailer found; expected 0x0d but got %02x", t)
	}
	log.Printf("10")
	log.Printf(string(d[0 : len(d)-2]))
	return d[0 : len(d)-2], nil
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
