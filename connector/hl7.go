package connector

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"io"
	"bytes"
	"github.com/pkg/errors"
	"github.com/nutanix/kps-connector-go-sdk/transport"
)

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

type Writer struct {
	w io.Writer
}
func (w Writer) WriteMessage(b []byte) error {
    if _, err := w.w.Write([]byte{0x0b}); err != nil {
      return err
    }
  
    for len(b) > 0 {
      n, err := w.w.Write(b)
      if err != nil {
        return err
      }
  
      b = b[n:]
    }
  
    if _, err := w.w.Write([]byte{0x0d, 0x1c, 0x0d}); err != nil {
      return err
    }
  
    return nil
  }

// MLLPClient reads and writes MLLP.
type MLLPClient struct {
	r *bufio.Reader
	w io.Writer
}
const (
	mllpStartBlock     = 0x0b
	mllpEndBlock       = 0x1c
	mllpCarriageReturn = 0x0d
)

// NewMLLPClient returns a new MLLPClient.
func NewMLLPClient(c net.Conn) *MLLPClient {
	return &MLLPClient{bufio.NewReader(c), c}
}
func (c *MLLPClient) Read() ([]byte, error) {

	log.Printf("MLLP: reading")
	b, err := c.r.ReadByte()
	if err != nil {
		return nil, errors.Wrap(err, "cannot read the first byte of mllp message")
	}
	if b != mllpStartBlock {
		return nil, errors.New("mllp: protocol error, missing Start Block")
	}
	payload, err := c.r.ReadBytes(mllpEndBlock)
	if err != nil {
		return nil, errors.Wrap(err, "cannot read mllp message")
	}
	// Remove the mllpEndBlock
	payload = payload[:len(payload)-1]
	b, err = c.r.ReadByte()
	if err != nil {
		return nil, errors.Wrap(err, "cannot read the last byte of mllp message")
	}
	if b != mllpCarriageReturn {
		return nil, errors.New("mllp: protocol error, missing End Carriage Return")
	}
	return payload, nil
}

func handleConnection(c net.Conn) (string, error){
	fmt.Printf("Serving %s\n", c.RemoteAddr().String())

	for {
        r := NewMLLPClient(c)
        buf , err := r.Read()
        if err != nil {
            log.Printf(err.Error())
            return "", err
        }

        log.Printf("Sending ACK")
        b := bytes.NewBuffer(nil)
        w := NewWriter(b)
        err = w.WriteMessage([]byte("MSH|^~\\&|......|ACK|......"))
		if err != nil {
			log.Printf(err.Error())
		}
        _ , err = c.Write(b.Bytes())
		if err != nil {
			log.Printf(err.Error())
		}
        log.Printf(string(buf))
		return string(buf), nil
	}
}


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
	return []byte(msg), nil
}

// subscribe wraps the logic to connect or subscribe to the corresponding stream
// from the relevant client or service
func (c *consumer) subscribe(ctx context.Context, metadata *streamMetadata) error {
	log.Printf("Subcribed")
	port := ":" + metadata.ListenPort
	go func() {
		l, err := net.Listen("tcp4", port)
		if err != nil {
			log.Printf("cannot listen on port")
			return
		}
		
		for {
			log.Printf("waiting...")
			con, err := l.Accept()
			
			if err != nil {
				log.Printf("error on accepting message")
				// wait before retry?
				continue
			}
			for {
				res, err := handleConnection(con)
				log.Printf("after Handler")
				if err != nil {
					// do something
					log.Printf("after handler error")
					break
				}
				c.msgCh <- res
				log.Printf("after channel")
			}
		}
	}()
	log.Printf("exit")
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
