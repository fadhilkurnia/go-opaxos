package paxi

import (
	"bufio"
	"github.com/ailidani/paxi/log"
	"io"
	"net"
	"net/rpc"
	"net/url"
)

// message format expected by rpc server:
// message type (1 byte), message_id (4 bytes), message_length (4 bytes), data (message_length bytes)
// message format returned by rpc server:
// message type (1 byte), message_id (4 bytes), message_length (4 bytes), data (message_length bytes)

// ClientBytesCommand wraps BytesCommand ([]byte) with pointer to RPCMessage
type ClientBytesCommand struct {
	*BytesCommand
	*RPCMessage
}

// rpc runs rpc server for client-replica communication
func (n *node) rpc() {
	httpURL, err := url.Parse(config.RPCAddrs[n.id])
	if err != nil {
		log.Fatal("http url parse error: ", err)
	}
	port := ":" + httpURL.Port()

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to start rpc server: %v", err)
	}
	defer listener.Close()

	log.Infof("listening on port %s for client-server rpc requests", port)
	// accept any incoming tcp connection request from client
	for {
		// Accept() blocks until it receive new connection request from client
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			log.Errorf("failed to accept client init connection request %v", err)
			continue
		}
		log.Debugf("client connection accepted: %s", conn.RemoteAddr())
		go n.handleClientRequests(conn)
	}
}

func (n *node) handleClientRequests(conn net.Conn) {
	defer conn.Close()

	clientReader := bufio.NewReader(conn)
	clientWriter := bufio.NewWriter(conn)

	for {
		// clientReader blocks until bytes are available in the underlying socket
		// thus, it is fine to have this busy-loop
		// read the command type, then the command itself.

		clientReqFirstByte, err := clientReader.ReadByte()
		if err != nil {
			if err == io.EOF {
				log.Debugf("client is terminating the connection")
				break
			}
			log.Errorf("fail to read byte from client, terminating the connection. %v", err)
			break
		}

		switch clientReqFirstByte {
		case COMMAND:
			msg, err := NewRPCMessageWithReply(clientReader, clientWriter)
			if err != nil {
				break
			}
			log.Debugf("receiving rpc message: %s", msg)

			cmd := BytesCommand(msg.Data)
			n.MessageChan <- &ClientBytesCommand{&cmd, msg}
		}

	}
}
