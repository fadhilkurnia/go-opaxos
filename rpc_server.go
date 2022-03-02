package paxi

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/ailidani/paxi/log"
	"io"
	"net"
	"net/rpc"
	"net/url"
	"os"
	"os/signal"
	"syscall"
)

var ClientType = flag.String("client_type", "default", "client type that sending command to the server")

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

	if *ClientType == "unix" {
		n.rpcWithUDS()
		return
	}

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
		log.Debugf("client connection accepted: %s, serving with client type: %s", conn.RemoteAddr(), *ClientType)

		if *ClientType == "default" || *ClientType == "" || *ClientType == "callback" {
			go n.handleClientRequests(conn)
		} else if *ClientType == "pipeline" {
			go n.handleGenericCommand(conn)
		} else {
			panic("unknown rpc client type")
		}
	}
}

func (n *node) rpcWithUDS() {
	socketAddress := fmt.Sprintf("/tmp/rpc_%s.sock", GetConfig().GetRPCPort(n.id))

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		os.Remove(socketAddress)
		os.Exit(1)
	}()

	listener, err := net.Listen("unix", socketAddress)
	if err != nil {
		log.Fatalf("failed to start rpc server with uds: %v", err)
	}
	defer listener.Close()

	log.Infof("listening on socket address: %s for client-server rpc requests", socketAddress)

	for {
		// Accept() blocks until it receive new connection request from client
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			log.Errorf("failed to accept client init connection request %v", err)
			continue
		}
		log.Debugf("client connection accepted: %s, serving with client type: %s", conn.RemoteAddr(), *ClientType)

		if *ClientType == "unix" {
			go n.handleGenericCommand(conn)
		} else {
			panic("unknown rpc client type")
		}
	}

}

func (n *node) handleClientRequests(conn net.Conn) {
	defer conn.Close()

	var err error
	var clientReqFirstByte byte
	var msg *RPCMessage

	clientReader := bufio.NewReader(conn)
	clientWriter := bufio.NewWriter(conn)

	for err == nil {
		// clientReader blocks until bytes are available in the underlying socket
		// thus, it is fine to have this busy-loop
		// read the command type, then the command itself.

		clientReqFirstByte, err = clientReader.ReadByte()
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
			msg, err = NewRPCMessageWithReply(clientReader, clientWriter)
			if err != nil {
				break
			}
			log.Debugf("receiving rpc message: %s", msg)

			cmd := BytesCommand(msg.Data)
			n.MessageChan <- &ClientBytesCommand{&cmd, msg}
		}
	}
	if err != io.EOF {
		log.Errorf("exiting from reader loop %s, terminating client connection", err.Error())
	}
}

// handleGenericCommand handles request from generic client
// request message: GenericCommand
// response message: CommandResponse
// check server.SendCommand for the sender implementation
func (n *node) handleGenericCommand(conn net.Conn) {
	defer conn.Close()

	clientReader := bufio.NewReader(conn)
	clientWriter := bufio.NewWriter(conn)

	var err error

	for {
		// clientReader blocks until bytes are available in the underlying socket
		// thus, it is fine to have this busy-loop
		// read the command type, then the command itself.

		var firstByte byte
		firstByte, err = clientReader.ReadByte()
		if err != nil {
			if err == io.EOF {
				log.Debugf("client is terminating the connection")
				break
			}
			log.Errorf("fail to read byte from client, terminating the connection. %v", err)
			break
		}

		if firstByte == COMMAND {
			var reqLen uint32
			var reqLenBuff [4]byte
			var reqBuff []byte

			log.Debugf("waiting length ...")
			_, err = io.ReadAtLeast(clientReader, reqLenBuff[:], 4)
			if err != nil {
				log.Errorf("fail to read command length %v", err)
				break
			}

			reqLen = binary.BigEndian.Uint32(reqLenBuff[:])
			reqBuff = make([]byte, reqLen)
			log.Debugf("waiting command msgBuff ...")
			_, err = io.ReadAtLeast(clientReader, reqBuff[:reqLen], int(reqLen))
			if err != nil {
				log.Errorf("fail to read command data %v", err)
				break
			}

			log.Debugf("len=%x(%d) data=%x", reqLenBuff, reqLen, reqBuff)

			cmd := BytesCommand(reqBuff)
			rpcMsg := RPCMessage{
				MessageID:  0,
				MessageLen: reqLen,
				Data:       reqBuff,
				Reply:      clientWriter,
			}
			log.Debugf("get command from client %x", reqBuff)
			n.MessageChan <- &ClientBytesCommand{&cmd, &rpcMsg}
		}
	}
	if err != io.EOF {
		log.Errorf("exiting from reader loop %s, terminating client connection", err.Error())
	}
}
