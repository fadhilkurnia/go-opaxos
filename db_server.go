package paxi

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/ailidani/paxi/lib"
	"github.com/ailidani/paxi/log"
	"io"
	"net"
	"net/rpc"
	"net/url"
	"os"
	"os/signal"
	"syscall"
)

func (n *node) runTCPServer() {
	rpcAddress, err := url.Parse(config.PublicAddrs[n.id])
	if err != nil {
		log.Fatalf("host public address parse error: %s", err)
	}
	port := ":" + rpcAddress.Port()

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to start tcp host server: %s", err)
	}
	defer listener.Close()

	log.Infof("listening on port %s for client-node communication", port)

	// accept any incoming TCP connection request from client
	for {
		// Accept() blocks until it receive new connection request from client
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			log.Errorf("failed to accept client init connection request %v", err)
			continue
		}
		log.Debugf("client connection accepted, serving with client type: %s", *ClientType)

		go n.handleIncomingCommands(conn)
	}
}

func (n *node) runUnixServer() {
	socketAddress := fmt.Sprintf("/tmp/rpc_%s.sock", GetConfig().GetPublicHostPort(n.id))

	_ = os.Remove(socketAddress)

	// remove socket file when the node is killed
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		os.Remove(socketAddress)
		os.Exit(1)
	}()

	listener, err := net.Listen("unix", socketAddress)
	if err != nil {
		log.Fatalf("failed to start unix host server: %v", err)
	}
	defer listener.Close()

	log.Infof("listening on socket address %s for client-node communication", socketAddress)

	// accept any incoming unix (uds) connection request from client
	for {
		// Accept() blocks until it receive new connection request from client
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			log.Errorf("failed to accept client init connection request %v", err)
			continue
		}
		log.Debugf("client connection accepted, serving with client type: %s", *ClientType)

		go n.handleIncomingCommands(conn)
	}
}

func (n *node) runUDPServer() {
	rpcAddress, err := url.Parse(config.PublicAddrs[n.id])
	if err != nil {
		log.Fatalf("host public address parse error: %s", err)
	}
	port := ":" + rpcAddress.Port()

	s, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		log.Fatalf("failed to resolve host address: %s", err)
	}

	conn, err := net.ListenUDP("udp", s)
	if err != nil {
		log.Fatalf("failed to start udp host server: %s", err)
	}
	defer conn.Close()

	log.Infof("listening on port %s for client-node communication", port)

	// accept any incoming data
	go n.handleIncomingCommands(conn)
}

func (n *node) handleIncomingCommands(conn net.Conn) {
	defer conn.Close()

	clientReader := bufio.NewReader(conn)
	clientWriter := bufio.NewWriter(conn)
	clientWriterChan := make(chan *CommandReply)

	// running the sender for sending command's reply to client
	go n.runResponseSender(clientWriterChan, clientWriter)

	var err error
	var firstByte byte
	var cmdLenBuff [4]byte
	var cmdLen uint32

	acceptableCommandType := lib.NewSet()
	acceptableCommandType.Add(TypeDBGetCommand)
	acceptableCommandType.Add(TypeDBPutCommand)
	acceptableCommandType.Add(TypeAdminCrashCommand)
	acceptableCommandType.Add(TypeOtherCommand)
	acceptableCommandType.Add(TypeGetMetadataCommand)
	acceptableCommandType.Add(TypeEmulatedCommand)
	acceptableCommandType.Add(TypeBeLeaderCommand)

	for {
		// clientReader blocks until bytes are available in the underlying socket
		// thus, it is fine to have this busy-loop
		// read the command type, then the command itself.

		firstByte, err = clientReader.ReadByte()
		if err != nil {
			if err == io.EOF {
				log.Debugf("client is terminating the connection")
				break
			}
			log.Errorf("fail to read byte from client, terminating the connection. %v", err)
			break
		}

		if !acceptableCommandType.Has(firstByte) {
			log.Errorf("unsupported client's command: %d, supported commands: %s", firstByte, acceptableCommandType)
			break
		}

		//log.Debugf("waiting length ...")
		_, err = io.ReadAtLeast(clientReader, cmdLenBuff[0:4], 4)
		if err != nil {
			log.Errorf("fail to read command length %v", err)
			break
		}
		cmdLen = binary.BigEndian.Uint32(cmdLenBuff[0:4])
		cmdBuff := make([]byte, cmdLen)

		//log.Debugf("waiting command buffer ...")
		_, err = io.ReadAtLeast(clientReader, cmdBuff[:cmdLen], int(cmdLen))
		if err != nil {
			log.Errorf("fail to read command data %v", err)
			break
		}
		//log.Debugf("len=%x(%d) data=%x", cmdLenBuff, cmdLen, cmdBuff)

		// handle AdminCommands
		if AdminCommandTypes.Has(firstByte) {
			n.handleIncomingAdminCommands(firstByte, cmdBuff, clientWriterChan)
			continue
		}

		cmd := &ClientCommand{
			CommandType:     firstByte,
			RawCommand:      cmdBuff,
			ReplyStream:     clientWriterChan,
		}

		log.Debugf("get command from client %x", cmdBuff)
		if len(n.MessageChan) == cap(n.MessageChan) {
			log.Warningf("Channel for client's command is full (len=%d)", len(n.MessageChan))
		}

		n.MessageChan <- cmd
	}

	if err != nil && err != io.EOF {
		log.Errorf("exiting from reader loop %s, terminating client connection", err.Error())
	}
}

func (n *node) runResponseSender(replyStream chan *CommandReply, clientWriter *bufio.Writer) {
	for rep := range replyStream {
		cmdRepBuff := rep.Serialize()
		cmdRepLenBuff := make([]byte, 4)
		cmdRepLen := len(cmdRepBuff)

		if err := clientWriter.WriteByte(TypeCommandReply); err != nil {
			log.Error(err)
			continue
		}
		binary.BigEndian.PutUint32(cmdRepLenBuff, uint32(cmdRepLen))
		nn, err := clientWriter.Write(cmdRepLenBuff)
		if err != nil {
			log.Error(err)
			continue
		}
		if nn != len(cmdRepLenBuff) {
			log.Errorf("short write: %d, expected %d", nn, len(cmdRepLenBuff))
			continue
		}

		nn, err = clientWriter.Write(cmdRepBuff)
		if err != nil {
			log.Error(err)
			continue
		}
		if nn != len(cmdRepBuff) {
			log.Errorf("short write: %d, expected %d", nn, len(cmdRepBuff))
			continue
		}
		if ferr := clientWriter.Flush(); ferr != nil {
			log.Error(ferr)
			continue
		}
	}
}

func (n *node) handleIncomingAdminCommands(cmdType byte, cmdBuff []byte, replyStreamChan chan *CommandReply) {
	cmdReply := &CommandReply{
		Code: CommandReplyOK,
	}

	switch cmdType {
	case TypeAdminCrashCommand:
		cmd := DeserializeAdminCommandCrash(cmdBuff)
		log.Debugf("crashing this node for %d seconds", cmd.Duration)
		n.Socket.Crash(int(cmd.Duration))

	case TypeAdminDropCommand:
		cmd := DeserializeAdminCommandDrop(cmdBuff)
		log.Debugf("dropping all messages to %s for %d seconds", cmd.TargetNode, cmd.Duration)
		n.Drop(cmd.TargetNode, int(cmd.Duration))

	case TypeAdminDelayCommand:
		panic("handler for delay command is still unimplemented")

	case TypeAdminSlowCommand:
		panic("handler for slow command is still unimplemented")

	case TypeAdminPartitionCommand:
		panic("handler for partition command is still unimplemented")

	case TypeBeLeaderCommand:
		n.MessageChan <- BeLeaderRequest{}
		return

	default:
		log.Errorf("unknown AdminCommand %d", cmdType)

	}

	// send the response back to client
	replyStreamChan <- cmdReply
}
