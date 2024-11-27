package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	quic "github.com/quic-go/quic-go"
)

const (
	CONNECT     byte = 0x10
	CONNACK     byte = 0x20
	PUBLISH     byte = 0x30
	PUBACK      byte = 0x40
	PUBREC      byte = 0x50
	PUBREL      byte = 0x60
	PUBCOMP     byte = 0x70
	SUBSCRIBE   byte = 0x80
	SUBACK      byte = 0x90
	UNSUBSCRIBE byte = 0xA0
	UNSUBACK    byte = 0xB0
	PINGREQ     byte = 0xC0
	PINGRESP    byte = 0xD0
	DISCONNECT  byte = 0xE0
	AUTH        byte = 0xF0
)

// Error codes
const (
	// Common error codes
	UnspecifiedError                    byte = 0x80
	MalformedPacket                     byte = 0x81
	ProtocolError                       byte = 0x82
	ImplementationSpecificError         byte = 0x83
	UnsupportedProtocolVersion          byte = 0x84
	ClientIdentifierNotValid            byte = 0x85
	BadUserNameOrPassword               byte = 0x86
	NotAuthorized                       byte = 0x87
	ServerUnavailable                   byte = 0x88
	ServerBusy                          byte = 0x89
	Banned                              byte = 0x8A
	ServerShuttingDown                  byte = 0x8B
	BadAuthenticationMethod             byte = 0x8C
	KeepAliveTimeout                    byte = 0x8D
	SessionTakenOver                    byte = 0x8E
	TopicFilterInvalid                  byte = 0x8F
	TopicNameInvalid                    byte = 0x90
	PacketIdentifierInUse               byte = 0x91
	PacketIdentifierNotFound            byte = 0x92
	ReceiveMaximumExceeded              byte = 0x93
	TopicAliasInvalid                   byte = 0x94
	PacketTooLarge                      byte = 0x95
	MessageRateTooHigh                  byte = 0x96
	QuotaExceeded                       byte = 0x97
	AdministrativeAction                byte = 0x98
	PayloadFormatInvalid                byte = 0x99
	RetainNotSupported                  byte = 0x9A
	QoSNotSupported                     byte = 0x9B
	UseAnotherServer                    byte = 0x9C
	ServerMoved                         byte = 0x9D
	SharedSubscriptionsNotSupported     byte = 0x9E
	ConnectionRateExceeded              byte = 0x9F
	MaximumConnectTime                  byte = 0xA0
	SubscriptionIdentifiersNotSupported byte = 0xA1
	WildcardSubscriptionsNotSupported   byte = 0xA2
)

const (
	DefaultPacketSize = 16 * 1024
	maxAckBufSize     = 1024
	ReadTimeout       = 60 * time.Second
	BatchSize         = 50
)

type TrieNode struct {
	children map[string]*TrieNode
	clients  map[*MQTTClient]bool // Using a map to store clients for simplicity, could be a list or another data structure
}

// Trie represents the Trie data structure for MQTT topic filters
type Trie struct {
	root *TrieNode
}

type Packet struct {
	cmd                 byte
	header              byte
	qos                 byte
	retain              bool
	duplicate           bool
	packetIdentifier    uint16
	hdrStart            []byte
	payloadStart        int
	remainingLengthSize int
	remainingLength     int
	remainingBytes      []byte
}

type Subscriber struct {
	topicFilter string
	QoS         int
}

// Config holds the configuration for the MQTT client
type Properties struct {
	PayloadFormatIndicator     byte
	MessageExpiryInterval      uint32
	RequestProblemInformation  bool   // Request problem information
	WillDelayInterval          uint32 // Will delay interval
	RequestResponseInformation bool   // Request response information
	ContentType                string
	ResponseTopic              string
	CorrelationData            []byte
	SubscriptionIdentifier     uint32
	SessionExpiryInterval      uint32
	AssignedClientID           string
	ServerKeepAlive            uint16
	AuthenticationMethod       string
	AuthenticationData         []byte
	RequestProblemInfo         byte
	RequestResponseInfo        byte
	ResponseInfo               string
	ServerReference            string //Allow the Server to specify an alternate Server to use on CONNACK or DISCONNECT.
	ReasonString               string
	ReceiveMaximum             uint16
	TopicAliasMaximum          uint16
	TopicAlias                 uint16
	MaximumQoS                 byte
	RetainAvailable            byte
	UserProperty               map[string]string
	MaximumPacketSize          uint32
	WildcardSubscription       byte
	SubscriptionIDAvailable    byte
	SharedSubscription         byte
}

type ConnectProperties struct {
	SessionExpiryInterval uint32
	ReceiveMaximum        uint16
	MaximumPacketSize     uint32
	TopicAliasMaximum     uint16
	RequestResponseInfo   byte
	RequestProblemInfo    byte
	UserProperty          map[string]string
	AuthenticationMethod  string
	AuthenticationData    []byte
}

type ConnectAckProperties struct {
	SessionExpiryInterval uint32
	ReceiveMaximum        uint16
	MaximumQoS            byte
	RetainAvailable       byte
	MaximumPacketSize     uint32
	AssignedClientID      string
	TopicAliasMaximum     uint16
}

// MQTTClient represents an MQTT client connection
type MQTTClient struct {
	conn                net.Conn
	stream              quic.Stream
	connWs              *websocketConn
	connType            string
	cmdType             byte
	packetBuf           []byte
	cmdBuf              []byte
	partialBuf          []byte
	partialLength       int
	reader              *bufio.Reader
	remainingBytes      []byte
	hdrStart            []byte
	remainingLengthSize int
	remainingLength     int
	currentReadOffset   int
	tcpConn             *net.TCPConn
	keepAlive           int16 // Keep alive interval in seconds
	cleanStart          bool   // Clean start flag
	Username            string // Username
	password            string // Password
	protocolLevel       byte
	config              Properties
	server              *MQTTServer
	clientID            string
	publishCount        int64
	totalTimeMicroSec   int64
	memStats            runtime.MemStats
	mu                  sync.Mutex
	pendingAckMu        sync.Mutex
	pendingAck          map[uint16][]byte
	qosLevel            byte
	wg                  sync.WaitGroup
	pendingMessages     map[uint16]Packet
	willTopic           string
	willMessage         []byte
	willQoS             byte
	willRetain          bool
	pubPktsRcvd         int
	subPktsSent         int
	keepAliveExceeded   bool
	networkActivity     bool
	socketLock          sync.Mutex
}

type topicType struct {
	name     string
	isSystem bool
}

type websocketConn struct {
	*websocket.Conn
}

var (
	serverStartTime time.Time
	serverMutex     sync.Mutex
	publishReceived int
	subscribeSent   int
)

type TrieNodeMsgs struct {
	children map[string]*TrieNodeMsgs
	message  []byte
	isLeaf   bool
}

type TrieMsgs struct {
	root *TrieNodeMsgs
}

func NewTrieMsgs() *TrieMsgs {
	return &TrieMsgs{
		root: &TrieNodeMsgs{
			children: make(map[string]*TrieNodeMsgs),
		},
	}
}

// MQTTServer represents an MQTT server
type MQTTServer struct {
	packetBufPool       sync.Pool
	useSyncPool         bool
	Broker              string // Broker address (e.g., "localhost:1883")
	subscriptions       *Trie
	retainedMessages    *TrieMsgs
	matchingClients     sync.Map // Using sync.Map for concurrent access
	verbose             bool
	debug               bool
	mu                  sync.Mutex
	subscribers         map[string]Subscriber
	connections         map[string]net.Conn
	systemTopics        map[string]string
	allowedSystemTopics []string
	clientCount         int
	startTime           time.Time
	logger              *log.Logger
}

// NewMQTTServer creates a new MQTTServer instance
func NewMQTTServer() *MQTTServer {
	tClients := NewTrie()
	tMsgs := NewTrieMsgs()

	return &MQTTServer{
		subscriptions:       tClients,
		retainedMessages:    tMsgs,
		subscribers:         make(map[string]Subscriber),
		connections:         make(map[string]net.Conn),
		systemTopics:        make(map[string]string),
		allowedSystemTopics: make([]string, 0),
		packetBufPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, DefaultPacketSize)
			},
		},
	}
}

func NewTrie() *Trie {
	return &Trie{
		root: &TrieNode{
			children: make(map[string]*TrieNode),
			clients:  make(map[*MQTTClient]bool),
		},
	}
}

// NewMQTTClient creates a new MQTTClient instance for tcp connection
func NewMQTTClient(conn net.Conn, server *MQTTServer) *MQTTClient {
	return &MQTTClient{
		conn:            conn,
		reader:          bufio.NewReader(conn),
		server:          server,
		pendingAck:      make(map[uint16][]byte),
		partialBuf:      make([]byte, 1024),
		pendingMessages: make(map[uint16]Packet),
		cmdBuf:          make([]byte, maxAckBufSize),
	}
}

// NewMQTTClient creates a new MQTTClient instance websocket connection
func NewMQTTClientWs(conn *websocketConn, server *MQTTServer) *MQTTClient {
	return &MQTTClient{
		connWs:          conn,
		server:          server,
		pendingAck:      make(map[uint16][]byte),
		partialBuf:      make([]byte, 1024),
		pendingMessages: make(map[uint16]Packet),
		cmdBuf:          make([]byte, maxAckBufSize),
	}
}

func NewMQTTClientQUIC(stream quic.Stream, server *MQTTServer) *MQTTClient {
	return &MQTTClient{
		stream:          stream,
		server:          server,
		pendingAck:      make(map[uint16][]byte),
		partialBuf:      make([]byte, 1024),
		pendingMessages: make(map[uint16]Packet),
		cmdBuf:          make([]byte, maxAckBufSize),
	}
}

func main() {
	brokerAddress := flag.String("b", ":1883", "Broker address")
	quicAddress := flag.String("q", ":4242", "QUIC Server address")
	verbose := flag.Bool("v", false, "Verbose mode")
	debugOption := flag.Bool("d", false, "Debug trace mode")
	useSyncPool := flag.Bool("s", false, "Sync Pool or Allocate from Heap, default is from Heap")
	loggerPath := flag.String("l", "mqttserver.log", "Path of log file")
	flag.Parse()

	server := NewMQTTServer()
	server.verbose = *verbose
	server.debug = *debugOption
	server.startTime = time.Now()
	server.useSyncPool = *useSyncPool

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered from panic: %v\n", r)
			fmt.Println(string(debug.Stack()))
			pid := os.Getpid() // Get the current process ID
			fmt.Printf("Waiting for debugger to attach (PID: %d)...\n", pid)

			for {
				time.Sleep(time.Second) // Wait for 1 second in each iteration
			}
		}
	}()

	logfile := filepath.Join(os.TempDir(), *loggerPath)
	server.initLogger(logfile)

	server.allowedSystemTopics = []string{"$SYS/broker/clients/connected", "$SYS/broker/uptime"}

	fmt.Println("MQTT server is listening on port 1883...")

	// Setup listeners for IPv4 and IPv6
	ipv4Listener, err := net.Listen("tcp4", *brokerAddress)
	if err != nil {
		fmt.Printf("Error opening IPv4 listen socket on port %v: %v\n", *brokerAddress, err)
		os.Exit(1)
	}
	defer ipv4Listener.Close()

	ipv6Listener, err := net.Listen("tcp6", *brokerAddress)
	if err != nil {
		fmt.Printf("Error opening IPv6 listen socket on port %v: %v\n", *brokerAddress, err)
		os.Exit(1)
	}
	defer ipv6Listener.Close()

	// Start TCP/IP listener
	go server.runServer(ipv4Listener)
	go server.runServer(ipv6Listener)

	// Setup listeners for IPv4 and IPv6
	quicListener, err := quic.ListenAddr(*quicAddress, generateTLSConfig(), nil)
	if err != nil {
		fmt.Printf("Error opening QUIC listen on port %v: %v\n", *quicAddress, err)
		os.Exit(1)
	}

	go server.runServerQUIC(quicListener)

	// Start SSL listener
	//go startSSLListener(":1884")

	// Start WebSocket listener
	go server.startWSListener(":1885")

	// Start WebSocket Secure listener
	//go startWSSListener(":1886")

	// Start HTTP Dashboard listener
	//go startHTTPListener(":1887")

	// Start pprof server
	startPprofServer(":6060")

	if *debugOption {
		go initServerStats()
	}

	select {}
}

func (c *MQTTClient) processMQTTPackets() ([]Packet, []byte, int, bool, bool, string, error) {
	switch c.connType {
	case "tcp":
		return c.processMQTTPacketsTCP_QUIC()
	case "websocket":
		return c.processMQTTPacketsWs()
	case "quic":
		return c.processMQTTPacketsTCP_QUIC()
	}

	errMsg := fmt.Sprintf("Unsupported connection type: %s", c.connType)
	return nil, nil, 0, false, false, "", fmt.Errorf(errMsg)
}

// handleConnection handles the MQTT client commands
func (c *MQTTClient) handleClientCmds() {
	s := c.server
	s.mu.Lock()
	s.clientCount++
	s.mu.Unlock()

	if c.connType == "tcp" {
		defer c.conn.Close()
	} else if c.connType == "quic" {
		//defer c.stream.Close()
	}

	defer s.subscriptions.RemoveAll(c)

	defer func() {
		if s.useSyncPool {
			s.packetBufPool.Put(c.packetBuf)
		}
		runtime.GC()
		s.mu.Lock()
		s.clientCount--
		s.mu.Unlock()
	}()

	for {
		if c.keepAliveExceeded {
			fmt.Println("Exiting keep alive timeout exceeded", c.clientID)
			return
		}
		packets, data, dataSize, allPublish, allSameQoS, topicName, err := c.processMQTTPackets()
		if err != nil {
			if err.Error() == "read timeout" {
				continue
			}
			s.logger.Printf("Error processing MQTT packets: %v %v", err, c.clientID)
			return
		}

		if len(packets) == 0 {
			continue
		}

		c.networkActivity = true

		if allPublish {
			c.pubPktsRcvd += len(packets)
			err := c.handlePublishBulk(packets, data, dataSize, topicName, allSameQoS)
			if err != nil {
				s.logger.Printf("Error doing Publish to subscribers in bulk: %v", err)
				return
			}
			serverMutex.Lock()
			c.pubPktsRcvd += len(packets)
			publishReceived += len(packets)
			serverMutex.Unlock()
			continue
		}

		for _, pkt := range packets {
			header := pkt.hdrStart[0]
			cmdType := header & 0xF0
			c.cmdType = cmdType
			c.remainingLengthSize = pkt.remainingLengthSize
			c.remainingLength = pkt.remainingLength
			c.hdrStart = pkt.hdrStart
			c.remainingBytes = pkt.remainingBytes

			if s.debug {
				s.logger.Printf("Received Message of type: %v %v", getPacketType(cmdType), c.clientID)
			}
			switch cmdType {
			case CONNECT:
				err := c.handleConnect()
				if err != nil {
					s.logger.Printf("Error handling CONNECT: %v", err)
					return
				}
				s.logger.Printf("Client connected: %v", c.clientID)
			case SUBSCRIBE:
				err := c.handleSubscribe()
				if err != nil {
					s.logger.Printf("Error handling SUBSCRIBE: %v", err)
					return
				}
			case UNSUBSCRIBE:
				err := c.handleUnsubscribe()
				if err != nil {
					s.logger.Printf("Error handling UNSUBSCRIBE: %v", err)
					return
				}
			case PUBLISH:
				err := c.handlePublish(pkt)
				if err != nil {
					s.logger.Printf("Error handling PUBLISH: Closing connection: %v", err)
					return
				}
				c.pubPktsRcvd += 1
				serverMutex.Lock()
				publishReceived += 1
				serverMutex.Unlock()
			case PUBACK:
				err = c.handlePuback()
				if err != nil {
					s.logger.Printf("Error handling PUBACK: %v", err)
					return
				}
			case PUBREC:
				err = c.handlePubrec()
				if err != nil {
					s.logger.Printf("Error handling PUBREC: %v", err)
					return
				}
			case PUBREL:
				err = c.handlePubrel()
				if err != nil {
					s.logger.Printf("Error handling PUBREL: %v", err)
					return
				}
			case PUBCOMP:
				err = c.handlePubcomp()
				if err != nil {
					s.logger.Printf("Error handling PUBCOMP: %v", err)
					return
				}
			case PINGREQ:
				err := c.handlePingReq()
				if err != nil {
					s.logger.Printf("Error handling PINGREQ: %v", err)
					return
				}
			case DISCONNECT:
				c.handleDisconnect()
				return
			default:
				s.logger.Printf("Unsupported commanf type: cmd: %v client ID: %v", cmdType>>4, c.clientID)
				//return
			}
		}
	}
}

func (c *MQTTClient) handleConnect() error {
	var userName, password string
	var err error

	s := c.server
	remainingBytes := c.remainingBytes

	offset := 0
	protocolNameLength := int(binary.BigEndian.Uint16(remainingBytes[offset : offset+2]))
	offset += 2
	protocolName := string(remainingBytes[offset : offset+protocolNameLength])
	if protocolName != "MQTT" {
		c.sendConnack(0x84, nil) // Protocol Error
		return fmt.Errorf("unsupported protocol: %v", protocolName)
	}
	offset += protocolNameLength

	protocolLevel := byte(remainingBytes[offset])
	c.protocolLevel = protocolLevel
	if protocolLevel != 4 && protocolLevel != 5 {
		c.sendConnack(0x84, nil) // Protocol Error
		return fmt.Errorf("unsupported protocol version: %v", protocolLevel)
	}
	offset += 1

	connectFlag := byte(remainingBytes[offset])
	willFlag := (connectFlag & 0x04) != 0
	willQoS := (connectFlag & 0x18) >> 3
	willRetain := (connectFlag & 0x20) != 0
	offset += 1

	keepAlive := int16(binary.BigEndian.Uint16(remainingBytes[offset : offset+2]))
	c.keepAlive = keepAlive
	offset += 2

	if c.connType == "tcp" {
		err = c.tcpConn.SetKeepAlive(true)
		if err != nil {
			c.conn.Close()
			return fmt.Errorf("Error enabling KeepAlive: %v", err)
		}
		err = c.tcpConn.SetKeepAlivePeriod(time.Duration(keepAlive) * time.Second)
		if err != nil {
			c.conn.Close()
			return fmt.Errorf("Error setting KeepAlive interval: %v", err)
		}
	}

	if protocolLevel == 5 {
		_, propertiesLength, bytesRead, err := c.parseProperties(remainingBytes, offset)
		if err != nil {
			return fmt.Errorf("failed to read properties: %v", err)
		}
		offset += (propertiesLength + bytesRead)
	}

	clientIDLength := int(binary.BigEndian.Uint16(remainingBytes[offset : offset+2]))
	offset += 2
	clientID := string(remainingBytes[offset : offset+clientIDLength])
	offset += clientIDLength
	c.clientID = clientID

	var willTopic string
	var willMessage []byte
	if willFlag {
		willTopicLength := int(binary.BigEndian.Uint16(remainingBytes[offset : offset+2]))
		offset += 2
		willTopic = string(remainingBytes[offset : offset+willTopicLength])
		offset += willTopicLength

		willMessageLength := int(binary.BigEndian.Uint16(remainingBytes[offset : offset+2]))
		offset += 2
		willMessage = remainingBytes[offset : offset+willMessageLength]
		offset += willMessageLength

		c.willTopic = willTopic
		c.willMessage = willMessage
		c.willQoS = willQoS
		c.willRetain = willRetain

		// Create PUBLISH packet for will message
		// Calculate the remaining length
		remainingLength := 2 + len(willTopic) + len(willMessage) // topic length + topic + message
		if willQoS > 0 {
			remainingLength += 2 // For packet identifier if QoS > 0
		}

		// Create the buffer
		buf := make([]byte, 1+calcVariableByteInteger(remainingLength)+remainingLength)

		// Fixed header
		buf[0] = byte(3<<4 | (willQoS << 1)) // PUBLISH packet type with QoS
		if willRetain {
			buf[0] |= 1 // Set retain flag
		}

		// Remaining length
		bufOffset := 1
		bufOffset += encodeVariableByteIntegerNew(buf[bufOffset:], remainingLength)

		// Variable header
		// Topic name
		binary.BigEndian.PutUint16(buf[bufOffset:], uint16(len(willTopic)))
		bufOffset += 2
		copy(buf[bufOffset:], willTopic)
		bufOffset += len(willTopic)

		// Packet Identifier (only for QoS > 0)
		if willQoS > 0 {
			packetID := uint16(1)
			binary.BigEndian.PutUint16(buf[bufOffset:], packetID)
			bufOffset += 2
		}

		// Payload
		copy(buf[bufOffset:], willMessage)

		// Store in retained messages
		s.mu.Lock()
		s.retainedMessages.Insert(willTopic, buf)
		s.mu.Unlock()
	}

	if connectFlag&0x80 != 0 && connectFlag&0x40 != 0 {
		userNameLength := int(binary.BigEndian.Uint16(remainingBytes[offset : offset+2]))
		offset += 2
		userName = string(remainingBytes[offset : offset+userNameLength])
		offset += userNameLength
		passwordLength := int(binary.BigEndian.Uint16(remainingBytes[offset : offset+2]))
		offset += 2
		password = string(remainingBytes[offset : offset+passwordLength])
		offset += passwordLength
	}

	if c.connType == "tcp" {
		err = c.addConnection(c.clientID, c.conn)
		if err != nil {
			s.logger.Printf("Connect error: %v", c.clientID)
			c.sendConnack(0x01, nil)
			return err
		}
	}

	if s.debug {
		s.logger.Printf("CONNECT packet: Protocol Level: %v, Keep Alive %v, client ID: %v, User Name: %v, Password: %v",
			protocolLevel, keepAlive, clientID, userName, password)
		if willFlag {
			s.logger.Printf("Will Topic: %v, Will QoS: %v, Will Retain: %v", willTopic, willQoS, willRetain)
		}
	}

	switch protocolLevel {
	case 4:
		err = c.sendConnack(0, nil)
		if err != nil {
			return fmt.Errorf("failed to send CONNACK: %v", err)
		}
		if s.debug {
			s.logger.Printf("CONNECT packet (MQTT 3.1.1) handled successfully.")
		}
	case 5:
		connackProperties := []byte{} // Fill with appropriate properties if needed
		err = c.sendConnack(0, connackProperties)
		if err != nil {
			return fmt.Errorf("failed to send CONNACK: %v", err)
		}
		if s.debug {
			s.logger.Printf("CONNECT packet (MQTT 5.0) handled successfully.")
		}
	default:
		fmt.Println(protocolLevel)
		c.sendConnack(0x01, nil)
		s.logger.Printf("Unsupported Protocol Version: %v", protocolLevel)
		return fmt.Errorf("unsupported protocol version: %v", protocolLevel)
	}

	go c.startKeepAliveMonitor()
	return nil
}

func (c *MQTTClient) sendConnack(reasonCode byte, properties []byte) error {
	s := c.server
	// Construct the initial fixed header for CONNACK packet (0x20 for CONNACK)
	//packet := []byte{0x20, 0x00}
	packet := []byte{CONNACK, 0x00}

	// Protocol-specific handling
	switch c.protocolLevel {
	case 4: // MQTT 3.1.1
		variableHeader := []byte{0x00, reasonCode} // Variable header: Acknowledge flags (0x00) and reason code
		packet = append(packet, variableHeader...)
		packet[1] = byte(len(variableHeader)) // Remaining length

	case 5: // MQTT 5.0
		variableHeader := []byte{0x00, reasonCode} // Variable header: Acknowledge flags (0x00) and reason code
		packet = append(packet, variableHeader...)

		// Append properties length and properties
		propertiesLength, propertiesLengthBytes := encodeVariableByteInteger(len(properties))
		packet = append(packet, propertiesLengthBytes...)
		packet = append(packet, properties...)

		// Update the remaining length in the fixed header
		packet[1] = byte(len(variableHeader) + propertiesLength + len(properties))

	default:
		return fmt.Errorf("unsupported protocol version: %v", c.protocolLevel)
	}

	err := c.sendPacket(packet, len(packet))
	if err != nil {
		s.logger.Printf("Error sending packet: %v", err.Error())
		return err
	}

	return nil
}

func (c *MQTTClient) parseProperties(remainingBytes []byte, offset int) (map[byte]interface{}, int, int, error) {
	s := c.server
	// Process properties
	propertiesLength, bytesRead := decodeVariableByteInteger(remainingBytes[offset:])
	properties := remainingBytes[offset+bytesRead : offset+bytesRead+propertiesLength]

	// Parse properties
	parsedProperties := make(map[byte]interface{})
	index := 0
	for index < len(properties) {
		propIdentifier := properties[index]
		index++
		switch propIdentifier {
		case 0x01: // Payload Format Indicator (Value Type: Byte)
			parsedProperties[propIdentifier] = properties[index]
			if value, ok := parsedProperties[propIdentifier].(byte); ok {
				c.config.PayloadFormatIndicator = value
			}
			index++
		case 0x02: // Message Expiry Interval (Value Type: uint32)
			parsedProperties[propIdentifier] = binary.BigEndian.Uint32(properties[index : index+4])
			// Assert the type to uint32 and assign to c.config.MessageExpiryInterval
			if value, ok := parsedProperties[propIdentifier].(uint32); ok {
				c.config.MessageExpiryInterval = value
			}
			index += 4
		case 0x03: // Content Type (Type: String)
			length := binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			parsedProperties[propIdentifier] = string(properties[index : index+int(length)])
			if value, ok := parsedProperties[propIdentifier].(string); ok {
				c.config.ContentType = value
			}
			index += int(length)
		case 0x08: // Response Topic (Type String)
			length := binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			parsedProperties[propIdentifier] = string(properties[index : index+int(length)])
			if value, ok := parsedProperties[propIdentifier].(string); ok {
				c.config.ResponseTopic = value
			}
			index += int(length)
		case 0x09: // Correlation Data (Type []byte)
			length := binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			parsedProperties[propIdentifier] = properties[index : index+int(length)]
			if value, ok := parsedProperties[propIdentifier].([]byte); ok {
				c.config.CorrelationData = value
			}
			index += int(length)
		case 0x0B: // Subscription Identifier (Type uint32)
			parsedProperties[propIdentifier] = binary.BigEndian.Uint32(properties[index : index+4])
			index += 4
			if value, ok := parsedProperties[propIdentifier].(uint32); ok {
				c.config.SubscriptionIdentifier = value
			}
		case 0x11: // Session Expiry Interval (Type uint32)
			parsedProperties[propIdentifier] = binary.BigEndian.Uint32(properties[index : index+4])
			index += 4
			if value, ok := parsedProperties[propIdentifier].(uint32); ok {
				c.config.SessionExpiryInterval = value
			}
		case 0x12: // Assigned Client Identifier (Type String)
			length := binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			parsedProperties[propIdentifier] = string(properties[index : index+int(length)])
			index += int(length)
			if value, ok := parsedProperties[propIdentifier].(string); ok {
				c.config.AssignedClientID = value
			}
		case 0x13: // Server keep Alive (Type uint16)
			parsedProperties[propIdentifier] = binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			if value, ok := parsedProperties[propIdentifier].(uint16); ok {
				c.config.ServerKeepAlive = value
			}
		case 0x15: // Authentication Method (Type String)
			length := binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			parsedProperties[propIdentifier] = string(properties[index : index+int(length)])
			index += int(length)
			if value, ok := parsedProperties[propIdentifier].(string); ok {
				c.config.AuthenticationMethod = value
			}
		case 0x16: // Authentication Data  (Type []byte)
			length := binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			parsedProperties[propIdentifier] = string(properties[index : index+int(length)])
			index += int(length)
			if value, ok := parsedProperties[propIdentifier].([]byte); ok {
				c.config.AuthenticationData = value
			}
		case 0x17: // Request Problem Information (Type Byte)
			parsedProperties[propIdentifier] = properties[index]
			index++
			if value, ok := parsedProperties[propIdentifier].(byte); ok {
				c.config.RequestProblemInfo = value
			}
		case 0x18: // Will Delay Interval (Type uint4)
			parsedProperties[propIdentifier] = binary.BigEndian.Uint32(properties[index : index+4])
			index += 4
			if value, ok := parsedProperties[propIdentifier].(uint32); ok {
				c.config.WillDelayInterval = value
			}
		case 0x19: // Request Response Information (Type Byte)
			parsedProperties[propIdentifier] = properties[index]
			index++
			if value, ok := parsedProperties[propIdentifier].(byte); ok {
				c.config.RequestResponseInfo = value
			}
		case 0x1A: // Response Information (Type String)
			length := binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			parsedProperties[propIdentifier] = string(properties[index : index+int(length)])
			index += int(length)
			if value, ok := parsedProperties[propIdentifier].(string); ok {
				c.config.ResponseInfo = value
			}
		case 0x1C: // Server Reference (Type String)
			length := binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			parsedProperties[propIdentifier] = string(properties[index : index+int(length)])
			index += int(length)
			if value, ok := parsedProperties[propIdentifier].(string); ok {
				c.config.ServerReference = value
			}
		case 0x1F: // Reason String (Type String)
			length := binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			parsedProperties[propIdentifier] = string(properties[index : index+int(length)])
			index += int(length)
			if value, ok := parsedProperties[propIdentifier].(string); ok {
				c.config.ReasonString = value
			}
		case 0x21: // Receive Maximum (Type uint16)
			parsedProperties[propIdentifier] = binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			if value, ok := parsedProperties[propIdentifier].(uint16); ok {
				c.config.ReceiveMaximum = value
			}
		case 0x22: // Topic Alias Maximum (Type uint16)
			parsedProperties[propIdentifier] = binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			if value, ok := parsedProperties[propIdentifier].(uint16); ok {
				c.config.TopicAliasMaximum = value
			}
		case 0x23: // Topic Alias(2 Bytes) (Type uint16)
			parsedProperties[propIdentifier] = binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			if value, ok := parsedProperties[propIdentifier].(uint16); ok {
				c.config.TopicAlias = value
			}
		case 0x24: // Maximum QoS (1 Byte)
			parsedProperties[propIdentifier] = properties[index]
			index++
			if value, ok := parsedProperties[propIdentifier].(byte); ok {
				c.config.MaximumQoS = value
			}
		case 0x25: // Retain Available (1 Byte)
			parsedProperties[propIdentifier] = properties[index]
			index++
			if value, ok := parsedProperties[propIdentifier].(byte); ok {
				c.config.RetainAvailable = value
			}
		case 0x26: // User Property (Type Key/Value)
			if parsedProperties[propIdentifier] == nil {
				parsedProperties[propIdentifier] = make(map[string]string)
			}

			keyLength := binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			key := string(properties[index : index+int(keyLength)])
			index += int(keyLength)
			valueLength := binary.BigEndian.Uint16(properties[index : index+2])
			index += 2
			value := string(properties[index : index+int(valueLength)])
			index += int(valueLength)

			// Type assertion to access the map
			if m, ok := parsedProperties[propIdentifier].(map[string]string); ok {
				m[key] = value
			}

		case 0x27: // Maximum Packet Size (Type uint32)
			parsedProperties[propIdentifier] = binary.BigEndian.Uint32(properties[index : index+4])
			index += 4
			if value, ok := parsedProperties[propIdentifier].(uint32); ok {
				c.config.MaximumPacketSize = value
			}
		case 0x28: // Wildcard Subscription Avaialble (Type byte)
			parsedProperties[propIdentifier] = properties[index]
			index++
			if value, ok := parsedProperties[propIdentifier].(byte); ok {
				c.config.WildcardSubscription = value
			}
		case 0x29: // Subscription Indentifier Avaialble (Type byte)
			parsedProperties[propIdentifier] = properties[index]
			index++
			if value, ok := parsedProperties[propIdentifier].(byte); ok {
				c.config.SubscriptionIDAvailable = value
			}
		case 0x2A: // Shared Subscription Avaialble (Type byte)
			parsedProperties[propIdentifier] = properties[index]
			index++
			if value, ok := parsedProperties[propIdentifier].(byte); ok {
				c.config.SharedSubscription = value
			}
		default:
			s.logger.Printf("Unknown property identifier: 0x%X\n", propIdentifier)
			return nil, 0, 0, fmt.Errorf("unknown property identifier: 0x%X", propIdentifier)
		}
	}
	return parsedProperties, propertiesLength, bytesRead, nil
}

func (c *MQTTClient) handleDisconnect() {
	s := c.server
	s.logger.Printf("Client disconnected: %v", c.clientID)
	c.removeConnection(c.clientID)
}
func (c *MQTTClient) handlePingReq() error {
	var err error
	s := c.server
	// PINGRESP fixed header
	var fixedHeader byte = PINGRESP
	var remainingLength byte = 0x00

	// Create PINGRESP packet
	packet := []byte{
		fixedHeader,     // Fixed header
		remainingLength, // Remaining length
	}

	// Send the PINGRESP packet to the client
	err = c.sendPacket(packet, len(packet))
	if err != nil {
		s.logger.Printf("Error sending packet: %v", err.Error())
		return err
	}

	return nil
}
func (c *MQTTClient) sendRetainedMsgToSubscriber(topicFilterArr []string) error {
	s := c.server
	for _, topicFilter := range topicFilterArr {
		results := s.retainedMessages.Search(topicFilter)
		for _, result := range results {
			packet := result.Message
			err := c.sendPacket(packet, len(packet))
			if err != nil {
				s.logger.Printf("Error sending packet: %v", err.Error())
				return err
			}
		}
	}
	return nil
}

func startPprofServer(address string) {
	go func() {
		fmt.Printf("Starting pprof server on %s\n", address)
		if err := http.ListenAndServe(address, nil); err != nil {
			fmt.Printf("Error starting pprof server: %v\n", err)
		}
	}()
}

func (s *MQTTServer) runServer(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			os.Exit(1)
		}

		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			fmt.Printf("Error: connection is not a TCP connection\n")
			os.Exit(1)
		}

		// Set TCP_NODELAY
		err = tcpConn.SetNoDelay(true)
		if err != nil {
			fmt.Printf("Error setting TCP_NODELAY: %v\n", err)
			os.Exit(1)
		}

		if err := tcpConn.SetReadBuffer(DefaultPacketSize); err != nil {
			fmt.Printf("Failed to set ReadBuffer: %v\n", err)
			os.Exit(1)
		}

		if err := tcpConn.SetWriteBuffer(DefaultPacketSize); err != nil {
			fmt.Printf("Failed to set WriteBuffer: %v\n", err)
			os.Exit(1)
		}

		client := NewMQTTClient(conn, s)
		if s.useSyncPool {
			client.packetBuf = s.packetBufPool.Get().([]byte)[:DefaultPacketSize]
		} else {
			client.packetBuf = make([]byte, DefaultPacketSize)
		}
		client.tcpConn = tcpConn
		client.connType = "tcp"
		go client.handleClientCmds()
	}
}

func (s *MQTTServer) runServerQUIC(listener *quic.Listener) { // Main server
	for {
		// Accept a new connection from a client
		conn, err := listener.Accept(context.Background())
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		// Handle the client connection in a new goroutine
		go s.handleConnectionQUIC(conn)
	}
}

func (s *MQTTServer) handleConnectionQUIC(conn quic.Connection) { // Per client handler
	for {
		stream, err := conn.AcceptStream(context.Background())
		activityCount := 0
		if err != nil {
			// Check if the error indicates that the connection is closed
			if strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "closed") {
				log.Println("Client exited, closing connection")
				return // Exit to prevent additional error logging
			}

			// If no recent activity, close the connection and exit
			if strings.Contains(err.Error(), "no recent network activity") {
				activityCount++
				if activityCount > 5 {
					log.Println("No recent network activity, closing connection due to inactivity")
					return
				}
				return // Exit to prevent additional error logging
			}

			// Handle other types of errors
			log.Printf("Error accepting stream: %v", err)
			return
		}

		// Process the stream in a separate goroutine
		client := NewMQTTClientQUIC(stream, s)
		if s.useSyncPool {
			client.packetBuf = s.packetBufPool.Get().([]byte)[:DefaultPacketSize]
		} else {
			client.packetBuf = make([]byte, DefaultPacketSize)
		}
		client.connType = "quic"

		go client.handleClientCmds()
	}
}

func (s *MQTTServer) startSSLListener(address string) {
	cert, err := tls.LoadX509KeyPair("server.crt", "server.key")
	if err != nil {
		fmt.Printf("Failed to load SSL certificate: %v\n", err)
		os.Exit(1)
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}}
	ln, err := tls.Listen("tcp", address, config)
	if err != nil {
		fmt.Printf("Failed to start SSL listener: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Listener ssl:default on %s started\n", address)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("SSL accept error: %v\n", err)
			os.Exit(1)
		}
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			fmt.Printf("Error: connection is not a TCP connection\n")
			os.Exit(1)
		}

		// Set TCP_NODELAY
		err = tcpConn.SetNoDelay(true)
		if err != nil {
			fmt.Printf("Error setting TCP_NODELAY: %v\n", err)
			os.Exit(1)
		}
		client := NewMQTTClient(conn, s)
		if s.useSyncPool {
			client.packetBuf = s.packetBufPool.Get().([]byte)[:DefaultPacketSize]
		} else {
			client.packetBuf = make([]byte, DefaultPacketSize)
		}
		client.tcpConn = tcpConn
		client.connType = "tcp"
		go client.handleClientCmds()
	}
}

// Start WebSocket listener
func (s *MQTTServer) startWSListener(address string) {
	http.HandleFunc("/mqtt-ws", func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for now
			},
		}

		// Upgrade HTTP to WebSocket
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			fmt.Printf("WebSocket upgrade error: %v\n", err)
			return
		}

		// Start handling the WebSocket connection
		go handleWSClientCmds(conn, s)
	})

	fmt.Printf("Listener ws:default on %s started\n", address)
	log.Fatal(http.ListenAndServe(address, nil))
}

// Setup a bare-bones TLS config for the server
func generateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"quic-echo"},
	}
}

func getPacketType(packetType byte) string {
	switch packetType {
	case CONNECT:
		return "CONNECT"
	case CONNACK:
		return "CONNACK"
	case PUBLISH:
		return "PUBLISH"
	case PUBACK:
		return "PUBACK"
	case PUBREC:
		return "PUBREC"
	case PUBREL:
		return "PUBREL"
	case PUBCOMP:
		return "PUBCOMP"
	case SUBSCRIBE:
		return "SUBSCRIBE"
	case SUBACK:
		return "SUBACK"
	case UNSUBSCRIBE:
		return "UNSUBSCRIBE"
	case UNSUBACK:
		return "UNSUBACK"
	case PINGREQ:
		return "PINGREQ"
	case PINGRESP:
		return "PINGRESP"
	case DISCONNECT:
		return "DISCONNECT"
	case AUTH:
		return "AUTH"
	default:
		return "UNKNOWN"
	}
}

func (c *MQTTClient) addConnection(clientID string, conn net.Conn) error {
	s := c.server
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.connections[clientID]; exists {
		delete(s.connections, clientID)
	}

	s.connections[clientID] = conn
	return nil
}

func (c *MQTTClient) removeConnection(clientID string) {
	s := c.server
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.connections, clientID)
}

func (c *MQTTClient) addSubscriber(topicFilter string, qos int) {
	s := c.server
	s.mu.Lock()
	s.subscribers[topicFilter] = Subscriber{topicFilter: topicFilter, QoS: qos}
	s.mu.Unlock()
}

func (c *MQTTClient) getSubscriberQoS(topic string) int {
	s := c.server
	s.mu.Lock()
	defer s.mu.Unlock()
	for filter, subscriber := range s.subscribers {
		if matchTopicFilter(topic, filter) {
			return subscriber.QoS
		}
	}
	return 0 // Default QoS if no matching subscriber found
}

func (c *MQTTClient) queuePacket(pkt Packet) {
	c.mu.Lock()
	c.pendingMessages[pkt.packetIdentifier] = pkt
	c.mu.Unlock()
}

func (c *MQTTClient) acknowledgePacket(pktID uint16) {
	c.mu.Lock()
	delete(c.pendingMessages, pktID)
	c.mu.Unlock()
}

func calcVariableByteInteger(n int) int {
	if n == 0 {
		return 1
	}
	count := 0
	for n > 0 {
		count++
		n = n / 128
	}
	return count
}

func encodeVariableByteIntegerNew(buf []byte, n int) int {
	bytesWritten := 0
	for {
		encodedByte := byte(n % 128)
		n = n / 128
		if n > 0 {
			encodedByte |= 128
		}
		buf[bytesWritten] = encodedByte
		bytesWritten++
		if n == 0 {
			break
		}
	}
	return bytesWritten
}

// Helper function to decode a variable byte integer (as per MQTT 5.0 spec)
func decodeVariableByteInteger(buf []byte) (value int, bytesRead int) {
	multiplier := 1
	value = 0
	for {
		encodedByte := buf[bytesRead]
		bytesRead++
		value += int(encodedByte&127) * multiplier
		if encodedByte&128 == 0 {
			break
		}
		multiplier *= 128
		if multiplier > 128*128*128 {
			// Error: malformed variable byte integer
			return 0, bytesRead
		}
	}
	return value, bytesRead
}

// Utility function to encode variable byte integer (MQTT 5)
func encodeVariableByteInteger(value int) (int, []byte) {
	encodedBytes := []byte{}
	for {
		encodedByte := value % 128
		value /= 128
		if value > 0 {
			encodedByte |= 128
		}
		encodedBytes = append(encodedBytes, byte(encodedByte))
		if value == 0 {
			break
		}
	}
	return len(encodedBytes), encodedBytes
}

func (s *MQTTServer) initLogger(logfileName string) {
	logFile, err := os.OpenFile(logfileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("Failed to open log file:", err)
	}
	s.logger = log.New(logFile, "", log.Ldate|log.Ltime)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Insert inserts a client into the Trie under the specified topic filter
func (t *Trie) insert(topicFilter string, client *MQTTClient) {
	tokens := splitTopic(topicFilter)
	node := t.root

	for _, token := range tokens {
		if node.children == nil {
			node.children = make(map[string]*TrieNode)
		}

		childNode, exists := node.children[token]
		if !exists {
			childNode = &TrieNode{
				children: make(map[string]*TrieNode),
				clients:  make(map[*MQTTClient]bool),
			}
			node.children[token] = childNode
		}

		node = childNode
	}

	node.clients[client] = true
}

// Remove removes a client from the Trie under the specified topic filter
func (t *Trie) remove(topicFilter string, client *MQTTClient) {
	s := client.server
	tokens := splitTopic(topicFilter)
	nodes := []*TrieNode{t.root}

	for _, token := range tokens {
		var nextNodes []*TrieNode
		for _, node := range nodes {
			if node.children != nil {
				if childNode, exists := node.children[token]; exists {
					nextNodes = append(nextNodes, childNode)
				}
			}
		}
		nodes = nextNodes
	}

	for _, node := range nodes {
		delete(node.clients, client)
	}
	value, exists := s.matchingClients.Load(topicFilter)
	if exists {
		clients := value.(map[*MQTTClient]bool)
		delete(clients, client)

		// If no clients are left, delete the entire entry from the sync.Map
		if len(clients) == 0 {
			s.matchingClients.Delete(topicFilter)
		} else {
			s.matchingClients.Store(topicFilter, clients)
		}
	}
}

// FindMatchingClients finds all clients subscribed to a topic
func (t *Trie) findMatchingClients(topic string) map[*MQTTClient]bool {
	matchingClients := make(map[*MQTTClient]bool)
	var search func(node *TrieNode, parts []string)
	search = func(node *TrieNode, parts []string) {
		if len(parts) == 0 {
			for client := range node.clients {
				matchingClients[client] = true
			}
			return
		}
		part := parts[0]
		rest := parts[1:]

		if part == "#" {
			t.collectAllClients(node, matchingClients)
			return
		}

		if nextNode, ok := node.children[part]; ok {
			search(nextNode, rest)
		}
		if nextNode, ok := node.children["+"]; ok {
			search(nextNode, rest)
		}
		if part != "#" {
			if nextNode, ok := node.children["#"]; ok {
				t.collectAllClients(nextNode, matchingClients)
			}
		}
	}
	search(t.root, splitTopic(topic))
	return matchingClients
}

// collectAllClients collects all clients in the subtree rooted at node
func (t *Trie) collectAllClients(node *TrieNode, matchingClients map[*MQTTClient]bool) {
	for client := range node.clients {
		matchingClients[client] = true
	}
	for _, child := range node.children {
		t.collectAllClients(child, matchingClients)
	}
}

// RemoveAll removes a client from all topic filters in the Trie
func (t *Trie) RemoveAll(client *MQTTClient) {
	s := client.server
	s.mu.Lock()
	defer s.mu.Unlock()
	t.removeAllRecursive(t.root, client)
}

func (t *Trie) removeAllRecursive(node *TrieNode, client *MQTTClient) {
	for _, childNode := range node.children {
		t.removeAllRecursive(childNode, client)
	}
	delete(node.clients, client)
}

// Helper function to split a topic into its parts
func splitTopic(topic string) []string {
	return strings.Split(topic, "/")
}

func (t *TrieMsgs) Insert(topic string, message []byte) {
	node := t.root
	parts := strings.Split(topic, "/")
	for _, part := range parts {
		if _, exists := node.children[part]; !exists {
			node.children[part] = &TrieNodeMsgs{
				children: make(map[string]*TrieNodeMsgs),
			}
		}
		node = node.children[part]
	}
	node.message = make([]byte, len(message))
	copy(node.message, message)
	node.isLeaf = true
}

func (t *TrieMsgs) Search(filter string) []struct {
	Topic   string
	Message []byte
} {
	var results []struct {
		Topic   string
		Message []byte
	}
	t.searchRecursive(t.root, strings.Split(filter, "/"), 0, "", &results)
	return results
}

func (t *TrieMsgs) searchRecursive(node *TrieNodeMsgs, parts []string, index int, currentTopic string, results *[]struct {
	Topic   string
	Message []byte
}) {
	if index == len(parts) {
		if node.isLeaf {
			*results = append(*results, struct {
				Topic   string
				Message []byte
			}{Topic: currentTopic, Message: node.message})
		}
		return
	}

	part := parts[index]

	if part == "#" {
		t.collectAllMessages(node, currentTopic, results)
		return
	}

	if part == "+" || part == "#" {
		for key, child := range node.children {
			newTopic := currentTopic
			if newTopic != "" {
				newTopic += "/"
			}
			newTopic += key
			t.searchRecursive(child, parts, index+1, newTopic, results)
		}
	}

	if child, exists := node.children[part]; exists {
		newTopic := currentTopic
		if newTopic != "" {
			newTopic += "/"
		}
		newTopic += part
		t.searchRecursive(child, parts, index+1, newTopic, results)
	}
}

func (t *TrieMsgs) collectAllMessages(node *TrieNodeMsgs, currentTopic string, results *[]struct {
	Topic   string
	Message []byte
}) {
	if node.isLeaf {
		*results = append(*results, struct {
			Topic   string
			Message []byte
		}{Topic: currentTopic, Message: node.message})
	}
	for key, child := range node.children {
		newTopic := currentTopic
		if newTopic != "" {
			newTopic += "/"
		}
		newTopic += key
		t.collectAllMessages(child, newTopic, results)
	}
}

func matchTopicFilter(topic, filter string) bool {
	topicParts := strings.Split(topic, "/")
	filterParts := strings.Split(filter, "/")

	if len(filterParts) > len(topicParts) {
		return false
	}

	for i, filterPart := range filterParts {
		if filterPart == "#" {
			return true
		}
		if filterPart != "+" && filterPart != topicParts[i] {
			return false
		}
	}

	return len(topicParts) == len(filterParts)
}

func initServerStats() {
	serverStartTime = time.Now()
	go printServerStats()
}

func printServerStats() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastPublishReceived, lastSubscribeSent int

	for range ticker.C {
		serverMutex.Lock()
		currentPublishReceived := publishReceived
		currentSubscribeSent := subscribeSent
		serverMutex.Unlock()

		publishPerSecond := int64(float64(currentPublishReceived-lastPublishReceived) / 1.0)
		subscribePerSecond := int64(float64(currentSubscribeSent-lastSubscribeSent) / 1.0)

		fmt.Printf("\rServer - Received: %d (%.2f/s), Sent: %d (%.2f/s)",
			currentPublishReceived, float64(publishPerSecond),
			currentSubscribeSent, float64(subscribePerSecond))

		lastPublishReceived = currentPublishReceived
		lastSubscribeSent = currentSubscribeSent
	}
}

func (c *MQTTClient) startKeepAliveMonitor() {
	ticker := time.NewTicker(time.Duration(time.Duration(c.keepAlive) * time.Second))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			if c.networkActivity {
				c.networkActivity = false
				c.mu.Unlock()
			} else {
				// No activity, extend timer to 1.5 times
				ticker.Reset(time.Duration(float64(c.keepAlive) * 1.5))
				c.mu.Unlock()

				// Check again after the extended period
				<-ticker.C
				c.mu.Lock()
				if !c.networkActivity {
					// Still no activity, close connection
					c.mu.Unlock()
					c.keepAliveExceeded = true
					return
				}
				c.networkActivity = false
				ticker.Reset(time.Duration(c.keepAlive))
				c.mu.Unlock()
			}
		}
	}
}
