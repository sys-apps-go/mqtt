package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/gorilla/websocket"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"
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
	DefaultPacketSize = 1024 * 1204
)

type TrieNode struct {
	children map[string]*TrieNode
	clients  map[*MQTTClient]bool // Using a map to store clients for simplicity, could be a list or another data structure
}

// Trie represents the Trie data structure for MQTT topic filters
type Trie struct {
	root *TrieNode
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
	conn              net.Conn
	connWs            *websocketConn
	connType          string
	cmdType           byte
	packetBufPool     sync.Pool
	publishBuf        []byte
	packetBuf         []byte
	reader            *bufio.Reader
	wsBuffer          []byte
	tcpConn           *net.TCPConn
	KeepAlive         uint16 // Keep alive interval in seconds
	CleanStart        bool   // Clean start flag
	Username          string // Username
	Password          string // Password
	protocolLevel     byte
	config            Properties
	server            *MQTTServer
	clientID          string
	publishCount      int64
	totalTimeMicroSec int64
	memStats          runtime.MemStats
	mu                sync.Mutex
	pendingAckMu      sync.Mutex
	pendingAck        map[uint16][]byte
	qosLevel          byte
	wg                sync.WaitGroup
}

// MQTTServer represents an MQTT server
type MQTTServer struct {
	Broker          string // Broker address (e.g., "localhost:1883")
	subscriptions   *Trie
	matchingClients sync.Map // Using sync.Map for concurrent access
	verbose         bool
	debug           bool
	mu              sync.Mutex
}

// NewMQTTServer creates a new MQTTServer instance
func NewMQTTServer() *MQTTServer {
	t := NewTrie()

	return &MQTTServer{
		subscriptions: t,
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
		conn:       conn,
		reader:     bufio.NewReader(conn),
		server:     server,
		pendingAck: make(map[uint16][]byte),
		packetBufPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, DefaultPacketSize)
			},
		},
		packetBuf: make([]byte, 32),
	}
}

// NewMQTTClient creates a new MQTTClient instance websocket connection
func NewMQTTClientWs(conn *websocketConn, server *MQTTServer) *MQTTClient {
	return &MQTTClient{
		connWs:     conn,
		server:     server,
		wsBuffer:   make([]byte, 128*1024),
		pendingAck: make(map[uint16][]byte),
		packetBuf: make([]byte, 32),
	}
}

func main() {
	brokerAddress := flag.String("b", ":1883", "Broker address")
	verbose := flag.Bool("v", false, "Verbose mode")
	debug := flag.Bool("d", false, "Debug trace mode")
	flag.Parse()
	server := NewMQTTServer()
	server.verbose = *verbose
	server.debug = *debug

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

	// Start SSL listener
	//go startSSLListener(":1884")

	// Start WebSocket listener
	go server.startWSListener(":1885")

	// Start WebSocket Secure listener
	//go startWSSListener(":1886")

	// Start HTTP Dashboard listener
	go startHTTPListener(":1887")

	select {}
}

func (c *MQTTClient) readControlByte() (byte, error) {
	switch c.connType {
	case "tcp":
		header, err := c.reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return header, nil
	case "websocket":
		packet, err := c.connWs.Read()
		if err != nil || len(packet) < 2 {
			if len(packet) < 2 {
				err = fmt.Errorf("Invalid packet size: %v\n", len(packet))
			}
			return 0, err
		}
		copy(c.wsBuffer, packet)
		header := packet[0]
		return header, nil
	default:
	}
	return 0, fmt.Errorf("Invalid connection type: %v\n", c.connType)
}

// HandleConnection handles the MQTT client connection
func (c *MQTTClient) HandleConnection() {
	s := c.server

	if c.connType == "tcp" {
		defer c.conn.Close()
	}
	defer s.subscriptions.RemoveAll(c)

	if s.verbose {
		fmt.Println("Client connected:", c.conn.RemoteAddr())
	}

	for {
		header, err := c.readControlByte()
		if err != nil {
			if s.debug {
				if err == io.EOF {
					fmt.Println("Client exited:", err.Error())
				} else {
					fmt.Println("Error reading header:", err.Error())
				}
			}
			return
		}
		msgType := header & 0xF0
		c.cmdType = msgType
		if s.debug {
			fmt.Printf("Received Message of type: %v %v\n", msgType>>4, c.clientID)
		}
		switch msgType {
		case CONNECT:
			err := c.handleConnect()
			if err != nil {
				fmt.Println("Error handling CONNECT:", err)
				return
			}
		case SUBSCRIBE:
			err := c.handleSubscribe(c.reader)
			if err != nil {
				fmt.Println("Error handling SUBSCRIBE:", err)
				return
			}
		case UNSUBSCRIBE:
			err := c.handleUnsubscribe(c.reader)
			if err != nil {
				fmt.Println("Error handling UNSUBSCRIBE:", err)
				return
			}
		case PUBLISH:
			qosLevel := int(header&0x06) >> 1
			err := c.handlePublish(qosLevel, header)
			if err != nil {
				fmt.Println("Error handling PUBLISH: Closing connection", err)
				return
			}
		case PUBACK:
			err := c.handlePuback()
			if err != nil {
				fmt.Println("Error handling PUBACK:", err)
				return
			}
		case PUBREC:
			err := c.handlePubrec()
			if err != nil {
				fmt.Println("Error handling PUBREC:", err)
				return
			}
		case PUBREL:
			err := c.handlePubrel(false)
			if err != nil {
				fmt.Println("Error handling PUBREL:", err)
				return
			}
		case PUBCOMP:
			err := c.handlePubcomp()
			if err != nil {
				fmt.Println("Error handling PUBCOMP:", err)
				return
			}
		case PINGREQ:
			err := c.handlePingReq()
			if err != nil {
				fmt.Println("Error handling PINGREQ:", err)
				return
			}
		case DISCONNECT:
			c.handleDisconnect()
			return
		default:
			fmt.Println("Unsupported message type:", msgType)
			//return
		}
	}
}

func (c *MQTTClient) handleConnect() error {
	var userName, password string
	s := c.server

	remainingBytes, _, err := c.readRemainingBytes()
	if err != nil {
		return err
	}

	// Process the variable header fields for CONNECT packet
	offset := 0
	protocolNameLength := int(binary.BigEndian.Uint16(remainingBytes[offset : offset+2]))
	offset += 2
	protocolName := string(remainingBytes[offset : offset+protocolNameLength])
	if protocolName != "MQTT" {
		c.sendConnack(0x84, nil) // Protocol Error
		return fmt.Errorf("unsupported protocol: %v", protocolName)
	}

	offset += protocolNameLength

	//Protocol Level
	protocolLevel := byte(remainingBytes[offset])
	c.protocolLevel = protocolLevel
	if protocolLevel != 4 && protocolLevel != 5 {
		c.sendConnack(0x84, nil) // Protocol Error
		return fmt.Errorf("unsupported protocol version: %v", protocolLevel)
	}

	offset += 1
	//Connect flag
	connectFlag := byte(remainingBytes[offset])

	offset += 1
	// Keep Alive
	keepAlive := int(binary.BigEndian.Uint16(remainingBytes[offset : offset+2]))

	if c.connType == "tcp" {
		// Set TCP NODELAY true to reduce latency
		err = c.tcpConn.SetKeepAlive(true)
		if err != nil {
			c.conn.Close()
			return fmt.Errorf("Error enabling KeepAlive: %v", err)
		}

		// Set KeepAlive interval
		err = c.tcpConn.SetKeepAlivePeriod(time.Duration(keepAlive) * time.Second)
		if err != nil {
			c.conn.Close()
			return fmt.Errorf("Error setting KeepAlive interval: %v", err)
		}
	}

	offset += 2
	if protocolLevel == 5 {
		// Process properties
		_, propertiesLength, bytesRead, err := c.parseProperties(remainingBytes, offset)
		if err != nil {
			return fmt.Errorf("failed to read properties: %v", err)
		}
		offset += (propertiesLength + bytesRead)
	}

	//Process Payload
	clientIDLength := int(binary.BigEndian.Uint16(remainingBytes[offset : offset+2]))
	offset += 2
	clientID := string(remainingBytes[offset : offset+clientIDLength])
	offset += clientIDLength
	c.clientID = clientID
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
	if s.debug {
		fmt.Printf("CONNECT packet: Keep Alive %v, client ID: %v, User Name: %v, Password: %v\n",
			keepAlive, clientID, userName, password)
	}

	switch protocolLevel {
	case 4:
		// Handle CONNECT packet with parsed information
		// Example: Validate client credentials, establish session, etc.
		err = c.sendConnack(0, nil)
		if err != nil {
			return fmt.Errorf("failed to send CONNACK: %v", err)
		}
		if s.debug {
			fmt.Println("CONNECT packet (MQTT 3.1.1) handled successfully.")
		}
	case 5:

		// Handle CONNECT packet with parsed information
		// Example: Validate client credentials, establish session, etc.
		connackProperties := []byte{} // Fill with appropriate properties if needed
		err = c.sendConnack(0, connackProperties)
		if err != nil {
			return fmt.Errorf("failed to send CONNACK: %v", err)
		}
		if s.debug {
			fmt.Println("CONNECT packet (MQTT 5.0) handled successfully.")
		}

	default:
		c.sendConnack(0x01, nil)
		fmt.Printf("Unsupported Protocol Version: %v\n", protocolLevel)
		return fmt.Errorf("unsupported protocol version: %v", protocolLevel)
	}

	return nil
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

func (c *MQTTClient) sendConnack(reasonCode byte, properties []byte) error {
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
		fmt.Println("Error sending packet:", err.Error())
		return err
	}

	return nil
}

func (c *MQTTClient) sendPacket(packet []byte, pktSize int) error {
	// Send the packet
	switch c.connType {
	case "tcp":
		_, err := c.conn.Write(packet[0:pktSize])
		if err != nil {
			fmt.Printf("Error sending packet: %v, error: %v\n", packet[0]>>4, err.Error())
			return err
		}
	case "websocket":
		_, err := c.connWs.Write(packet[0:pktSize])
		if err != nil {
			fmt.Printf("Error sending packet using Websocket: %v, error: %v\n", packet[0]>>4, err.Error())
			return err
		}
	}
	return nil
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

func (c *MQTTClient) readRemainingBytes() ([]byte, int, error) {
	var remainingBytes []byte
	var remainingLength int
	var err error

	switch c.connType {
	case "tcp":
		// Read remaining length
		remainingLength, err = c.readRemainingLength(c.reader)
		if err != nil {
			fmt.Println("Error reading remaining length:", err.Error())
			return nil, 0, err
		}
		if remainingLength == 0 {
			return nil, 0, fmt.Errorf("remaining length is zero")
		}
		if c.cmdType == PUBLISH {
			if (remainingLength + 2) <= DefaultPacketSize {
				remainingBytes = c.publishBuf[0 : remainingLength+2]
			} else {
				remainingBytes = make([]byte, remainingLength+2)
			}
			_, err = io.ReadFull(c.reader, remainingBytes[2:remainingLength+2])
		} else {
			remainingBytes = make([]byte, remainingLength)
			_, err = io.ReadFull(c.reader, remainingBytes)
		}
		if err != nil {
			fmt.Println("Error reading remaining length:", err)
			return nil, 0, err
		}
	case "websocket":
		remainingLength = int(c.wsBuffer[1])
		if c.cmdType == PUBLISH {
			remainingBytes = c.wsBuffer[0:]
		} else {
			remainingBytes = c.wsBuffer[2:]
		}
	default:
	}
	return remainingBytes, remainingLength, nil
}

func (c *MQTTClient) handleSubscribe(reader *bufio.Reader) error {
	var err error
	s := c.server

	remainingBytes, remainingLength, err := c.readRemainingBytes()
	if err != nil {
		return err
	}

	// Read variable header (packet identifier)
	packetIdentifier := binary.BigEndian.Uint16(remainingBytes[0:2])
	offset := 2

	// MQTT 5.0: Read properties if present
	if c.protocolLevel == 5 {
		properties, propertiesLength, bytesRead, err := c.parseProperties(remainingBytes, offset)
		if err != nil {
			fmt.Println("Error reading properties:", err)
			return err
		}
		if err != nil {
			fmt.Println("Error reading properties:", err)
			return err
		}
		if s.debug {
			printStructFields(properties)
		}
		offset += bytesRead
		offset += propertiesLength
	}

	// Read payload (topic filters and QoS levels)
	payload := remainingBytes[offset:remainingLength]
	var index int
	qosLevels := make([]byte, 0)

	var qosLevel byte
	qosArray := make([]byte, 0)
	topicFilterCount := 0
	for index < len(payload) {
		// Extract topic filter length
		topicFilterLength := int(payload[index])<<8 + int(payload[index+1])
		index += 2

		// Validate topic filter length
		if topicFilterLength <= 0 || index+topicFilterLength > len(payload) {
			return fmt.Errorf("invalid topic filter length")
		}

		// Extract topic filter
		topicFilter := string(payload[index : index+topicFilterLength])
		index += topicFilterLength

		// Extract QoS level
		if index >= len(payload) {
			return fmt.Errorf("missing QoS level")
		}
		qosLevel = payload[index]
		qosArray = append(qosArray, qosLevel)
		index++

		if s.debug {
			fmt.Printf("Received SUBSCRIBE for topic filter %v with QoS %v\n", topicFilter, qosLevel)
		}
		qosLevels = append(qosLevels, qosLevel) // Collect all QoS levels
		// Store the subscription
		c.StoreSubscription(topicFilter)
		topicFilterCount += 1
	}
	c.qosLevel = qosLevel
	// Send SUBACK packet acknowledging the subscription
	c.sendSuback(packetIdentifier, topicFilterCount, qosArray)
	return nil
}

/* SUBACK: Version 4(3.1.1): Remaining Size: 2 byte Packet Identifier +  Return code corresponds to each Topic Filter
 * (topicFilterCount * 1) + 2
 * 0x00 - Success - Maximum QoS 0
 * 0x01 - Success - Maximum QoS 1
 * 0x02 - Success - Maximum QoS 2
 * 0x80 - Failure
 * SUBACK: Version 5: Remaining Size: 2 byte Packet Identifier +  Property size + Return code corresponds to each Topic Filter
 */
func (c *MQTTClient) sendSuback(packetIdentifier uint16, topicFilterCount int, qosArray []byte) error {
	var err error

	s := c.server

	// SUBACK fixed header
	var fixedHeader byte = SUBACK
	var remainingLength = 2 + (1 * topicFilterCount)

	if c.protocolLevel == 5 {
		remainingLength += 1
	}

	packetIdentifierBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(packetIdentifierBytes, packetIdentifier)

	// Prepare SUBACK packet
	packet := []byte{fixedHeader}
	packet = append(packet, byte(remainingLength))
	packet = append(packet, packetIdentifierBytes...)
	if c.protocolLevel == 5 {
		packet = append(packet, []byte{0}...)
	}
	for i := 0; i < topicFilterCount; i++ {
		packet = append(packet, []byte{qosArray[i]}...)
	}

	err = c.sendPacket(packet, len(packet))
	if err != nil {
		fmt.Println("Error sending packet:", err.Error())
		return err
	}

	if err != nil {
		fmt.Println("Error sending SUBACK packet:", err)
		return err
	}
	if s.debug {
		fmt.Printf("Sent SUBACK packet %v\n", packet)
	}
	return nil
}

func (c *MQTTClient) handlePublish(qosLevel int, header byte) error {
	var err error
	s := c.server

	remainingBytesWithHeader, remainingLength, err := c.readRemainingBytes()
	if err != nil {
		return err
	}

	remainingBytes := remainingBytesWithHeader[2 : remainingLength+2]

	topicNameLength := binary.BigEndian.Uint16(remainingBytes[0:2])
	// Read topic name
	topicNameBytes := remainingBytes[2 : 2+topicNameLength]
	topicName := string(topicNameBytes)

	// Read packet identifier if QoS > 0
	var packetIdentifier uint16
	offset := int(2 + topicNameLength)
	if qosLevel > 0 {
		packetIdentifier = binary.BigEndian.Uint16(remainingBytes[offset : offset+2])
		offset += 2
	}

	// Process MQTT 5.0 specific properties
	if c.protocolLevel == 5 {
		properties, propertiesLength, bytesRead, err := c.parseProperties(remainingBytes, offset)
		if err != nil {
			return fmt.Errorf("failed to read properties: %v", err)
		}
		if s.debug {
			printStructFields(properties)
		}
		offset += bytesRead
		offset += propertiesLength
	}

	packet := remainingBytesWithHeader
	packet[0] = header
	packet[1] = byte(remainingLength)
	c.wg.Add(1)
	go func(topic string, pkt []byte, qos byte) {
		defer c.wg.Done()
		// Forward the payload to all subscribers of the topic
		c.PublishToSubscribers(topic, pkt, qos, packetIdentifier, remainingLength+2)
		if s.debug {
			fmt.Printf("Received PUBLISH command with QoS %v\n", qos)
		}
	}(topicName, packet, byte(qosLevel))

	// Send response based on QoS level
	switch qosLevel {
	case 1:
		// Send PUBACK
		err := c.sendPuback(packetIdentifier)
		if err != nil {
			fmt.Println("Error sending PUBACK:", err)
			return err
		}
	case 2:
		// Send PUBREC
		err := c.sendPubrec(packetIdentifier)
		if err != nil {
			fmt.Println("Error sending PUBREC:", err)
			return err
		}
	}

	c.wg.Wait()

	return nil
}

func (c *MQTTClient) handlePingReq() error {
	var err error
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
		fmt.Println("Error sending packet:", err.Error())
		return err
	}

	return nil
}

func (c *MQTTClient) handleDisconnect() {
	// Handle DISCONNECT logic if needed
}

// StoreSubscription stores a subscription for a topic filter
func (c *MQTTClient) StoreSubscription(topicFilter string) {
	s := c.server
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions.Insert(topicFilter, c)
	if s.debug {
		fmt.Printf("Stored subscription for topic filter %v \n", topicFilter)
	}
}

// RemoveSubscription removes a subscription for a topic filter
func (c *MQTTClient) RemoveSubscription(topicFilter string) {
	s := c.server
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions.Remove(topicFilter, c)
}

// RemoveSubscription removes subscription for client in all topic filters
func (c *MQTTClient) RemoveSubscriptionClientAllFilters() {
	s := c.server
	s.subscriptions.RemoveAll(c)
}

// PublishToSubscribers publishes a message to all subscribers of a topic
func (c *MQTTClient) PublishToSubscribers(topicFilter string, packet []byte, qos byte, packetIdentifier uint16, pktSize int) {
	var clients map[*MQTTClient]bool
	s := c.server
	// Find all clients subscribed to the topic using the Trie
	s.mu.Lock()
	clients = s.subscriptions.FindMatchingClients(topicFilter)
	s.mu.Unlock()

	for client, _ := range clients {
		qosLevel := qos
		if qosLevel > client.qosLevel {
			qosLevel = client.qosLevel
		}
		packet[0] &= 0xF9
		packet[0] |= (qosLevel << 1) //Subscriber QoS level upto maximum
		client.savePublish(packetIdentifier, packet)
		client.sendPublish(packet, pktSize)
	}
}

// topicMatchesFilter checks if a topic matches a topic filter
func topicMatchesFilter(topic, filter string) bool {
	// Simplified matching for now; wildcard support can be added later
	return topic == filter
}

// sendPublish sends a PUBLISH packet to the client
func (c *MQTTClient) sendPublish(packet []byte, pktSize int) error {
	var err error
	// Send the PUBLISH packet to the client
	c.mu.Lock()
	defer c.mu.Unlock()
	err = c.sendPacket(packet, pktSize)
	if err != nil {
		return err
	}
	return nil
}

// encodeRemainingLength encodes the remaining length using the MQTT encoding scheme
func encodeRemainingLength(length int) []byte {
	var encodedBytes []byte
	for length > 0 {
		encodedByte := length % 128
		length /= 128
		if length > 0 {
			encodedByte |= 128
		}
		encodedBytes = append(encodedBytes, byte(encodedByte))
	}
	return encodedBytes
}

func (c *MQTTClient) handleType(header byte) error {
	s := c.server
	msgType := header & 0xF0
	switch msgType {
	case SUBSCRIBE:
		err := c.handleSubscribe(c.reader)
		if err != nil {
			fmt.Println("Error handling SUBSCRIBE:", err)
			return err
		}
	case UNSUBSCRIBE:
		err := c.handleUnsubscribe(c.reader)
		if err != nil {
			fmt.Println("Error handling UNSUBSCRIBE:", err)
			return err
		}
	case PUBLISH:
		var start time.Time
		if s.debug {
			start = time.Now()
		}
		qosLevel := int(header&0x06) >> 1
		err := c.handlePublish(qosLevel, header)
		if err != nil {
			fmt.Println("Error handling PUBLISH: Closing connection", err)
			return err
		}
		if s.debug {
			elapsed := time.Since(start)
			c.publishCount += 1
			c.totalTimeMicroSec += elapsed.Microseconds()
		}
	case PUBACK:
		err := c.handlePuback()
		if err != nil {
			fmt.Println("Error handling PUBACK:", err)
			return err
		}
	case PUBREC:
		err := c.handlePubrec()
		if err != nil {
			fmt.Println("Error handling PUBREC:", err)
			return err
		}
	case PUBREL:
		err := c.handlePubrel(false)
		if err != nil {
			fmt.Println("Error handling PUBREL:", err)
			return err
		}
	case PUBCOMP:
		err := c.handlePubcomp()
		if err != nil {
			fmt.Println("Error handling PUBCOMP:", err)
			return err
		}
	case PINGREQ:
		err := c.handlePingReq()
		if err != nil {
			fmt.Println("Error handling PINGREQ:", err)
			return err
		}
	case DISCONNECT:
		c.handleDisconnect()
		return nil
	default:
		err := fmt.Errorf("Unsupported message type:", msgType)
		return err
	}
	return nil

}

func (c *MQTTClient) handlePubrel(readHeader bool) error {
	var err error

	s := c.server
	if readHeader {
		header, err := c.readControlByte()
		if err != nil {
			return err
		}
		if header&0xF0 != PUBREL {
			return c.handleType(header)
		}
	}
	remainingBytes, _, err := c.readRemainingBytes()
	if err != nil {
		return err
	}

	// Read packet identifier
	packetIdentifier := binary.BigEndian.Uint16(remainingBytes[0:2])
	if s.debug {
		fmt.Printf("Received PUBREL for Packet Identifier: %v %v", packetIdentifier, c.clientID)
	}

	// Send PUBCOMP
	err = c.sendPubcomp(packetIdentifier)
	if err != nil {
		fmt.Println("Error reading PUBCOMP:", err)
		return err
	}
	if s.debug {
		fmt.Printf("Received PUBREL from Client: %v\n", c.clientID)
	}

	return nil
}

func (c *MQTTClient) sendPuback(packetIdentifier uint16) error {
	var err error
	var packet = c.packetBuf

	s := c.server
	// PUBACK fixed header
	var fixedHeader byte = PUBACK
	remainingLength := 2
	if c.protocolLevel == 5 {
		remainingLength = 4
	}

	// PUBACK variable header (packet identifier)
	packetIdentifierBytes := packet[2:4]
	binary.BigEndian.PutUint16(packetIdentifierBytes, packetIdentifier)

	// Prepare PUBACK packet
	packet[0] = fixedHeader
	packet[1] = byte(remainingLength)
	if c.protocolLevel == 5 {
		packet[4] = byte(0)
		packet[5] = byte(0)
	}

	// Send the PUBACK packet to the client
	err = c.sendPacket(packet, remainingLength+2)
	if err != nil {
		fmt.Println("Error sending packet:", err.Error())
		return err
	}
	if s.debug {
		fmt.Printf("Sent PUBACK to Client: %v\n", c.clientID)
	}
	return nil
}

func (c *MQTTClient) sendPubrec(packetIdentifier uint16) error {
	var err error
	var packet = c.packetBuf

	s := c.server
	// PUBREC fixed header
	var fixedHeader byte = PUBREC
	remainingLength := 2

	// PUBREC variable header (packet identifier)
	packetIdentifierBytes := packet[2:4]
	binary.BigEndian.PutUint16(packetIdentifierBytes, packetIdentifier)

	if c.protocolLevel == 5 {
		remainingLength += 2
	}
	// Prepare PUBREC packet
	packet[0] = fixedHeader
	packet[1] = byte(remainingLength)
	if c.protocolLevel == 5 {
		packet[4] = byte(0)
		packet[5] = byte(0)
	}

	// Send the PUBREC packet to the client
	err = c.sendPacket(packet, remainingLength+2)
	if err != nil {
		fmt.Println("Error sending packet:", err.Error())
		return err
	}
	if s.debug {
		fmt.Printf("Sent PUBREC to Client: %v\n", c.clientID)
	}
	err = c.handlePubrel(true)
	if err != nil {
		fmt.Println("Error handling PUBREL:", err)
		return err
	}
	return nil
}

func (c *MQTTClient) sendPubcomp(packetIdentifier uint16) error {
	var err error
	var packet = c.packetBuf

	s := c.server

	// PUBCOMP fixed header
	var fixedHeader byte = PUBCOMP
	remainingLength := 2

	// PUBCOMP variable header (packet identifier)
	packetIdentifierBytes := packet[2:4]
	binary.BigEndian.PutUint16(packetIdentifierBytes, packetIdentifier)

	if c.protocolLevel == 5 {
		remainingLength += 2
	}
	// Prepare PUBCOMP packet
	packet[0] = fixedHeader
	packet[1] = byte(remainingLength)
	if c.protocolLevel == 5 {
		packet[4] = byte(0)
		packet[5] = byte(0)
	}

	// Send the PUBCOMP packet to the client
	err = c.sendPacket(packet, remainingLength+2)
	if err != nil {
		fmt.Println("Error sending packet:", err.Error())
		return err
	}
	if s.debug {
		fmt.Printf("3.Sent PUBCOMP to Client: %v\n", c.clientID)
	}
	return nil
}

func (c *MQTTClient) handleUnsubscribe(reader *bufio.Reader) error {
	var remainingLength int
	var remainingBytes []byte
	var err error

	s := c.server

	// Read remaining length
	remainingLength, err = c.readRemainingLength(reader)
	if err != nil {
		fmt.Println("Error reading remaining length:", err.Error())
		return err
	}

	// Read remaining bytes
	if c.connType == "websocket" {
		remainingBytes = c.wsBuffer[2:]
	} else {
		remainingBytes = make([]byte, remainingLength)
		_, err = io.ReadFull(reader, remainingBytes)
		if err != nil {
			fmt.Println("Error reading remaining bytes:", err.Error())
			return err
		}
	}

	offset := 0
	packetIdentifier := binary.BigEndian.Uint16(remainingBytes[offset : offset+2])
	offset += 2

	if c.protocolLevel == 5 {
		//propertyLength := remainingBytes[2]
		properties, propertiesLength, bytesRead, err := c.parseProperties(remainingBytes, offset)
		if err != nil {
			return err
		}
		if s.debug {
			printStructFields(properties)
		}
		offset += (propertiesLength + bytesRead)
	}

	// Read payload (topic filters)
	//payload := remainingBytes[offset:remainingLength-offset] // remainingLength includes 2 bytes for packet identifier + 1 byte property length

	// Process each topic filter in the payload
	for offset < remainingLength {
		// Extract topic filter length
		topicFilterLength := binary.BigEndian.Uint16(remainingBytes[offset : offset+2])
		offset += 2

		// Validate topic filter length
		if topicFilterLength <= 0 || offset+int(topicFilterLength) > remainingLength {
			return fmt.Errorf("invalid topic filter length")
		}

		// Extract topic filter
		topicFilter := string(remainingBytes[offset : offset+int(topicFilterLength)])
		offset += int(topicFilterLength)

		if s.debug {
			fmt.Printf("Received UNSUBSCRIBE for topic filter '%s'\n", topicFilter)
		}

		// Handle UNSUBSCRIBE (unsubscribe the client from the topic)
		c.RemoveSubscription(topicFilter)
	}

	// Send UNSUBACK packet acknowledging the unsubscribe request
	c.sendUnsuback(packetIdentifier)
	return nil
}

func (c *MQTTClient) sendUnsuback(packetIdentifier uint16) error {
	var err error
	s := c.server

	// UNSUBACK fixed header
	var fixedHeader byte = UNSUBACK
	var remainingLength byte = 3 // 2 bytes packet identifier + 1 byte topic status( + qos 2 bits)
	if c.protocolLevel == 5 {
		remainingLength = 4 // 1 byte property length
	}

	// UNSUBACK variable header (packet identifier)
	packetIdentifierBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(packetIdentifierBytes, packetIdentifier)

	// Prepare UNSUBACK packet
	var packet []byte
	if c.protocolLevel == 5 {
		packet = append([]byte{
			fixedHeader,              // Fixed header
			remainingLength,          // Remaining length
			packetIdentifierBytes[0], // Packet Identifier MSB
			packetIdentifierBytes[1], // Packet Identifier LSB
		},
			byte(0), // The first byte status
			byte(0), // The second byte Property length
		)
	} else {
		packet = append([]byte{
			fixedHeader,              // Fixed header
			remainingLength,          // Remaining length
			packetIdentifierBytes[0], // Packet Identifier MSB
			packetIdentifierBytes[1], // Packet Identifier LSB
		},
			byte(0), // The first byte status
		)
	}

	// Send the UNSUBACK packet to the client
	err = c.sendPacket(packet, len(packet))
	if err != nil {
		fmt.Println("Error sending packet:", err.Error())
		return err
	}
	if s.debug {
		fmt.Println("Sent UNSUBACK for Packet Identifier:", packetIdentifier, packet)
	}
	return nil
}

// Insert inserts a client into the Trie under the specified topic filter
func (t *Trie) Insert(topicFilter string, client *MQTTClient) {
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
func (t *Trie) Remove(topicFilter string, client *MQTTClient) {
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
func (t *Trie) FindMatchingClients(topic string) map[*MQTTClient]bool {
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

func (c *MQTTClient) printMemStats(prefix string) {
	runtime.ReadMemStats(&c.memStats)
	fmt.Printf("%s - Alloc = %v MiB", prefix, bToMb(c.memStats.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(c.memStats.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(c.memStats.Sys))
	fmt.Printf("\tNumGC = %v\n", c.memStats.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func (c *MQTTClient) printMemoryStatus() {
	c.printMemStats("Memory Status: End of Session")
}

func (c *MQTTClient) printPublishCPUStatus() {
	if c.publishCount > 0 {
		fmt.Printf("CPU Status: Publish count: %v, Average time: %v Microseconds\n", c.publishCount, c.totalTimeMicroSec/c.publishCount)
	}
}

// readRemainingLength reads the remaining length from the reader
func (c *MQTTClient) readRemainingLength(reader io.Reader) (int, error) {
	if c.connType == "websocket" {
		return int(c.wsBuffer[1]), nil
	}
	maxBytes := 4
	count := 0
	remainingSize := 0
	nextByte := true
	mf := 1
	b := make([]byte, 1)
	for count < maxBytes && nextByte {
		_, err := reader.Read(b)
		if err != nil {
			return 0, err
		}
		if int(b[0])&0x80 == 0 {
			nextByte = false
		}
		digit := int(b[0] & 0x7F)
		remainingSize += (digit * mf)
		mf *= 128
		count += 1
	}
	return remainingSize, nil
}

func (c *MQTTClient) handlePuback() error {
	var err error

	s := c.server

	remainingBytes, _, err := c.readRemainingBytes()
	if err != nil {
		return err
	}

	packetIdentifier := binary.BigEndian.Uint16(remainingBytes[0:2])

	// Handle the PUBACK (e.g., remove the message from the pending list)
	c.acknowledgePublish(packetIdentifier)

	if s.debug {
		fmt.Printf("Received PUBACK for packet identifier %v\n", packetIdentifier)
	}

	return nil
}

func (c *MQTTClient) handlePubrec() error {
	var err error

	s := c.server
	remainingBytes, _, err := c.readRemainingBytes()
	if err != nil {
		return err
	}

	packetIdentifier := binary.BigEndian.Uint16(remainingBytes[0:2])

	// Respond with PUBREL
	err = c.sendPubrel(packetIdentifier)
	if err != nil {
		fmt.Println("Error sending PUBREL:", err)
		return err
	}

	if s.debug {
		fmt.Printf("Received PUBREC for packet identifier %v from Client: %v \n", packetIdentifier, c.clientID)
	}

	return nil
}

func (c *MQTTClient) sendPubrel(packetIdentifier uint16) error {
	var err error
	var packet = c.packetBuf

	s := c.server

	// PUBREL fixed header (type 6, flags 0x02)
	fixedHeader := byte(PUBREL | 0x02)
	remainingLength := byte(2)

	// Packet Identifier
	packetIdentifierBytes := packet[2:4]
	binary.BigEndian.PutUint16(packetIdentifierBytes, packetIdentifier)

	// Combine the fixed header, remaining length, and packet identifier
	packet[0] = fixedHeader
	packet[1] = byte(remainingLength)

	// Send the PUBREL packet
	err = c.sendPacket(packet, 4)
	if err != nil {
		fmt.Println("Error sending packet:", err.Error())
		return err
	}
	if s.debug {
		fmt.Printf("Sent PUBREL to Client: %v \n", c.clientID)
	}

	return nil
}

func (c *MQTTClient) handlePubcomp() error {
	s := c.server
	remainingBytes, _, err := c.readRemainingBytes()
	if err != nil {
		return err
	}

	packetIdentifier := binary.BigEndian.Uint16(remainingBytes)

	// Handle the PUBCOMP (e.g., remove the message from the pending acknowledgment list)
	c.acknowledgePublish(packetIdentifier)

	if s.debug {
		fmt.Printf("Received PUBCOMP from Client: %v\n", c.clientID)
	}

	return nil
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

		client := NewMQTTClient(conn, s)
		client.publishBuf = client.packetBufPool.Get().([]byte)[:0]
		client.tcpConn = tcpConn
		client.connType = "tcp"
		go client.HandleConnection()
	}
}

func (c *MQTTClient) savePublish(packetIdentifier uint16, packet []byte) {
	// Remove the message from the pending acknowledgment list
	c.pendingAckMu.Lock()
	c.pendingAck[packetIdentifier] = packet
	c.pendingAckMu.Unlock()
}

func (c *MQTTClient) acknowledgePublish(packetIdentifier uint16) {
	// Remove the message from the pending acknowledgment list
	c.pendingAckMu.Lock()
	delete(c.pendingAck, packetIdentifier)
	c.pendingAckMu.Unlock()
}

func (c *MQTTClient) parseProperties(remainingBytes []byte, offset int) (map[byte]interface{}, int, int, error) {
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
			fmt.Printf("Unknown property identifier: 0x%X\n", propIdentifier)
			return nil, 0, 0, fmt.Errorf("unknown property identifier: 0x%X", propIdentifier)
		}
	}
	return parsedProperties, propertiesLength, bytesRead, nil
}

func printStructFields(s interface{}) {
	val := reflect.ValueOf(s)
	typ := reflect.TypeOf(s)

	if val.Kind() != reflect.Struct {
		fmt.Println("Not a struct")
		return
	}

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)
		fmt.Printf("%s: %v\n", fieldType.Name, field.Interface())
	}
}

func startTCPListener(address string) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Printf("Failed to start TCP listener: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Listener tcp:default on %s started\n", address)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("TCP accept error: %v\n", err)
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func startSSLListener(address string) {
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
		go handleConnection(conn)
	}
}

func ReadMQTTPacket(wsc *websocketConn) ([]byte, error) {
	buffer := bytes.Buffer{}

	// WebSocket messages are read as frames, you may need to buffer partial messages
	for {
		// Read a WebSocket message
		_, message, err := wsc.Conn.ReadMessage()
		if err != nil {
			return nil, err
		}
		// Append the message to the buffer
		buffer.Write(message)

		// Try to extract a full MQTT packet from the buffer
		packet, err := extractMQTTPacket(&buffer)
		if err == nil {
			// We have a full MQTT packet, return it
			return packet, nil
		}

		if err == io.EOF {
			// We need to read more data, continue the loop
			continue
		}

		return nil, err // Other errors
	}
}

// extractMQTTPacket tries to extract a full MQTT packet from the buffer
func extractMQTTPacket(buffer *bytes.Buffer) ([]byte, error) {
	// We need at least 2 bytes for the fixed header
	if buffer.Len() < 2 {
		return nil, io.EOF // Need more data
	}

	// Read the first byte (Control byte)
	controlByte, err := buffer.ReadByte()
	if err != nil {
		return nil, err
	}

	remainingBytesSize, _ := buffer.ReadByte()
	if remainingBytesSize&0x80 == 0 {
		packetLength := int(remainingBytesSize + 2)
		packet := make([]byte, packetLength)
		packet[0] = controlByte
		packet[1] = remainingBytesSize
		copy(packet[2:], buffer.Next(packetLength-2))
		return packet, nil
	}

	// Read the remaining length field (variable length encoding)
	remainingLength, bytesConsumed, err := readRemainingLengthWs(buffer)
	if err != nil {
		return nil, err
	}

	// Calculate the total packet length
	totalLength := 1 + bytesConsumed + remainingLength

	// Check if we have the full packet
	if buffer.Len() < totalLength-1 {
		// Not enough data yet
		buffer.UnreadByte() // Put the control byte back
		return nil, io.EOF  // Need more data
	}

	// We have a full MQTT packet, extract it
	packet := make([]byte, totalLength)
	packet[0] = controlByte
	_, err = buffer.Read(packet[1:totalLength])
	if err != nil {
		return nil, err
	}

	return packet, nil
}

// readRemainingLength decodes the MQTT remaining length field (variable-length encoding)
func readRemainingLengthWs(buffer *bytes.Buffer) (int, int, error) {
	multiplier := 1
	value := 0
	bytesConsumed := 0

	for {
		if buffer.Len() == 0 {
			return 0, 0, io.EOF // Need more data
		}

		encodedByte, err := buffer.ReadByte()
		if err != nil {
			return 0, 0, err
		}

		value += int(encodedByte&127) * multiplier
		bytesConsumed++

		if encodedByte&128 == 0 {
			break
		}

		multiplier *= 128
		if multiplier > 128*128*128 {
			return 0, 0, fmt.Errorf("malformed remaining length")
		}
	}

	return value, bytesConsumed, nil
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
		go handleWSConnection(conn, s)
	})

	fmt.Printf("Listener ws:default on %s started\n", address)
	log.Fatal(http.ListenAndServe(address, nil))
}

// Handle WebSocket connection
func handleWSConnection(wsConn *websocket.Conn, s *MQTTServer) {

	// Create a bridge between WebSocket and net.Conn
	conn := &websocketConn{wsConn}

	// Create an MQTT client and handle the connection
	client := NewMQTTClientWs(conn, s)
	client.connWs = conn
	client.connType = "websocket"
	go client.HandleConnection()
}

// websocketConn implements net.Conn interface for WebSocket
type websocketConn struct {
	*websocket.Conn
}

// Read method for websocketConn, implements net.Conn
func (wsc *websocketConn) Read() (packet []byte, err error) {
	msgType, message, err := wsc.Conn.ReadMessage()
	if err != nil || msgType != websocket.BinaryMessage {
		if msgType != websocket.BinaryMessage {
			err = fmt.Errorf("Received message is not binary: %v\n", msgType)
		}
		return nil, err
	}
	return []byte(message), nil
}

// Write method for websocketConn, implements net.Conn
func (wsc *websocketConn) Write(b []byte) (n int, err error) {
	err = wsc.Conn.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

// LocalAddr returns the local network address
func (wsc *websocketConn) LocalAddr() net.Addr {
	return wsc.Conn.LocalAddr()
}

// RemoteAddr returns the remote network address
func (wsc *websocketConn) RemoteAddr() net.Addr {
	return wsc.Conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines
func (wsc *websocketConn) SetDeadline(t time.Time) error {
	err := wsc.Conn.SetReadDeadline(t)
	if err != nil {
		return err
	}
	return wsc.Conn.SetWriteDeadline(t)
}
func handleConnection(conn net.Conn) {
	defer conn.Close()
	// Handle TCP and SSL connections
}

func startHTTPListener(address string) {
	// Define routes
	http.HandleFunc("/", handleRoot)
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/info", handleInfo)
	http.HandleFunc("/clients", handleClients)
	http.HandleFunc("/topics", handleTopics)
	http.HandleFunc("/messages", handleMessages)
	http.HandleFunc("/stats", handleStats)
	http.HandleFunc("/config", handleConfig)

	// Serve static files
	fs := http.FileServer(http.Dir("./static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))

	// Start the HTTP server
	fmt.Printf("Starting HTTP Dashboard on %s\n", address)
	err := http.ListenAndServe(address, nil)
	if err != nil {
		fmt.Printf("Failed to start HTTP server: %v\n", err)
		os.Exit(1)
	}
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Welcome to the MQTT Server Dashboard")
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "MQTT Server is healthy")
}

func handleInfo(w http.ResponseWriter, r *http.Request) {
	// Implement logic to fetch and return server info
	fmt.Fprintf(w, "MQTT Server Information")
}

func handleClients(w http.ResponseWriter, r *http.Request) {
	// Implement logic to fetch and return connected clients
	fmt.Fprintf(w, "Connected Clients")
}

func handleTopics(w http.ResponseWriter, r *http.Request) {
	// Implement logic to fetch and return active topics
	fmt.Fprintf(w, "Active Topics")
}

func handleMessages(w http.ResponseWriter, r *http.Request) {
	// Implement logic to fetch recent messages or message stats
	fmt.Fprintf(w, "Recent Messages")
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	// Implement logic to return server statistics
	fmt.Fprintf(w, "Server Statistics")
}

func handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		// Return current configuration
		fmt.Fprintf(w, "Current Server Configuration")
	} else if r.Method == "POST" {
		// Handle configuration updates
		fmt.Fprintf(w, "Configuration Updated")
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
func dumpStack() {
	// Create a buffer to hold the stack trace
	buf := make([]byte, 1<<16) // 64 KB buffer
	n := runtime.Stack(buf, true) // Capture stack trace
	fmt.Printf("Stack trace:\n%s\n", buf[:n]) // Print the stack trace
}
