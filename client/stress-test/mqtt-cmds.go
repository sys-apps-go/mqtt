package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	quic "github.com/quic-go/quic-go"
	"io"
	"net"
	"os"
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
	ReadTimeout       = 60 * time.Second
	DefaultPacketSize = 16 * 1024
)

// Client struct
type Client struct {
	conn              net.Conn
	stream            quic.Stream
	connType          string
	ClientID          string
	config            ConfigParameters
	protocolLevel     byte
	sequenceID        uint32
	verbose           bool
	pendingAckMu      sync.Mutex
	packetIdentifier  int16
	packetBuf         []byte
	partialBuf        []byte
	currentReadOffset int
	partialLength     int
	disconnected      bool
	publishPktsAcked  int
	networkActivity   bool
	socketMu          sync.Mutex
	mu                sync.Mutex
	endConnection     bool
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

// Config holds the configuration for the MQTT client
type ConfigParameters struct {
	ProtocolLevel              byte
	Broker                     string // broker address (e.g., "localhost:1883")
	QuicServer                 string
	ConnType                   string
	KeepAlive                  uint16 // Keep alive interval in seconds
	CleanStart                 bool   // Clean start flag
	UserName                   string // userName
	Password                   string // password
	RequestProblemInformation  bool   // Request problem information
	WillDelayInterval          uint32 // Will delay interval
	RequestResponseInformation bool   // Request response information
	PayloadFormatIndicator     byte
	MessageExpiryInterval      uint32
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
	ResponseInformation        string
	ServerReference            string
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

var (
	PubMsgRcvd int
	msgMu      sync.Mutex
)

func (c *Client) Connect(config ConfigParameters) error {
	if config.ConnType == "quic" {
		tlsConf := &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{"quic-echo"},
		}

		session, err := quic.DialAddr(context.Background(), config.QuicServer, tlsConf, nil)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		stream, err := session.OpenStreamSync(context.Background())
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		c.stream = stream
	} else {
		// Split the broker address into host and port
		host, port, err := net.SplitHostPort(config.Broker)
		if err != nil {
			fmt.Printf("Invalid broker address: %v\n", err)
			os.Exit(1)
		}

		// Resolve the TCP address
		tcpAddr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(host, port))
		if err != nil {
			return fmt.Errorf("failed to resolve broker address: %v", err)
		}

		// Connect to the broker
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			return fmt.Errorf("failed to connect to broker: %v", err)
		}

		// No need for type assertion, conn is already *net.TCPConn
		if err := conn.SetReadBuffer(DefaultPacketSize); err != nil {
			fmt.Printf("Failed to set ReadBuffer: %v\n", err)
			os.Exit(1)
		}

		if err := conn.SetWriteBuffer(DefaultPacketSize); err != nil {
			fmt.Printf("Failed to set WriteBuffer: %v\n", err)
			os.Exit(1)
		}

		c.conn = conn
	}

	c.protocolLevel = byte(config.ProtocolLevel)

	// Send the CONNECT packet
	err := c.sendConnectPacket(config)
	if err != nil {
		return fmt.Errorf("failed to send CONNECT packet: %v", err)
	}

	// Read the CONNACK packet
	err = c.ReadConnackPacket()
	if err != nil {
		return fmt.Errorf("failed to read CONNACK packet: %v", err)
	}

	go c.keepAliveThread()
	return nil
}

/* 3.1.1:
 * Fixed header:
 * 0x10, Flag: 0, Remaining Length
 * Variable header:
 * Protocol name Length, Protocol Name, Protocol Level(1 byte) Values 4/5
 * Connect flag: 1 Byte:
 * User Name flag 1 bit, password 1 bit, Will Retain: 1 bit, QoS: 2 bits, Will flag: 1 bit,
 * Clean session: 1 bit, Reserved: 1 bit
 * Keep Alive: 2 bytes
 * Payload:
 * Client Identifier, Will Topic, Will Message, User Name, password
 * 5:
 * Same: Except in Variable header at the end: Properties Length as Variable Byte
 * Integer
 *
	SessionExpiryInterval uint32
	ReceiveMaximum        uint16
	MaximumPacketSize     uint32
	TopicAliasMaximum     uint16
	RequestResponseInfo   byte
	RequestProblemInfo    byte
	UserProperty          map[string]string
	AuthenticationMethod  string
	AuthenticationData    []byte
*/

func (c *Client) sendConnectPacket(config ConfigParameters) error {
	var packet bytes.Buffer

	// Fixed header
	packet.WriteByte(byte(CONNECT))
	packet.WriteByte(0) // Remaining length (placeholder)

	// Protocol Name
	protocolName := "MQTT"
	protocolNameLength := uint16(len(protocolName))
	binary.Write(&packet, binary.BigEndian, protocolNameLength)
	packet.WriteString(protocolName)

	// Protocol Level
	protocolLevel := c.protocolLevel
	packet.WriteByte(byte(protocolLevel))

	// Connect Flags
	connectFlags := byte(0)
	if config.CleanStart {
		connectFlags |= 0x02 // Set the Clean Start flag
	}
	if config.UserName != "" {
		connectFlags |= 0x80 // Set the userName flag
	}
	if config.Password != "" {
		connectFlags |= 0x40 // Set the password flag
	}
	packet.WriteByte(connectFlags)

	// Keep Alive
	binary.Write(&packet, binary.BigEndian, config.KeepAlive)

	if c.protocolLevel == 5 {
		// Properties
		var properties bytes.Buffer

		// Iterate over each property and encode it into the properties buffer

		if config.SessionExpiryInterval > 0 {
			properties.WriteByte(0x11) // Property identifier for Session Expiry Interval
			binary.Write(&properties, binary.BigEndian, config.SessionExpiryInterval)
		}
		if len(config.AuthenticationMethod) > 0 {
			properties.WriteByte(0x15) // Property identifier for Authentication Method
			binary.Write(&properties, binary.BigEndian, uint16(len(config.AuthenticationMethod)))
			properties.WriteString(config.AuthenticationMethod)
		}
		if len(config.AuthenticationData) > 0 {
			properties.WriteByte(0x16) // Property identifier for Authentication Data
			binary.Write(&properties, binary.BigEndian, uint16(len(config.AuthenticationData)))
			properties.Write(config.AuthenticationData)
		}
		if config.RequestProblemInfo > 0 {
			properties.WriteByte(0x17) // Property identifier for Request Problem Information
			properties.WriteByte(config.RequestProblemInfo)
		}
		if config.RequestResponseInfo > 0 {
			properties.WriteByte(0x19) // Property identifier for Request Response Information
			properties.WriteByte(config.RequestResponseInfo)
		}
		if config.ReceiveMaximum > 0 {
			properties.WriteByte(0x21) // Property identifier for Receive Maximum
			binary.Write(&properties, binary.BigEndian, config.ReceiveMaximum)
		}
		if config.TopicAliasMaximum > 0 {
			properties.WriteByte(0x22) // Property identifier for Topic Alias Maximum
			binary.Write(&properties, binary.BigEndian, config.TopicAliasMaximum)
		}
		for key, value := range config.UserProperty {
			properties.WriteByte(0x26) // Property identifier for User Property
			binary.Write(&properties, binary.BigEndian, uint16(len(key)))
			properties.WriteString(key)
			binary.Write(&properties, binary.BigEndian, uint16(len(value)))
			properties.WriteString(value)
		}
		if config.MaximumPacketSize > 0 {
			properties.WriteByte(0x27) // Property identifier for Maximum Packet Size
			binary.Write(&properties, binary.BigEndian, config.MaximumPacketSize)
		}

		if c.verbose {
			// Print out the encoded properties
			fmt.Printf("Properties Length: %d\n", properties.Len())
			fmt.Printf("Properties: %v\n", properties.Bytes())
		}

		// Write properties length and properties to the variable header
		propertyLength := properties.Len()
		//encodeVariableByteInteger(&packet, propertyLength)
		plBytes := encodeVariableByteInteger(uint32(propertyLength))
		for i := 0; i < len(plBytes); i++ {
			packet.WriteByte(plBytes[i])
		}
		packet.Write(properties.Bytes())
	}

	// Client ID
	clientIDLength := uint16(len(c.ClientID))
	binary.Write(&packet, binary.BigEndian, clientIDLength)
	packet.WriteString(c.ClientID)

	// userName (if provided)
	if config.UserName != "" {
		usernameLength := uint16(len(config.UserName))
		binary.Write(&packet, binary.BigEndian, usernameLength)
		packet.WriteString(config.UserName)
	}

	// password (if provided)
	if config.Password != "" {
		passwordLength := uint16(len(config.Password))
		binary.Write(&packet, binary.BigEndian, passwordLength)
		packet.WriteString(config.Password)
	}

	// Remaining Length
	remainingLength := len(packet.Bytes()) - 2
	packet.Bytes()[1] = byte(remainingLength)
	//encodedLength, err := encodeRemainingLength(remainingLength)

	// Write the fixed header, variable header, and payload to the writer
	c.writePacket(packet.Bytes())

	return nil
}

func (c *Client) ReadConnackPacket() error {
	// Read the fixed header
	header, err := c.readByte()
	if err != nil {
		return fmt.Errorf("failed to read fixed header: %v", err)
	}
	if header&0xF0 != CONNACK { // Check if it's a CONNACK packet
		return fmt.Errorf("unexpected packet type: %v", header&0xF0)
	}

	// Read the remaining length
	remainingLength, err := c.readVariableByteInteger()
	if err != nil {
		return fmt.Errorf("failed to read remaining length: %v", err)
	}

	// Read the variable header
	variableHeader := make([]byte, remainingLength)
	_, err = c.readFull(variableHeader)
	if err != nil {
		return fmt.Errorf("failed to read variable header: %v", err)
	}

	// Check the connect acknowledge flags and return code
	if variableHeader[1] != 0 { // Non-zero return code indicates an error
		return fmt.Errorf("connection refused, return code: %v", variableHeader[1])
	}

	if c.protocolLevel == 5 {
		// Parse and print properties
		propertiesLength, bytesRead := decodeVariableByteInteger(variableHeader[2:])
		properties := variableHeader[2+bytesRead : 2+bytesRead+propertiesLength]
		if c.verbose {
			fmt.Printf("Properties Length: %d\n", propertiesLength)
			fmt.Printf("Properties: %v\n", properties)
		}
	}

	return nil
}

func (c *Client) readVariableByteInteger() (int, error) {
	multiplier := 1
	value := 0
	for {
		encodedByte, err := c.readByte()
		if err != nil {
			return 0, err
		}
		value += int(encodedByte&127) * multiplier
		if encodedByte&128 == 0 {
			break
		}
		multiplier *= 128
		if multiplier > 128*128*128 {
			return 0, fmt.Errorf("malformed remaining length")
		}
	}
	return value, nil
}

func decodeVariableByteInteger(data []byte) (int, int) {
	multiplier := 1
	value := 0
	bytesRead := 0
	for {
		encodedByte := data[bytesRead]
		value += int(encodedByte&127) * multiplier
		bytesRead++
		if encodedByte&128 == 0 {
			break
		}
		multiplier *= 128
		if bytesRead > 3 {
			return 0, 0 // malformed variable byte integer
		}
	}
	return value, bytesRead
}

// encodeVariableByteInteger encodes an integer into a variable byte format as specified by MQTT protocol
func encodeVariableByteInteger(value uint32) []byte {
	var encoded []byte
	for {
		digit := value % 128
		value /= 128
		if value > 0 {
			digit |= 0x80
		}
		encoded = append(encoded, byte(digit))
		if value == 0 {
			break
		}
	}
	return encoded
}

// encodeVariableByteInteger encodes an integer into MQTT's variable byte format and writes it to `buf`.
func encodeVariableByteIntegerArray(buf *bytes.Buffer, length int) {
	for {
		digit := length % 128
		length = length / 128
		if length > 0 {
			digit = digit | 0x80
		}
		buf.WriteByte(byte(digit))
		if length <= 0 {
			break
		}
	}
}

// NewClient creates a new MQTT client
func NewClient(clientID string, keepAlive uint16, connType string) *Client {
	c := &Client{
		ClientID:   clientID,
		packetBuf:  make([]byte, DefaultPacketSize),
		partialBuf: make([]byte, 1024),
		connType:   connType,
	}
	c.config.KeepAlive = keepAlive
	return c
}

// Publish sends a PUBLISH packet
func (c *Client) Publish(topic, message string, qos byte) error {
	packet := c.buildPublishPacket(topic, message, qos)
	if c.connType == "tcp" {
		err := c.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if err != nil {
			// Handle error
			return err
		}
	}
	err := c.writePacket(packet)
	if err != nil {
		return err
	}

	if qos == 1 {
		return c.readPuback()
	} else if qos == 2 {
		return c.readPubrec()
	}
	c.mu.Lock()
	c.networkActivity = true
	c.mu.Unlock()
	return nil
}

// buildPublishPacket constructs a PUBLISH packet
func (c *Client) buildPublishPacket(topic, message string, qos byte) []byte {
	var packet bytes.Buffer

	// Fixed header
	packet.WriteByte(byte(PUBLISH | qos<<1))
	packet.WriteByte(0) // Remaining length (placeholder)

	// Variable header
	binary.Write(&packet, binary.BigEndian, uint16(len(topic)))
	packet.WriteString(topic)

	if qos > 0 {
		c.packetIdentifier++
		binary.Write(&packet, binary.BigEndian, uint16(c.packetIdentifier))
	}
	if c.protocolLevel == 5 {
		packet.WriteByte(0) // Property Length
		// Set Properties
	}

	// Payload
	packet.WriteString(message)

	// Update remaining length
	remainingLength := len(packet.Bytes()) - 2
	packet.Bytes()[1] = byte(remainingLength)

	return packet.Bytes()
}

// readPuback reads and processes a PUBACK packet: QoS = 1
func (c *Client) readPuback() error {
	var header byte
	var err error

	header, err = c.readByte()
	if err != nil {
		return err
	}

	if header&0xF0 == PINGRESP {
		_, err = c.readByte()
		if err != nil {
			return err
		}
		header, err = c.readByte()
		if err != nil {
			return err
		}
	}

	if header&0xF0 != PUBACK {
		return fmt.Errorf("unexpected packet type: %d", header&0xF0)
	}

	remainingLength, err := c.readByte()
	if err != nil {
		return err
	}

	remainingBytes := make([]byte, remainingLength)
	_, err = c.readFull(remainingBytes)
	if err != nil {
		return err
	}

	return nil
}

// readPubrec reads and processes a PUBREC packet: Qos = 2: Read PUBREC -> Send PUBREL -> Read PUBCOMP
func (c *Client) readPubrec() error {
	var header byte
	var err error

	header, err = c.readByte()
	if err != nil {
		return err
	}

	if header&0xF0 == PINGRESP {
		_, err = c.readByte()
		if err != nil {
			return err
		}
		header, err = c.readByte()
		if err != nil {
			return err
		}
	}

	if header&0xF0 != PUBREC {
		return fmt.Errorf("unexpected packet type: %d", header&0xF0)
	}

	remainingLength, err := c.readByte()
	if err != nil {
		return err
	}

	remainingBytes := make([]byte, remainingLength)
	_, err = c.readFull(remainingBytes)
	if err != nil {
		return err
	}

	packetIdentifier := binary.BigEndian.Uint16(remainingBytes[0:2])

	// Respond with PUBREL
	err = c.sendPubrel(packetIdentifier, remainingBytes, remainingLength)
	if err != nil {
		fmt.Println("Error sending PUBREL:", err)
		return err
	}

	return nil
}

func (c *Client) buildSubscribePacket(topic string, qos byte) []byte {
	packet := new(bytes.Buffer)

	// Fixed header
	packet.WriteByte(0x82) // Subscribe packet type with reserved bits

	// Variable header
	remainingLength := 2 + 2 + len(topic) + 1 // Packet ID (2 bytes) + Topic Length (2 bytes) + Topic + QoS
	if c.protocolLevel == 5 {
		remainingLength += 1 // Property length
	}
	packet.WriteByte(byte(remainingLength))

	// Packet ID
	binary.Write(packet, binary.BigEndian, uint16(1))
	if c.protocolLevel == 5 {
		packet.WriteByte(0)
	}

	// Topic
	binary.Write(packet, binary.BigEndian, uint16(len(topic)))
	packet.WriteString(topic)

	// QoS
	packet.WriteByte(qos)

	return packet.Bytes()
}

func (c *Client) readSuback() error {
	// Read SUBACK fixed header
	header, err := c.readByte()
	if err != nil {
		return err
	}
	if header&0xF0 != SUBACK { // 9 is SUBACK
		return fmt.Errorf("expected SUBACK, got packet type %d", header&0xF0)
	}

	remainingLength, err := c.readByte()
	if err != nil {
		return err
	}
	remainingBytes := make([]byte, remainingLength)
	_, err = c.readFull(remainingBytes)
	if err != nil {
		return err
	}

	statusCodeOffset := 2
	if c.protocolLevel == 5 {
		statusCodeOffset += 1
	}

	statusCode := remainingBytes[statusCodeOffset]
	if c.verbose {
		fmt.Println(statusCode, remainingBytes)
	}

	return nil
}

func (c *Client) Subscribe(topic string, qos byte, callback func(string, string)) error {
	packet := c.buildSubscribePacket(topic, qos)
	err := c.writePacket(packet)
	if err != nil {
		return err
	}

	err = c.readSuback()
	if err != nil {
		return err
	}

	go c.startMsgSubscription(topic, qos, callback)
	return nil
}

func (c *Client) startMsgSubscription(topic string, qos byte, callback func(string, string)) error {
	for {
		if c.disconnected {
			return nil
		}
		packets, err := c.processMQTTPackets()

		if err != nil {
			if err.Error() == "read timeout" {
				continue
			}
			return err
		}

		for _, pkt := range packets {
			header := pkt.hdrStart[0]
			cmdType := header & 0xF0
			remainingBytes := pkt.remainingBytes

			switch cmdType {
			case UNSUBACK:
			case PUBLISH:
				// Process the publish packet
				topicLen := binary.BigEndian.Uint16(remainingBytes[0:2])
				receivedTopic := string(remainingBytes[2 : 2+topicLen])
				if receivedTopic != topic {
					continue // ignore messages for other topics
				}

				// Extract QoS level and Packet Identifier if QoS > 0
				receivedQos := int((header & 0x06) >> 1)
				var packetID uint16
				payloadStart := 2 + topicLen
				if receivedQos > 0 {
					packetID = binary.BigEndian.Uint16(remainingBytes[payloadStart : payloadStart+2])
					payloadStart += 2
				}

				payload := remainingBytes[payloadStart:]
				// Call the callback function with the message
				message := string(payload)
				callback(topic, message)

				// Send PUBACK for QoS 1
				if receivedQos == 1 {
					ackPacket := new(bytes.Buffer)
					ackPacket.WriteByte(PUBACK) // PUBACK fixed header
					ackPacket.WriteByte(2)      // Remaining length
					binary.Write(ackPacket, binary.BigEndian, packetID)
					c.pendingAckMu.Lock()
					err := c.writePacket(ackPacket.Bytes())
					if err != nil {
						c.pendingAckMu.Unlock()
						return err
					}
					c.pendingAckMu.Unlock()
				}

				// Handle QoS 2
				if receivedQos == 2 {
					recPacket := new(bytes.Buffer)
					recPacket.WriteByte(PUBREC) // PUBREC fixed header
					recPacket.WriteByte(2)      // Remaining length
					binary.Write(recPacket, binary.BigEndian, packetID)
					c.pendingAckMu.Lock()
					err := c.writePacket(recPacket.Bytes())
					if err != nil {
						c.pendingAckMu.Unlock()
						return err
					}
					c.pendingAckMu.Unlock()
				}
			case PUBREL:
				packetID := binary.BigEndian.Uint16(remainingBytes[0:2])

				// Send PUBCOMP
				compPacket := new(bytes.Buffer)
				compPacket.WriteByte(PUBCOMP) // PUBCOMP fixed header
				compPacket.WriteByte(2)       // Remaining length
				binary.Write(compPacket, binary.BigEndian, packetID)
				c.pendingAckMu.Lock()
				err := c.writePacket(compPacket.Bytes())
				if err != nil {
					c.pendingAckMu.Unlock()
					return err
				}
				c.pendingAckMu.Unlock()
			case PINGRESP:
			}
		}
	}
}

// Unsubscribe sends an UNSUBSCRIBE packet
func (c *Client) Unsubscribe(topic string, readUnSubAck bool) error {
	packet := c.buildUnsubscribePacket(topic)
	err := c.writePacket(packet)
	if err != nil {
		return err
	}

	if readUnSubAck {
		c.readUnsuback(true)
	}
	return nil
}

// buildUnsubscribePacket constructs an UNSUBSCRIBE packet
func (c *Client) buildUnsubscribePacket(topic string) []byte {
	var packet bytes.Buffer

	// Fixed header
	packet.WriteByte(byte(UNSUBSCRIBE | 2))
	packet.WriteByte(0) // Remaining length (placeholder)

	// Variable header
	packet.WriteByte(0) // Packet Identifier MSB
	packet.WriteByte(1) // Packet Identifier LSB

	if c.protocolLevel == 5 {
		packet.WriteByte(0) // Property length
	}

	// Payload
	binary.Write(&packet, binary.BigEndian, uint16(len(topic)))
	packet.WriteString(topic)

	// Update remaining length
	remainingLength := len(packet.Bytes()) - 2
	packet.Bytes()[1] = byte(remainingLength)

	return packet.Bytes()
}

// readUnsuback reads and processes an UNSUBACK packet
func (c *Client) readUnsuback(fullPkt bool) error {
	if fullPkt {
		header, err := c.readByte()
		if err != nil {
			return err
		}
		if header&0xF0 != UNSUBACK {
			return fmt.Errorf("unexpected packet type: %d", header&0xF0)
		}
	}

	remainingLength, err := c.readByte()
	if err != nil {
		return err
	}

	remainingBytes := make([]byte, remainingLength)
	_, err = c.readFull(remainingBytes)

	if err != nil {
		return err
	}

	return nil
}

// Disconnect sends a DISCONNECT packet
func (c *Client) Disconnect() error {
	packet := []byte{byte(DISCONNECT), 0}
	err := c.writePacket(packet)
	if err != nil {
		return err
	}
	c.disconnected = true

	if c.connType == "tcp" {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) sendPubrel(packetIdentifier uint16, remainingBytes []byte, remainingLength byte) error {
	// PUBREL fixed header (type 6, flags 0x02)
	fixedHeader := byte(PUBREL | 0x02)

	// Combine the fixed header, remaining length, and remaining bytes
	packet := append([]byte{fixedHeader, remainingLength}, remainingBytes...)

	// Send the PUBREL packet
	_, err := c.conn.Write(packet)
	if err != nil {
		fmt.Println("Error writing PUBREL packet:", err)
		return err
	}

	c.readPubcomp()

	return nil
}

func (c *Client) readPubcomp() error {
	var header byte
	var err error

	header, err = c.readByte()
	if err != nil {
		return err
	}

	if header&0xF0 == PINGRESP {
		_, err = c.readByte()
		if err != nil {
			return err
		}
		header, err = c.readByte()
		if err != nil {
			return err
		}
	}

	if header&0xF0 != PUBCOMP {
		return fmt.Errorf("unexpected packet type: %d", header&0xF0)
	}

	// Read the remaining length
	remainingLength, err := c.readByte()
	if err != nil {
		fmt.Println("Error reading remaining length:", err)
		return err
	}
	remainingBytes := make([]byte, remainingLength)
	_, err = c.readFull(remainingBytes)
	if err != nil {
		fmt.Println("Error reading remaining bytes:", err)
		return err
	}
	if remainingLength < 2 {
		err := fmt.Errorf("Invalid remaining length for PUBCOMP: %d", remainingLength)
		fmt.Println(err)
		return err
	}

	//packetIdentifier := binary.BigEndian.Uint16(remainingBytes[0:2])
	if c.protocolLevel == 5 {
		pubcompReasonCode := remainingBytes[2]
		if pubcompReasonCode != 0 {
			err := fmt.Errorf("Pubcomp status code: %v", pubcompReasonCode)
			return err
		}
		// Process Property
		//propertyLength := remainingBytes[3]
	}
	return nil
}

/*
 * Both version 4 and version 5 has the same format for PINGREQ and PINGRESP
 */
func (c *Client) SendPingReq() error {
	// PINGREQ fixed header (type 12, flags 0x00)
	fixedHeader := PINGREQ

	// Packet structure: fixed header + remaining length
	packet := []byte{
		fixedHeader, // First byte: packet type
		0x00,        // Second byte: Remaining Length (always 0 for PINGREQ)
	}

	// Send the PINGREQ packet
	_, err := c.conn.Write(packet)
	if err != nil {
		return fmt.Errorf("error writing PINGREQ packet: %w", err)
	}
	if c.verbose {
		fmt.Println("Sent PINGREQ")
	}

	return nil
}

// encodeRemainingLength encodes the remaining length for MQTT 5.0 connect packet
func encodeRemainingLength(length int) ([]byte, error) {
	if length < 0 || length > 268435455 {
		return nil, fmt.Errorf("Invalid remaining length: %d", length)
	}

	var encoded bytes.Buffer
	for {
		encodedByte := byte(length % 128)
		length /= 128
		// if there are more data to encode, set the top bit of this byte
		if length > 0 {
			encodedByte |= 0x80
		}
		encoded.WriteByte(encodedByte)

		if length <= 0 {
			break
		}
	}
	return encoded.Bytes(), nil
}

func (c *Client) loadAllProperties() {
	config := c.config
	if c.protocolLevel == 5 {
		// Properties
		var properties bytes.Buffer

		// Iterate over each property and encode it into the properties buffer
		if config.PayloadFormatIndicator > 0 {
			properties.WriteByte(0x01) // Property identifier for Payload Format Indicator
			properties.WriteByte(config.PayloadFormatIndicator)
		}
		if config.MessageExpiryInterval > 0 {
			properties.WriteByte(0x02) // Property identifier for Message Expiry Interval
			binary.Write(&properties, binary.BigEndian, config.MessageExpiryInterval)
		}
		if len(config.ContentType) > 0 {
			properties.WriteByte(0x03) // Property identifier for Content Type
			binary.Write(&properties, binary.BigEndian, uint16(len(config.ContentType)))
			properties.WriteString(config.ContentType)
		}
		if len(config.ResponseTopic) > 0 {
			properties.WriteByte(0x08) // Property identifier for Response Topic
			binary.Write(&properties, binary.BigEndian, uint16(len(config.ResponseTopic)))
			properties.WriteString(config.ResponseTopic)
		}
		if len(config.CorrelationData) > 0 {
			properties.WriteByte(0x09) // Property identifier for Correlation Data
			binary.Write(&properties, binary.BigEndian, uint16(len(config.CorrelationData)))
			properties.Write(config.CorrelationData)
		}
		if config.SubscriptionIdentifier > 0 {
			properties.WriteByte(0x0B) // Property identifier for Subscription Identifier
			binary.Write(&properties, binary.BigEndian, uint32(config.SubscriptionIdentifier))
		}
		if config.SessionExpiryInterval > 0 {
			properties.WriteByte(0x11) // Property identifier for Session Expiry Interval
			binary.Write(&properties, binary.BigEndian, config.SessionExpiryInterval)
		}
		if len(config.AssignedClientID) > 0 {
			properties.WriteByte(0x12) // Property identifier for Assigned Client Identifier
			binary.Write(&properties, binary.BigEndian, uint16(len(config.AssignedClientID)))
			properties.WriteString(config.AssignedClientID)
		}
		if config.ServerKeepAlive > 0 {
			properties.WriteByte(0x13) // Property identifier for Server Keep Alive
			binary.Write(&properties, binary.BigEndian, config.ServerKeepAlive)
		}
		if len(config.AuthenticationMethod) > 0 {
			properties.WriteByte(0x15) // Property identifier for Authentication Method
			binary.Write(&properties, binary.BigEndian, uint16(len(config.AuthenticationMethod)))
			properties.WriteString(config.AuthenticationMethod)
		}
		if len(config.AuthenticationData) > 0 {
			properties.WriteByte(0x16) // Property identifier for Authentication Data
			binary.Write(&properties, binary.BigEndian, uint16(len(config.AuthenticationData)))
			properties.Write(config.AuthenticationData)
		}
		if config.RequestProblemInfo > 0 {
			properties.WriteByte(0x17) // Property identifier for Request Problem Information
			properties.WriteByte(config.RequestProblemInfo)
		}
		if config.WillDelayInterval > 0 {
			properties.WriteByte(0x18) // Property identifier for Will Delay Interval
			binary.Write(&properties, binary.BigEndian, config.WillDelayInterval)
		}
		if config.RequestResponseInfo > 0 {
			properties.WriteByte(0x19) // Property identifier for Request Response Information
			properties.WriteByte(config.RequestResponseInfo)
		}
		if len(config.ResponseInformation) > 0 {
			properties.WriteByte(0x1A) // Property identifier for Response Information
			binary.Write(&properties, binary.BigEndian, uint16(len(config.ResponseInformation)))
			properties.WriteString(config.ResponseInformation)
		}
		if len(config.ServerReference) > 0 {
			properties.WriteByte(0x1C) // Property identifier for Server Reference
			binary.Write(&properties, binary.BigEndian, uint16(len(config.ServerReference)))
			properties.WriteString(config.ServerReference)
		}
		if len(config.ReasonString) > 0 {
			properties.WriteByte(0x1F) // Property identifier for Reason String
			binary.Write(&properties, binary.BigEndian, uint16(len(config.ReasonString)))
			properties.WriteString(config.ReasonString)
		}

		if config.ReceiveMaximum > 0 {
			properties.WriteByte(0x21) // Property identifier for Receive Maximum
			binary.Write(&properties, binary.BigEndian, config.ReceiveMaximum)
		}
		if config.TopicAliasMaximum > 0 {
			properties.WriteByte(0x22) // Property identifier for Topic Alias Maximum
			binary.Write(&properties, binary.BigEndian, config.TopicAliasMaximum)
		}
		if config.TopicAlias > 0 {
			properties.WriteByte(0x23) // Property identifier for Topic Alias
			binary.Write(&properties, binary.BigEndian, config.TopicAlias)
		}
		if config.MaximumQoS > 0 {
			properties.WriteByte(0x24) // Property identifier for Maximum QoS
			properties.WriteByte(config.MaximumQoS)
		}

		if config.RetainAvailable > 0 {
			properties.WriteByte(0x25) // Property identifier for Retain Available
			properties.WriteByte(config.RetainAvailable)
		}
		for key, value := range config.UserProperty {
			properties.WriteByte(0x26) // Property identifier for User Property
			binary.Write(&properties, binary.BigEndian, uint16(len(key)))
			properties.WriteString(key)
			binary.Write(&properties, binary.BigEndian, uint16(len(value)))
			properties.WriteString(value)
		}
		if config.MaximumPacketSize > 0 {
			properties.WriteByte(0x27) // Property identifier for Maximum Packet Size
			binary.Write(&properties, binary.BigEndian, config.MaximumPacketSize)
		}
		if config.WildcardSubscription > 0 {
			properties.WriteByte(0x28) // Property identifier for Wildcard Subscription Available
			properties.WriteByte(config.WildcardSubscription)
		}
		if config.SubscriptionIDAvailable > 0 {
			properties.WriteByte(0x29) // Property identifier for Subscription Identifier Available
			properties.WriteByte(config.SubscriptionIDAvailable)
		}
		if config.SharedSubscription > 0 {
			properties.WriteByte(0x2A) // Property identifier for Shared Subscription Available
			properties.WriteByte(config.SharedSubscription)
		}
	}

}

func (c *Client) processMQTTPackets() ([]Packet, error) {
	var hdrStart []byte
	var err error
	var n, readN int

	conn := c.conn

	if c.connType == "tcp" {
		err = conn.SetReadDeadline(time.Now().Add(ReadTimeout))
		if err != nil {
			return nil, fmt.Errorf("error setting read deadline: %v", err)
		}
	}

	n = 0

	// Handle partial data from previous read
	if c.partialLength > 0 {
		copy(c.packetBuf[0:], c.partialBuf[:c.partialLength])
		n = c.partialLength
		c.partialLength = 0
	}

	retryCount := 0
	for retryCount < 3 {
		if c.connType == "tcp" {
			readN, err = conn.Read(c.packetBuf[n:])
		} else if c.connType == "quic" {
			readN, err = c.stream.Read(c.packetBuf[n:])
		}
		if err != nil {
			if err == io.EOF {
				return nil, err
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return nil, fmt.Errorf("read timeout")
			}
			if !isRetryableError(err) {
				return nil, fmt.Errorf("error reading from connection: %v", err)
			}
		} else {
			n += readN
			break
		}
		retryCount++
	}
	if err != nil {
		return nil, fmt.Errorf("error reading from connection: %v", err)
	}

	if n == 0 {
		return nil, errors.New("no data received")
	}

	var packets []Packet
	index := 0

	for index < n {
		if index+1 >= n {
			break // Not enough data for a complete packet
		}

		packetStart := index
		hdrStart = c.packetBuf[index:]
		cmd := c.packetBuf[index] & 0xF0
		header := c.packetBuf[index]
		index++

		// Parse remaining length
		remainingLength := 0
		multiplier := 1
		remainingLengthSize := 0
		for i := 0; i < 4; i++ {
			if index >= n {
				break // Not enough data to parse remaining length
			}

			encodedByte := c.packetBuf[index]
			index++

			remainingLength += int(encodedByte&127) * multiplier
			remainingLengthSize++
			multiplier *= 128

			if encodedByte&128 == 0 {
				break
			}
		}

		if index+remainingLength > n {
			partialLength := n - packetStart
			if partialLength > len(c.partialBuf) {
				c.partialBuf = make([]byte, partialLength)
			}
			copy(c.partialBuf[0:], c.packetBuf[packetStart:packetStart+partialLength])
			c.partialLength = partialLength
			index = packetStart // Reset index to the start of this incomplete packet
			break               // Not enough data for the full payload
		}

		p := Packet{
			cmd:                 cmd,
			header:              header,
			qos:                 (header & 0x06) >> 1,
			remainingLengthSize: remainingLengthSize,
			remainingLength:     remainingLength,
			payloadStart:        index,
			remainingBytes:      c.packetBuf[index : index+remainingLength],
			hdrStart:            hdrStart,
		}

		packets = append(packets, p)
		index += remainingLength
	}

	return packets, nil
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "connection reset") ||
		strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(err.Error(), "i/o timeout")
}

func (c *Client) writePacket(packet []byte) error {
	c.socketMu.Lock()
	defer c.socketMu.Unlock()
	switch c.connType {
	case "tcp":
		_, err := c.conn.Write(packet)
		return err
	case "websocket":
		//_, err := c.connWs.Write(packet)
		//return err
	case "quic":
		_, err := c.stream.Write(packet)
		return err
	default:
		return fmt.Errorf("unknown connection type: %s", c.connType)
	}
	return fmt.Errorf("unknown connection type: %s", c.connType)
}

func (c *Client) readByte() (byte, error) {
	switch c.connType {
	case "tcp":
		buf := make([]byte, 1)
		_, err := c.conn.Read(buf)
		if err == nil {
			return buf[0], nil
		}
		return 0, err
	case "websocket":
		//_, err := c.connWs.Write(packet)
		//return err
	case "quic":
		buf := make([]byte, 1)
		_, err := io.ReadFull(c.stream, buf)
		return buf[0], err
	default:
	}
	return 0, fmt.Errorf("unknown connection type: %s", c.connType)
}

func (c *Client) readFull(buf []byte) ([]byte, error) {
	switch c.connType {
	case "tcp":
		_, err := c.conn.Read(buf)
		return buf, err
	case "websocket":
		//_, err := c.connWs.Write(packet)
		//return nil, err
	case "quic":
		_, err := io.ReadFull(c.stream, buf)
		return buf, err
	default:
	}
	return nil, fmt.Errorf("unknown connection type: %s", c.connType)
}

func (c *Client) keepAliveThread() {
	ticker := time.NewTicker(time.Duration(c.config.KeepAlive-10) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			if c.networkActivity {
				c.networkActivity = false
				c.mu.Unlock()
			} else {
				c.mu.Unlock()
				err := c.SendPingReq()
				if err != nil {
					return
				}
			}
		}
	}
}
