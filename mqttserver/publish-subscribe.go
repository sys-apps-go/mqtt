package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/shirou/gopsutil/cpu"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

func (c *MQTTClient) processMQTTPacketsTCP_QUIC() ([]Packet, []byte, int, bool, bool, string, error) {
	var hdrStart []byte
	var topicName string
	var err error
	var n, readN int

	s := c.server
	m := make(map[string]bool)
	conn := c.conn

	if c.connType == "tcp" {
		err = conn.SetReadDeadline(time.Now().Add(ReadTimeout))
		if err != nil {
			return nil, nil, 0, false, false, "", fmt.Errorf("error setting read deadline: %v", err)
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
				if c.connType == "tcp" {
					if c.willRetain {
						c.sendRetainedMsgToSubscriber([]string{c.willTopic})
					}
					c.removeConnection(c.clientID)
				}
				return nil, nil, 0, false, false, "", err
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return nil, nil, 0, false, false, "", fmt.Errorf("read timeout")
			}
			if !isRetryableError(err) {
				return nil, nil, 0, false, false, "", fmt.Errorf("error reading from connection: %v", err)
			}
		} else {
			n += readN
			break
		}
		retryCount++
	}
	if err != nil {
		return nil, nil, 0, false, false, "", fmt.Errorf("error reading from connection: %v", err)
	}

	if n == 0 {
		return nil, nil, 0, false, false, "", errors.New("no data received")
	}

	var packets []Packet
	index := 0

	allPublishCmds := true
	for index < n {
		if index+1 >= n {
			c.partialBuf[0] = c.packetBuf[index]
			c.partialLength = 1
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
		if cmd == PUBLISH && header&0x01 != 0 {
			p.retain = true
		}
		if cmd == PUBLISH && header&0x08 != 0 {
			p.duplicate = true
		}

		if cmd != PUBLISH {
			allPublishCmds = false
		} else {
			if index+2 > n {
				break // Not enough data for topic name length
			}
			topicNameLength := int(binary.BigEndian.Uint16(c.packetBuf[index : index+2]))
			if index+2+topicNameLength > n {
				break // Not enough data for full topic name
			}
			topicNameBytes := c.packetBuf[index+2 : index+2+topicNameLength]
			topicName = string(topicNameBytes)
			if p.retain {
				s.mu.Lock()
				buf := make([]byte, p.remainingLength+2)
				copy(buf, p.hdrStart[0:p.remainingLength+2])
				s.retainedMessages.Insert(topicName, buf)
				s.mu.Unlock()
			}
			offset := int(2 + topicNameLength)
			if p.qos > 0 && offset+2 <= len(p.remainingBytes) {
				p.packetIdentifier = binary.BigEndian.Uint16(p.remainingBytes[offset : offset+2])
			}
			m[topicName] = true
		}

		packets = append(packets, p)
		index += remainingLength
	}

	if len(m) > 1 {
		allPublishCmds = false
	}
	qos := 0
	allSameQoS := true
	for i, pkt := range packets {
		if i != 0 {
			if qos != int(pkt.qos) {
				allSameQoS = false
				break
			}
		} else {
			qos = int(pkt.qos)
		}
	}
	return packets, c.packetBuf[:index], n, allPublishCmds, allSameQoS, topicName, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (c *MQTTClient) sendPacket(packet []byte, pktSize int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.server

	var err error
	retries := 3
	backoff := time.Millisecond * 100

	for i := 0; i < retries; i++ {
		err = c.writePacket(packet[:pktSize])
		if err == nil {
			return nil
		}

		if !isRetryableError(err) {
			return fmt.Errorf("non-retryable error sending packet: %v", err)
		}

		s.logger.Printf("Error sending packet (attempt %d/%d): %v", i+1, retries, err)
		time.Sleep(backoff)
		backoff *= 2 // Exponential backoff
	}

	c.removeConnection(c.clientID)
	return fmt.Errorf("failed to send packet after %d attempts: %v", retries, err)
}

func (c *MQTTClient) writePacket(packet []byte) error {
	switch c.connType {
	case "tcp":
		_, err := c.conn.Write(packet)
		return err
	case "websocket":
		_, err := c.connWs.Write(packet)
		return err
	case "quic":
		_, err := c.stream.Write(packet)
		return err
	default:
		return fmt.Errorf("unknown connection type: %s", c.connType)
	}
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "connection reset") ||
		strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(err.Error(), "i/o timeout")
}

func (c *MQTTClient) handleSubscribe() error {
	s := c.server

	remainingBytes, remainingLength := c.remainingBytes, c.remainingLength

	// Read variable header (packet identifier)
	packetIdentifier := binary.BigEndian.Uint16(remainingBytes[0:2])
	offset := 2

	// MQTT 5.0: Read properties if present
	if c.protocolLevel == 5 {
		_, propertiesLength, bytesRead, err := c.parseProperties(remainingBytes, offset)
		if err != nil {
			s.logger.Printf("Error reading properties: %v", err)
			return err
		}
		if err != nil {
			s.logger.Printf("Error reading properties: %v", err)
			return err
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
	var topicFilter string
	var topicFilterArr []string
	for index < len(payload) {
		// Extract topic filter length
		topicFilterLength := int(payload[index])<<8 + int(payload[index+1])
		index += 2

		// Validate topic filter length
		if topicFilterLength <= 0 || index+topicFilterLength > len(payload) {
			return fmt.Errorf("invalid topic filter length")
		}

		// Extract topic filter
		topicFilter = string(payload[index : index+topicFilterLength])
		topicFilterArr = append(topicFilterArr, topicFilter)
		index += topicFilterLength

		// Extract QoS level
		if index >= len(payload) {
			return fmt.Errorf("missing QoS level")
		}
		qosLevel = payload[index]
		qosArray = append(qosArray, qosLevel)
		index++

		qosLevels = append(qosLevels, qosLevel) // Collect all QoS levels

		topic := topicType{name: topicFilter, isSystem: c.isSystemTopic(topicFilter)}

		if topic.isSystem {
			allowed := false
			for _, allowedTopic := range s.allowedSystemTopics {
				if topicFilter == allowedTopic || strings.HasPrefix(allowedTopic, topicFilter+"/#") {
					allowed = true
					break
				}
			}
			if !allowed {
				fmt.Println("subscription to system topic not allowed", topicFilter)
				return fmt.Errorf("subscription to system topic %s not allowed", topicFilter)
			}
		}

		// Store the subscription
		c.storeSubscription(topicFilter)
		c.addSubscriber(topicFilter, int(qosLevel))
		topicFilterCount += 1
	}
	c.qosLevel = qosLevel
	// Send SUBACK packet acknowledging the subscription
	c.sendSuback(packetIdentifier, topicFilterCount, qosArray)
	c.sendRetainedMsgToSubscriber(topicFilterArr)
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
		s.logger.Printf("Error sending packet: %v", err.Error())
		return err
	}

	if err != nil {
		s.logger.Printf("Error sending SUBACK packet: %v", err)
		return err
	}
	return nil
}

func (c *MQTTClient) handlePublish(pkt Packet) error {
	s := c.server

	remainingBytes, remainingLength := c.remainingBytes, c.remainingLength
	remainingBytesWithHeader := c.hdrStart
	header := c.hdrStart[0]
	qosLevel := int((header & 0x06) >> 1)

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
		_, propertiesLength, bytesRead, err := c.parseProperties(remainingBytes, offset)
		if err != nil {
			return fmt.Errorf("failed to read properties: %v", err)
		}
		offset += bytesRead
		offset += propertiesLength
	}

	pktBuf := remainingBytesWithHeader
	c.wg.Add(1)

	subscriberQoS := c.getSubscriberQoS(topicName)
	effectiveQoS := minInt(qosLevel, subscriberQoS)
	qos := byte(effectiveQoS)
	pktBuf[0] &= 0xF9
	pktBuf[0] |= (byte(qos) << 1)
	go func(topic string, buf []byte, remLength int, p Packet) {
		defer c.wg.Done()
		packets := []Packet{p}
		c.publishToSubscribers(topic, buf, remLength+2, packets)
	}(topicName, pktBuf, remainingLength, pkt)

	// Send response based on QoS level
	switch qosLevel {
	case 1:
		// Send PUBACK
		err := c.sendPuback(packetIdentifier)
		if err != nil {
			s.logger.Printf("Error sending PUBACK: %v", err)
			return err
		}
	case 2:
		// Send PUBREC
		err := c.sendPubrec(packetIdentifier)
		if err != nil {
			s.logger.Printf("Error sending PUBREC: %v", err)
			return err
		}
	}

	c.wg.Wait()
	c.subPktsSent += 1

	return nil
}

func (c *MQTTClient) handlePublishBulk(packets []Packet, data []byte, dataSize int, topic string, allSameQoS bool) error {
	var err error
	var wg sync.WaitGroup
	s := c.server

	wg.Add(1)
	go func() {
		defer wg.Done()
		topicNameLength := len(topic)
		offset := int(2 + topicNameLength)
		count := 0
		cmdOffset := 0
		for _, pkt := range packets {
			qosLevel := int(pkt.header&0x06) >> 1
			if count != 0 && (count%128 == 0 || (cmdOffset > maxAckBufSize-32)) {
				err = c.sendPacket(c.cmdBuf, cmdOffset)
				if err != nil {
					s.logger.Printf("Error sending packet: %v", err.Error())
					return
				}
				cmdOffset = 0
				count = 0
			}
			if qosLevel > 0 {
				var packetIdentifier uint16
				packetIdentifier = binary.BigEndian.Uint16(pkt.remainingBytes[offset : offset+2])
				switch qosLevel {
				case 1:
					// Send PUBACK
					cmdOffset, err = c.buildPuback(packetIdentifier, cmdOffset)
					if err != nil {
						s.logger.Printf("Error sending PUBACK: %v", err)
						return
					}
				case 2:
					// Send PUBREC
					cmdOffset, err = c.buildPubrec(packetIdentifier, cmdOffset)
					if err != nil {
						s.logger.Printf("Error sending PUBREC: %v", err)
						return
					}
				}
			}
			count++
		}
		if count > 0 {
			err = c.sendPacket(c.cmdBuf, cmdOffset)
			if err != nil {
				s.logger.Printf("Error sending packet: %v", err.Error())
				return
			}
		}
	}()

	index := 0
	n := 0

	pktArr := make([]Packet, 0)
	for i, pkt := range packets {
		pktArr = append(pktArr, pkt)
		if pkt.qos > 0 {
			qosLevel := int(pkt.qos)
			subscriberQoS := c.getSubscriberQoS(topic)
			effectiveQoS := minInt(qosLevel, subscriberQoS)
			pkt.qos = byte(effectiveQoS)
			pkt.hdrStart[0] &= 0xF9
			pkt.hdrStart[0] |= byte(pkt.qos) << 1
		}
		if i != 0 && i%BatchSize == 0 {
			n += (1 + pkt.remainingLengthSize + pkt.remainingLength)
			c.publishToSubscribers(topic, data[index:], n, pktArr)
			pktArr = make([]Packet, 0)
			index += n
			n = 0
		} else {
			n += (1 + pkt.remainingLengthSize + pkt.remainingLength)
		}
	}
	if n != 0 {
		c.publishToSubscribers(topic, data[index:], n, pktArr)
	}
	c.subPktsSent += len(packets)

	wg.Wait()
	return nil
}

func (c *MQTTClient) handlePubrel() error {
	s := c.server
	remainingBytes := c.remainingBytes

	// Read packet identifier
	packetIdentifier := binary.BigEndian.Uint16(remainingBytes[0:2])
	if s.trace {
		s.logger.Printf("Received PUBREL for Packet Identifier: %v %v", packetIdentifier, c.clientID)
	}

	// Send PUBCOMP
	err := c.sendPubcomp(packetIdentifier)
	if err != nil {
		s.logger.Printf("Error reading PUBCOMP: %v", err)
		return err
	}
	if s.trace {
		s.logger.Printf("Received PUBREL from Client: %v", c.clientID)
	}

	return nil
}

func (c *MQTTClient) sendPuback(packetIdentifier uint16) error {
	var err error
	var packet = c.cmdBuf

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
		s.logger.Printf("Error sending packet: %v", err.Error())
		return err
	}
	if s.trace {
		s.logger.Printf("Sent PUBACK to Client: %v", c.clientID)
	}
	return nil
}

func (c *MQTTClient) buildPuback(packetIdentifier uint16, offset int) (int, error) {
	var packet = c.cmdBuf[offset:]

	// PUBACK fixed header
	var fixedHeader byte = PUBACK
	remainingLength := 2
	if c.protocolLevel == 5 {
		remainingLength = 4
	}

	// PUBACK variable header (packet identifier)
	packetIdentifierBytes := packet[offset+2 : offset+4]
	binary.BigEndian.PutUint16(packetIdentifierBytes, packetIdentifier)

	// Prepare PUBACK packet
	packet[offset+0] = fixedHeader
	packet[offset+1] = byte(remainingLength)
	if c.protocolLevel == 5 {
		packet[offset+4] = byte(0)
		packet[offset+5] = byte(0)
		offset += 6
	} else {
		offset += 4
	}

	return offset, nil
}

func (c *MQTTClient) buildPubrec(packetIdentifier uint16, offset int) (int, error) {
	var packet = c.cmdBuf[offset:]

	// PUBACK fixed header
	var fixedHeader byte = PUBREC
	remainingLength := 2
	if c.protocolLevel == 5 {
		remainingLength = 4
	}

	// PUBACK variable header (packet identifier)
	packetIdentifierBytes := packet[offset+2 : offset+4]
	binary.BigEndian.PutUint16(packetIdentifierBytes, packetIdentifier)

	// Prepare PUBACK packet
	packet[offset+0] = fixedHeader
	packet[offset+1] = byte(remainingLength)
	if c.protocolLevel == 5 {
		packet[offset+4] = byte(0)
		packet[offset+5] = byte(0)
		offset += 6
	} else {
		offset += 4
	}

	return offset, nil
}

func (c *MQTTClient) sendPubrec(packetIdentifier uint16) error {
	var err error
	var packet = c.cmdBuf

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
		s.logger.Printf("Error sending packet: %v", err.Error())
		return err
	}
	if s.trace {
		s.logger.Printf("Sent PUBREC to Client: %v", c.clientID)
	}
	c.cmdType = PUBREL
	return nil
}

func (c *MQTTClient) sendPubcomp(packetIdentifier uint16) error {
	var err error
	var packet = c.cmdBuf
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
		s.logger.Printf("Error sending packet: %v", err.Error())
		return err
	}
	return nil
}

func (c *MQTTClient) handleUnsubscribe() error {
	s := c.server
	remainingBytes, remainingLength := c.remainingBytes, c.remainingLength

	offset := 0
	packetIdentifier := binary.BigEndian.Uint16(remainingBytes[offset : offset+2])
	offset += 2

	if c.protocolLevel == 5 {
		//propertyLength := remainingBytes[2]
		_, propertiesLength, bytesRead, err := c.parseProperties(remainingBytes, offset)
		if err != nil {
			return err
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

		if s.trace {
			s.logger.Printf("Received UNSUBSCRIBE for topic filter '%s'", topicFilter)
		}

		// Handle UNSUBSCRIBE (unsubscribe the client from the topic)
		c.removeSubscription(topicFilter)
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
		s.logger.Printf("Error sending packet: %v", err.Error())
		return err
	}
	return nil
}

func (c *MQTTClient) handlePuback() error {
	s := c.server
	remainingBytes := c.remainingBytes

	packetIdentifier := binary.BigEndian.Uint16(remainingBytes[0:2])

	// Handle the PUBACK (e.g., remove the message from the pending list)
	c.acknowledgePacket(packetIdentifier)

	if s.trace {
		s.logger.Printf("Received PUBACK for packet identifier %v", packetIdentifier)
	}

	return nil
}

func (c *MQTTClient) handlePubrec() error {
	s := c.server
	remainingBytes := c.remainingBytes

	packetIdentifier := binary.BigEndian.Uint16(remainingBytes[0:2])

	// Respond with PUBREL
	err := c.sendPubrel(packetIdentifier)
	if err != nil {
		s.logger.Printf("Error sending PUBREL: %v", err)
		return err
	}

	if s.trace {
		s.logger.Printf("Received PUBREC for packet identifier %v from Client: %v ", packetIdentifier, c.clientID)
	}

	return nil
}

func (c *MQTTClient) sendPubrel(packetIdentifier uint16) error {
	var err error
	var packet = c.cmdBuf

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
		s.logger.Printf("Error sending packet: %v", err.Error())
		return err
	}
	if s.trace {
		s.logger.Printf("Sent PUBREL to Client: %v", c.clientID)
	}
	return nil
}

func (c *MQTTClient) handlePubcomp() error {
	s := c.server
	remainingBytes := c.remainingBytes
	packetIdentifier := binary.BigEndian.Uint16(remainingBytes)

	// Handle the PUBCOMP (e.g., remove the message from the pending acknowledgment list)
	c.acknowledgePacket(packetIdentifier)

	if s.trace {
		s.logger.Printf("Received PUBCOMP from Client: %v", c.clientID)
	}

	return nil
}

// publishToSubscribers publishes a message to all subscribers of a topic
func (c *MQTTClient) publishToSubscribers(topicFilter string, sndBuf []byte, pktSize int, packets []Packet) {
	var pktSave Packet
	var clients map[*MQTTClient]bool

	s := c.server

	// Find all clients subscribed to the topic using the Trie
	s.mu.Lock()
	clients = s.subscriptions.findMatchingClients(topicFilter)
	s.mu.Unlock()

	serverMutex.Lock()
	subscribeSent += len(packets)
	serverMutex.Unlock()

	for client, _ := range clients {
		client.sendPublish(sndBuf, pktSize)
	}

	for _, pkt := range packets {
		if pkt.qos > 0 {
			pktSave = pkt
		}
		for client, _ := range clients {
			if pkt.qos > 0 {
				client.queuePacket(pktSave)
			}
		}
	}
}

// StoreSubscription stores a subscription for a topic filter
func (c *MQTTClient) storeSubscription(topicFilter string) {
	s := c.server
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions.insert(topicFilter, c)
	if s.trace {
		s.logger.Printf("Stored subscription for topic filter %v", topicFilter)
	}
}

// RemoveSubscription removes a subscription for a topic filter
func (c *MQTTClient) removeSubscription(topicFilter string) {
	s := c.server
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions.remove(topicFilter, c)
}

// RemoveSubscription removes subscription for client in all topic filters
func (c *MQTTClient) removeSubscriptionClientAllFilters() {
	s := c.server
	s.subscriptions.RemoveAll(c)
}

// sendPublish sends a PUBLISH packet to the client
func (c *MQTTClient) sendPublish(packet []byte, pktSize int) error {
	var err error
	// Send the PUBLISH packet to the client
	err = c.sendPacket(packet, pktSize)
	if err != nil {
		return err
	}
	return nil
}

// isSystemTopic checks if a topic is a system topic
func (c *MQTTClient) isSystemTopic(topicName string) bool {
	return strings.HasPrefix(topicName, "$")
}

// PublishSystemStats publishes system information to $SYS topics
func (c *MQTTClient) publishSystemStats() {
	s := c.server
	for {
		// Update system topics
		clientCount := s.clientCount
		uptime := time.Since(s.startTime).String()

		systemTopics := map[string]string{
			"$SYS/broker/clients/connected": fmt.Sprintf("%d", clientCount),
			"$SYS/broker/uptime":            uptime,
		}

		// Add system load if available
		if load, err := cpu.Percent(time.Second, false); err == nil {
			systemTopics["$SYS/broker/load"] = fmt.Sprintf("%.2f", load[0])
		}

		// Publish each system topic
		for topic, value := range systemTopics {
			s.mu.Lock()
			clients := s.subscriptions.findMatchingClients(topic)
			s.mu.Unlock()
			for client, _ := range clients {
				client.sendPublish([]byte(value), len([]byte(value)))
			}
		}

		s.logger.Printf("System stats updated: %v", systemTopics)

		time.Sleep(60 * time.Second) // Update every minute
	}
}
