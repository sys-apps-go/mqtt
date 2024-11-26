package main

import (
	"bytes"
	"fmt"
	"github.com/gorilla/websocket"
	"io"
	"net"
	"time"
)

func (c *MQTTClient) readPacketWs() (int, error) {
	err := c.connWs.SetReadDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		return 0, fmt.Errorf("failed to set read deadline: %v", err)
	}

	packet, err := c.connWs.Read()
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return 0, fmt.Errorf("client connection timed out")
		}
		if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
			return 0, fmt.Errorf("client disconnected unexpectedly: %v", err)
		}
		return 0, err
	}

	if len(packet) < 2 {
		return 0, fmt.Errorf("invalid packet size: %v", len(packet))
	}

	copy(c.packetBuf, packet)
	return len(packet), nil
}

func ReadMQTTPacket(wsc *websocketConn) ([]byte, error) {
	buffer := bytes.Buffer{}

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

// Handle WebSocket connection
func handleWSClientCmds(wsConn *websocket.Conn, s *MQTTServer) {

	// Create a bridge between WebSocket and net.Conn
	conn := &websocketConn{wsConn}

	// Create an MQTT client and handle the connection
	client := NewMQTTClientWs(conn, s)
	client.connWs = conn
	client.connType = "websocket"
	if s.useSyncPool {
		client.packetBuf = s.packetBufPool.Get().([]byte)[:DefaultPacketSize]
	} else {
		client.packetBuf = make([]byte, DefaultPacketSize)
	}
	go client.handleClientCmds()
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

func (c *MQTTClient) processMQTTPacketsWs() ([]Packet, []byte, int, bool, bool, string, error) {
	packetBuf := c.packetBuf
	n, err := c.readPacketWs()
	if err != nil {
		return nil, nil, 0, false, false, "", fmt.Errorf("error reading websocket: %v", err)
	}

	packetBuf = packetBuf[:n]
	var packets []Packet
	index := 0

	hdrStart := packetBuf[index:]
	cmd := packetBuf[index]
	index++

	// Parse remaining length
	remainingLength := 0
	multiplier := 1
	remainingLengthSize := 0
	for i := 0; i < 4; i++ {
		if index >= n {
			break // Not enough data to parse remaining length
		}

		encodedByte := packetBuf[index]
		index++

		remainingLength += int(encodedByte&127) * multiplier
		remainingLengthSize++
		multiplier *= 128

		if encodedByte&128 == 0 {
			break
		}
	}
	packets = append(packets, Packet{
		cmd:                 cmd,
		remainingLengthSize: remainingLengthSize,
		remainingLength:     remainingLength,
		payloadStart:        index,
		remainingBytes:      packetBuf[index : index+remainingLength],
		hdrStart:            hdrStart,
	})
	index += remainingLength
	return packets, packetBuf[:index], n, false, false, "", nil
}
