package main

import (
	"flag"
	"fmt"
	"sync"
	"time"
)

var wg sync.WaitGroup
var pubStartTime, subStartTime time.Time
var numClients, numMessages *int
var pubFinished, subFinished bool

type clientInfo struct {
	name        string
	subCount    int
	subAckCount int
	pubCount    int
	pubAckCount int
	wgDone      bool
}

var clientsPub []clientInfo
var clientsSub []clientInfo

var pubQoS, subQoS *int

var errorFlag bool

func main() {
	broker := flag.String("b", "localhost:1883", "Broker address")
	quicServer := flag.String("q", "localhost:4242", "QUIC Server address")
	versionMQTT := flag.Int("v", 4, "MQTT Version")
	pubQoS = flag.Int("p", 0, "Client QoS for Publish")
	subQoS = flag.Int("s", 0, "Client QoS for Subscription")
	numMessages = flag.Int("m", 25000, "Number of messages")
	numClients = flag.Int("c", 200, "Number of publishers/subscribers")
	connType := flag.String("t", "tcp", "Connection type")
	flag.Parse()

	clientsPub = make([]clientInfo, *numClients)
	clientsSub = make([]clientInfo, *numClients)

	// Example configuration
	config := ConfigParameters{
		Broker:                 *broker,
		QuicServer:             *quicServer,
		ProtocolLevel:          byte(*versionMQTT),
		ConnType:               *connType,
		KeepAlive:              60,
		CleanStart:             true,
		UserName:               "user",
		Password:               "pass",
		PayloadFormatIndicator: 1,
		MessageExpiryInterval:  3600,
		ContentType:            "text/plain",
		ResponseTopic:          "response/topic",
		CorrelationData:        []byte{0x12, 0x34, 0x56, 0x78},
		SubscriptionIdentifier: 123,
		SessionExpiryInterval:  86400,
		AssignedClientID:       "test-client",
		ServerKeepAlive:        60,
		AuthenticationMethod:   "PLAIN",
		AuthenticationData:     []byte("username:password"),
		RequestProblemInfo:     1,
		WillDelayInterval:      10,
		RequestResponseInfo:    1,
		ResponseInformation:    "OK",
		ServerReference:        "mqtt://server.example.com",
		ReasonString:           "Connection refused: bad client ID",
		ReceiveMaximum:         1024,
		TopicAliasMaximum:      10,
		TopicAlias:             5,
		MaximumQoS:             2,
		RetainAvailable:        1,
		UserProperty: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
		MaximumPacketSize:       65535,
		WildcardSubscription:    1,
		SubscriptionIDAvailable: 1,
		SharedSubscription:      1,
	}

	// Create clients and run publisher
	wg.Add(*numClients * 2)

	for i := 0; i < *numClients; i++ {
		clientsPub[i].name = fmt.Sprintf("Client.pub.%v", i+1)
		clientsSub[i].name = fmt.Sprintf("Client.sub.%v", i+1)
	}

	subStartTime = time.Now()

	for i := 0; i < *numClients; i++ {
		// Subscriber
		go func(idx int) {
			defer wg.Done()
			clientSub := NewClient(clientsSub[idx].name, uint16(100), config.ConnType)
			err := clientSub.Connect(config)
			if err != nil {
				fmt.Println("Failed to connect subscriber:", err)
				errorFlag = true
				return
			}

			topic := fmt.Sprintf("example/topic-%d", idx+1)
			err = clientSub.Subscribe(topic, byte(*subQoS), func(topic, msg string) {
				clientsSub[idx].subCount++
			})
			if err != nil {
				fmt.Println("Error subscribing:", err)
				errorFlag = true
				return
			}

			for clientsSub[idx].subCount < *numMessages {
				time.Sleep(100 * time.Millisecond) // Wait for messages
			}
			clientSub.Unsubscribe(topic, false)
			clientSub.Disconnect()
		}(i)
	}
	time.Sleep(time.Millisecond * 250)

	pubStartTime = time.Now()

	for i := 0; i < *numClients; i++ {
		// Publisher
		go func(idx int) {
			defer wg.Done()
			clientPub := NewClient(clientsPub[idx].name, uint16(100), config.ConnType)
			err := clientPub.Connect(config)
			if err != nil {
				fmt.Println("Failed to connect publisher:", err)
				errorFlag = true
				return
			}

			topic := fmt.Sprintf("example/topic-%d", idx+1)
			for clientsPub[idx].pubCount < *numMessages {
				err = clientPub.Publish(topic, fmt.Sprintf("Message %d", idx+1), byte(*pubQoS))
				if err != nil {
					fmt.Println("Error publishing:", err)
					errorFlag = true
					return
				}
				clientsPub[idx].pubCount++
				clientsPub[idx].pubAckCount++
			}
			for clientsPub[idx].pubAckCount != clientsPub[idx].pubCount {
				time.Sleep(time.Millisecond * 100)
			}
			clientPub.Disconnect()
		}(i)
	}

	// Start the stats printing goroutine
	go printStats()

	wg.Wait()
	pubFinished = true
	subFinished = true

	// Print final stats
	if !errorFlag {
		printFinalStats()
	}
}

func printStats() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastTotalPub, lastTotalSub int

	for range ticker.C {
		if pubFinished && subFinished {
			return
		}

		totalPub := 0
		totalSub := 0
		for i := 0; i < *numClients; i++ {
			totalPub += clientsPub[i].pubCount
			totalSub += clientsSub[i].subCount
		}

		pubPerSecond := int(float64(totalPub-lastTotalPub) / 1.0)
		subPerSecond := int(float64(totalSub-lastTotalSub) / 1.0)
		fmt.Printf("\rPublished: %d (%.2f/s), Subscribed: %d (%.2f/s)",
			totalPub, float64(pubPerSecond), totalSub, float64(subPerSecond))
		lastTotalPub = totalPub
		lastTotalSub = totalSub
	}
}

func printFinalStats() {
	totalPub := 0
	totalSub := 0
	for i := 0; i < *numClients; i++ {
		totalPub += clientsPub[i].pubCount
		totalSub += clientsSub[i].subCount
	}

	pubElapsedSeconds := time.Since(pubStartTime).Seconds()
	subElapsedSeconds := time.Since(subStartTime).Seconds()
	pubPerSecond := float64(totalPub) / pubElapsedSeconds
	subPerSecond := float64(totalSub) / subElapsedSeconds

	fmt.Printf("\n\nFinal Statistics:\n")
	fmt.Printf("Total Published: %d (%.2f/s)\n", totalPub, pubPerSecond)
	fmt.Printf("Total Subscribed: %d (%.2f/s)\n", totalSub, subPerSecond)
	fmt.Printf("Publish Time: %.2f seconds\n", pubElapsedSeconds)
	fmt.Printf("Subscribe Time: %.2f seconds\n", subElapsedSeconds)
	fmt.Printf("Published/Subscribed %v messages with QoS %v\n", (*numClients)*(*numMessages), *pubQoS)
}
