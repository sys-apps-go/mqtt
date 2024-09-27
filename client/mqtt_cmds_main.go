package main

import (
	"flag"
	"fmt"
	"sync"
	"time"
)

var wg sync.WaitGroup
var startSubscription, startPublish time.Time
var elapsedTimeSubscription, elapsedTimePublish float64
var pubClients, subClients int
var numClients, numMessages *int

func main() {
	broker := flag.String("b", "localhost:1883", "Broker address")
	versionMQTT := flag.Int("v", 4, "MQTT Version")
	subQoS := flag.Int("s", 2, "Client QoS")
	pubQoS := flag.Int("p", 2, "Server QoS")
	numMessages = flag.Int("m", 10000, "Number of messages")
	numClients = flag.Int("c", 100, "Number of publishers/subscribers")
	flag.Parse()

	// Example configuration
	config := ConfigParameters{
		Broker:                 *broker,
		ProtocolLevel:          byte(*versionMQTT),
		KeepAlive:              60,
		CleanStart:             true,
		Username:               "user",
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

	// Create clients and run publishers and subscribers
	wg.Add(*numClients * 2)
	startSubscription = time.Now()
	var pCount, sCount int
	for i := 0; i < *numClients; i++ {
		// Subscriber
		go func(clientID int, subCount int) {
			defer wg.Done()

			clientSub := NewClient(fmt.Sprintf("Subscriber-Client-%d", clientID), uint16(100))
			err := clientSub.Connect(config)
			if err != nil {
				fmt.Println("Failed to connect subscriber:", err)
				return
			}

			topic := fmt.Sprintf("example/topic-%d", clientID)
			defer clientSub.updateElapsedTimeSubscription(topic)
			err = clientSub.Subscribe(topic, byte(*subQoS), func(topic, msg string) {
				subCount++
			})
			if err != nil {
				fmt.Println("Error subscribing:", err)
				return
			}

			for subCount < *numMessages {
				time.Sleep(100 * time.Millisecond) // Wait for messages
			}
			defer clientSub.Disconnect()

		}(i, sCount)
	}
	time.Sleep(time.Millisecond * 250)

	startPublish = time.Now()
	for i := 0; i < *numClients; i++ {
		// Publisher
		go func(clientID int, pubCount int) {
			defer wg.Done()
			defer updateElapsedTimePublish()
			clientPub := NewClient(fmt.Sprintf("Publisher-Client-%d", clientID), uint16(100))
			err := clientPub.Connect(config)
			if err != nil {
				fmt.Println("Failed to connect publisher:", err)
				return
			}
			defer clientPub.Disconnect()

			topic := fmt.Sprintf("example/topic-%d", clientID)
			for pubCount < *numMessages {
				err = clientPub.Publish(topic, fmt.Sprintf("Message %d", pubCount), byte(*pubQoS))
				if err != nil {
					fmt.Println("Error publishing:", err)
					return
				}
				pubCount++
				if pubCount % 1000 == 0 {
					fmt.Printf(".")
				}
				//time.Sleep(time.Millisecond)
			}
		}(i, pCount)

	}

	wg.Wait()

	if elapsedTimeSubscription == float64(0) && elapsedTimePublish > float64(0) {
		elapsedTimeSubscription = elapsedTimePublish
	}
	// Print global statistics
	totalMsgCount := (*numClients) * (*numMessages)
	fmt.Printf("\nTotal Published Messages: %d, Total Time: %.2f seconds, Overall Publish Rate: %.2f messages/second\n",
		totalMsgCount, elapsedTimePublish, float64(totalMsgCount)/elapsedTimePublish)
	fmt.Printf("Total Subscribed Messages: %d, Total Time: %.2f seconds, Overall Subscription Rate: %.2f messages/second\n",
		totalMsgCount, elapsedTimeSubscription, float64(totalMsgCount)/elapsedTimeSubscription)
}

func (c *Client) updateElapsedTimeSubscription(topic string) {
	subClients++
	if subClients >= *numClients {
		c.Unsubscribe(topic)
		elapsedTimeSubscription = float64(time.Since(startSubscription).Milliseconds()) / float64(1000)
	}
}

func updateElapsedTimePublish() {
	pubClients++
	if pubClients >= *numClients {
		elapsedTimePublish = float64(time.Since(startPublish).Milliseconds()) / float64(1000)
	}
}
