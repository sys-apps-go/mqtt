package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	numPublishers  = 11 // Number of paths in the map
	numSubscribers = 11 // Number of paths in the map
)

// Paths to simulate MQTT topics
var paths = map[int]string{
	0:  "/root",
	1:  "/root/path1",
	2:  "/root/path1/path2",
	3:  "/root/path1/path2/path3",
	4:  "/root/path1/path2/path3/path4",
	5:  "/root/path1/path2/path3/path4/path5",
	6:  "/root/path1/path2/path3/path4/path5/path6",
	7:  "/root/path1/path2/path3/path4/path5/path6/path7",
	8:  "/root/path1/path2/path3/path4/path5/path6/path7/path8",
	9:  "/root/path1/path2/path3/path4/path5/path6/path7/path8/path9",
	10: "/root/path1/path2/path3/path4/path5/path6/path7/path8/path9/path10",
}

func main() {
	// Set command-line arguments
	broker := flag.String("b", "tcp://localhost:1883", "Broker URI")
	versionMQTT := flag.Int("v", 5, "MQTT Version (4 for 3.1.1, 5 for 5.0)")
	flag.Parse()

	// Initialize a wait group for publishers and subscribers
	var wg sync.WaitGroup

	// Start subscribers (each subscriber is a separate MQTT client)
	for _, path := range paths {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			// Create a new client for each subscriber
			client := createMQTTClient(*broker, *versionMQTT, "sub-"+topic)
			subscribeToTopic(client, topic)
		}(path)
	}

	// Small delay to ensure subscribers are ready
	time.Sleep(time.Second)

	// Start publishers (each publisher is a separate MQTT client)
	for _, path := range paths {
		payload := fmt.Sprintf("Message from publisher at %s", path)
		wg.Add(1)
		go func(topic, payload string) {
			defer wg.Done()
			// Create a new client for each publisher
			client := createMQTTClient(*broker, *versionMQTT, "pub-"+topic)
			publishToTopic(client, topic, payload)
		}(path, payload)
	}

	// Wait for all publishers and subscribers to finish
	wg.Wait()

	fmt.Println("All publishers and subscribers completed")
}

// Function to create a new MQTT client
func createMQTTClient(broker string, versionMQTT int, clientID string) mqtt.Client {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetProtocolVersion(uint(versionMQTT))
	opts.SetClientID(clientID)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Error connecting to MQTT broker as client %s: %v", clientID, token.Error())
	}
	return client
}

// Function to subscribe to a specific topic
func subscribeToTopic(client mqtt.Client, topic string) {
	fmt.Printf("Subscribing to topic: %s\n", topic)
	token := client.Subscribe(topic, 1, func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received message on topic %s: %s\n", msg.Topic(), msg.Payload())
	})
	if token.Wait() && token.Error() != nil {
		fmt.Printf("Error subscribing to topic %s: %v\n", topic, token.Error())
	}
}

// Function to publish to a specific topic
func publishToTopic(client mqtt.Client, topic, payload string) {
	fmt.Printf("Publishing to topic %s: %s\n", topic, payload)
	token := client.Publish(topic, 1, false, payload)
	token.Wait()
	if token.Error() != nil {
		fmt.Printf("Failed to publish to topic %s: %v\n", topic, token.Error())
	}
}

