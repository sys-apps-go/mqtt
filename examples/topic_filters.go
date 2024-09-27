package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var wg sync.WaitGroup

func main() {
	broker := flag.String("b", "localhost:1883", "Broker address")
	versionMQTT := flag.Int("v", 5, "MQTT Version")
	flag.Parse()
	version := *versionMQTT
	// MQTT broker address
	topicSub := "environment/sensors/#"
	wg.Add(1)
	go subscribe(topicSub, *broker, version)
	time.Sleep(time.Second)

	// Publish function
	topicPub := "environment/sensors/path1/path2/path3/path4/path5"
	for i := 0; i < 5; i++ {
		wg.Add(1)
		switch i {
		case 0:
			topicPub = "environment/sensors/path1"
		case 1:
			topicPub = "environment/sensors/path1/path2"
		case 2:
			topicPub = "environment/sensors/path1/path2/path3"
		case 3:
			topicPub = "environment/sensors/path1/path2/path3/path4"
		case 4:
			topicPub = "environment/sensors/path1/path2/path3/path4/path5"
		}
		go publish(i, topicPub, *broker, version)
	}

	wg.Wait()
	fmt.Println("Disconnected.")
}

func publish(threadID int, topic, broker string, version int) {
	defer wg.Done()
	// Create a client options
	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetProtocolVersion(uint(version)) // Set the protocol version to 4 (3.1.1)
	opts.SetClientID(fmt.Sprintf("%v.%v", "mqtt_client", threadID))
	// Create the MQTT client
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Println("Error connecting to MQTT broker:", token.Error())
		return
	}
	defer client.Disconnect(250)
	for {
		// Simulate temperature and humidity data
		temperature := rand.Float64() * 50 // Random temperature between 0 and 50
		humidity := rand.Float64() * 100   // Random humidity between 0 and 100

		// Format the payload
		payload := fmt.Sprintf(`{"id": %v, "temperature": %.2f, "humidity": %.2f}`, threadID, temperature, humidity)

		// Check if the client is connected before publishing
		if !client.IsConnected() {
			if token := client.Connect(); token.Wait() && token.Error() != nil {
				fmt.Println("Error reconnecting to MQTT broker:", token.Error())
				return
			}
		}

		// Publish message
		token := client.Publish(topic, 1, false, payload)
		token.Wait() // Wait for the publishing operation to complete
		if token.Error() != nil {
			fmt.Println("Error publishing message:", token.Error())
		} else {
		}
		//time.Sleep(time.Second * 1)

		// Wait for a few seconds before publishing the next message
	}

}

func subscribe(topic, broker string, version int) {
	defer wg.Done()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetProtocolVersion(uint(version)) // Set the protocol version to 4 (3.1.1)
	opts.SetClientID(fmt.Sprintf("%v.%v", "mqtt_client", "sub"))
	// Create the MQTT client
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Println("Error connecting to MQTT broker:", token.Error())
		return
	}
	defer client.Disconnect(250)
	token := client.Subscribe(topic, 1, func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Topic: %v, Message: %v\n", msg.Topic(), string(msg.Payload()))
	})
	if token.Wait() && token.Error() != nil {
		fmt.Println("Error subscribing to topic:", token.Error())
		return
	}
	select {}
}
