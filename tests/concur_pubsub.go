package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

const (
	brokerURL   = "tcp://localhost:1883" // Replace with MQTT broker URL
	topic       = "test/topic"
	clientCount = 500
)

var qosLevels = []byte{0, 1, 2}

func createClient(clientID string, broker string, version int) MQTT.Client {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(clientID)
	opts.SetProtocolVersion(uint(version))          // Set the protocol version to 4 (3.1.1)
	opts.AddBroker(fmt.Sprintf("tcp://%v", broker)) // Replace with broker's address
	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		fmt.Printf("Client %s received message on topic %s: %s\n", clientID, msg.Topic(), string(msg.Payload()))
	})

	return MQTT.NewClient(opts)
}

func startPublisher(client MQTT.Client, id string, qos byte, wg *sync.WaitGroup) {
	defer wg.Done()
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Printf("Publisher %s connection error: %v\n", id, token.Error())
		return
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			payload := fmt.Sprintf("Message from Publisher %v", id)
			token := client.Publish(topic, qos, false, payload)
			token.Wait()
			if token.Error() != nil {
				fmt.Printf("Publisher %s publish error: %v\n", id, token.Error())
			}
		}
	}
}

func startSubscriber(client MQTT.Client, id string, qos byte, wg *sync.WaitGroup) {
	defer wg.Done()
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Printf("Subscriber %s connection error: %v\n", id, token.Error())
		return
	}

	token := client.Subscribe(topic, qos, nil)
	token.Wait()
	if token.Error() != nil {
		fmt.Printf("Subscriber %s subscribe error: %v\n", id, token.Error())
		return
	}
	fmt.Printf("Subscriber %s subscribed to topic %s\n", id, topic)
}

func main() {
	broker := flag.String("b", "localhost:1883", "Broker address")
	versionMQTT := flag.Int("v", 5, "MQTT Version")
	flag.Parse()
	version := *versionMQTT
	var wg sync.WaitGroup
	var clientID string

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < clientCount; i++ {
		if i%2 == 0 {
			clientID = fmt.Sprintf("client-pub-%d", i)
		} else {
			clientID = fmt.Sprintf("client-sub-%d", i)
		}
		client := createClient(clientID, *broker, version)
		qos := qosLevels[rand.Intn(len(qosLevels))]

		wg.Add(1)
		if i%2 == 0 {
			go startPublisher(client, clientID, qos, &wg)
		} else {
			go startSubscriber(client, clientID, qos, &wg)
		}
	}

	wg.Wait()
}
