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
var exitCleanup bool

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

func main() {
	broker := flag.String("b", "localhost:1883", "Broker address")
	versionMQTT := flag.Int("v", 4, "MQTT Version")
	numClients := flag.Int("c", 100, "Number of Pub/Sub")
	flag.Parse()
	version := *versionMQTT

	*numClients = *numClients + 2
	clientsPub = make([]clientInfo, *numClients)
	clientsSub = make([]clientInfo, *numClients)

	for i := 0; i < *numClients; i++ {
		clientsPub[i].name = fmt.Sprintf("client.publish.%v", i+1)
		clientsSub[i].name = fmt.Sprintf("client.subscribe.%v", i+1)
	}

	for {
		exitCleanup = false
		wg = sync.WaitGroup{}
		
		// Start 5 subscribers
		for i := 0; i < *numClients-2; i++ {
			wg.Add(1)
			go subscribe(i+1, fmt.Sprintf("environment/sensors/path%d", i+1), *broker, version)
		}

		// Start 5 publishers
		for i := 0; i < *numClients-2; i++ {
			wg.Add(1)
			go publish(i+1, fmt.Sprintf("environment/sensors/path%d", i+1), *broker, version)
		}

		wg.Add(1)
		go subscribe(*numClients-1, "environment/sensors/#", *broker, version)

		wg.Add(1)
		go subscribe(*numClients, "$SYS/broker/uptime", *broker, version)

		// Wait for 5 minutes
		time.Sleep(60 * time.Second)

		// Signal all goroutines to exit
		exitCleanup = true

		// Wait for all goroutines to finish
		wg.Wait()

		fmt.Println("Restarting clients...")
	}
}

func publish(threadID int, topic, broker string, version int) {
	defer wg.Done()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetProtocolVersion(uint(version))
	opts.SetClientID(clientsPub[threadID-1].name)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Println("Error connecting to MQTT broker:", token.Error())
		return
	}
	defer client.Disconnect(250)

	for !exitCleanup {
		temperature := rand.Float64() * 50
		humidity := rand.Float64() * 100

		payload := fmt.Sprintf(`{"id": %v, "temperature": %.2f, "humidity": %.2f}`, threadID, temperature, humidity)

		if !client.IsConnected() {
			if token := client.Connect(); token.Wait() && token.Error() != nil {
				fmt.Println("Error reconnecting to MQTT broker:", token.Error())
				return
			}
		}

		token := client.Publish(topic, 0, false, payload)
		token.Wait()
		if token.Error() != nil {
			fmt.Println("Error publishing message:", token.Error())
		}

	}
}

func subscribe(threadID int, topic, broker string, version int) {
	defer wg.Done()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetProtocolVersion(uint(version))
	opts.SetClientID(clientsSub[threadID-1].name)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Println("Error connecting to MQTT broker:", token.Error())
		return
	}
	defer client.Disconnect(250)

	token := client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Topic: %v, Message: %v\n", msg.Topic(), string(msg.Payload()))
	})
	if token.Wait() && token.Error() != nil {
		fmt.Println("Error subscribing to topic:", token.Error())
		return
	}

	for !exitCleanup {
		time.Sleep(time.Second)
	}
}
