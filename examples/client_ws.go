package main

import (
	"fmt"
//	"log"
//	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var wg sync.WaitGroup
var numMessages = 100

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connect lost: %v\n", err)
}

func main() {
	//mqtt.DEBUG = log.New(os.Stdout, "", 0)
	//mqtt.ERROR = log.New(os.Stdout, "", 0)
	opts := mqtt.NewClientOptions()
	opts.AddBroker("ws://localhost:1885/mqtt-ws") // Use WebSocket URL
	opts.SetProtocolVersion(uint(4))      // Set the protocol version to 4 (3.1.1)
	opts.SetClientID("go_mqtt_ws_client")
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	topic := "test/topic"
	wg.Add(1)
	go subscribe(topic, "ws://localhost:1885/mqtt-ws", 4)
	time.Sleep(time.Second)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	for i := 0; i < numMessages; i++ {
		text := fmt.Sprintf("Message %d", i)
		token := client.Publish(topic, 0, false, text)
		token.Wait()
		fmt.Printf("Published message: %s\n", text)
	}

	wg.Wait()
	client.Disconnect(250)
}

func subscribe(topic, broker string, version int) {
	defer wg.Done()
	opts := mqtt.NewClientOptions()
	opts.AddBroker("ws://localhost:1885/mqtt-ws") // Use WebSocket URL
	opts.SetProtocolVersion(uint(4))      // Set the protocol version to 4 (3.1.1)
	opts.SetClientID("go_mqtt_ws_client")
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	// Create the MQTT client
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Println("Error connecting to MQTT broker:", token.Error())
		return
	}
	defer client.Disconnect(250)
	count := 0
	token := client.Subscribe(topic, 1, func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Topic: %v, Message: %v\n", msg.Topic(), string(msg.Payload()))
		count++
		if count >= numMessages {
			return
		}
	})
	if token.Wait() && token.Error() != nil {
		fmt.Println("Error subscribing to topic:", token.Error())
		return
	}
	select {}
}
