package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	broker := "ssl://localhost:1884"
	clientID := "testClient"
	topic := "test/topic"

	// Load client cert
	cert, err := tls.LoadX509KeyPair("client.crt", "client.key")
	if err != nil {
		log.Fatalf("Failed to load client certificate: %v", err)
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile("ca.crt")
	if err != nil {
		log.Fatalf("Failed to load CA certificate: %v", err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		log.Fatalf("Failed to append CA certificate")
	}

	// Configure TLS
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(clientID)
	opts.SetTLSConfig(tlsConfig)
	opts.SetDefaultPublishHandler(messageHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to connect to broker: %v", token.Error())
	}

	// Publish a message
	token := client.Publish(topic, 0, false, "Hello MQTT over SSL")
	token.Wait()

	// Subscribe to a topic
	if token := client.Subscribe(topic, 0, nil); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to subscribe to topic: %v", token.Error())
	}

	// Keep the connection alive
	time.Sleep(30 * time.Second)
	client.Disconnect(250)
}

var messageHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected to MQTT broker")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connection lost: %v", err)
}

