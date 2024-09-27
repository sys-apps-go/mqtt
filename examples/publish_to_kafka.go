package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"
	"context"
	"log"
	"strings"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var wg sync.WaitGroup

var kafkaBroker []string
var topicMap map[string]bool
func main() {
	broker := flag.String("b", "localhost:1883", "Broker address")
	kBroker := flag.String("k", "localhost:9092", "Main broker address of Kafka")
	versionMQTT := flag.Int("v", 5, "MQTT Version")
	flag.Parse()
	kafkaBroker = []string{*kBroker}
	topicMap = make(map[string]bool)

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
		token := client.Publish(topic, 2, false, payload)
		token.Wait() // Wait for the publishing operation to complete
		if token.Error() != nil {
			fmt.Println("Error publishing message:", token.Error())
		} else {
		}
		//time.Sleep(time.Millisecond * 100)

		// Wait for a few seconds before publishing the next message
	}

}

type msgData struct {
	topic string
	msg   string
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

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker[0]})
	if err != nil {
		log.Fatalf("Failed to create kafka producer: %v", err)
	}
	defer producer.Close()

	kafkaChannel := make(chan msgData)
	for i := 0; i < 32; i++ {
		go writeToKafka(kafkaChannel, topic, producer)
	}
	token := client.Subscribe(topic, 1, func(client mqtt.Client, msg mqtt.Message) {
		topic1 := strings.Replace(msg.Topic(), "/", "_", -1)
		_, exists := topicMap[topic1]
		if !exists {
			createTopic(kafkaBroker, topic1)
			topicMap[topic1] = true
		}
		message := msg.Payload()
		m := msgData{}
		m.msg = string(message)
		m.topic = topic1
		kafkaChannel <- m
	})
	if token.Wait() && token.Error() != nil {
		fmt.Println("Error subscribing to topic:", token.Error())
		return
	}
	select {}
}

func writeToKafka(kchan chan msgData, topic string, producer *kafka.Producer) {
	for m := range kchan {
		err := writeMessage(producer, m.topic, m.msg)
		if err != nil {
			log.Fatalf("Error writing messages: %v", err)
		}
	}
}

func createTopic(brokers []string, topic string) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": brokers[0]})
	if err != nil {
		return fmt.Errorf("failed to create kafka admin client: %w", err)
	}
	defer adminClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results, err := adminClient.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1}},
		kafka.SetAdminOperationTimeout(10*time.Second))

	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			return fmt.Errorf("failed to create topic: %v", result.Error)
		}
	}

	return nil
}

func writeMessage(producer *kafka.Producer, topic string, message string) error {
	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}
	return nil
}

func topicExists(brokers []string, topic string) (bool, error) {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": brokers[0]})
	if err != nil {
		return false, fmt.Errorf("failed to create kafka admin client: %w", err)
	}
	defer adminClient.Close()

	metadata, err := adminClient.GetMetadata(&topic, false, 10000)
	if err != nil {
		return false, fmt.Errorf("failed to get metadata: %w", err)
	}

	for _, t := range metadata.Topics {
		if t.Topic == topic {
			return true, nil
		}
	}

	return false, nil
}
