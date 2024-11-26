package main

import (
    "fmt"
    "log"
    "sync"
    "time"

    mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
    numClients = 10
    brokerURL  = "tcp://localhost:1883"
    clientID   = "go-mqtt-client-"
    topic      = "test/topic"
)

func createMQTTClient(id int, wg *sync.WaitGroup) {
    defer wg.Done()

    opts := mqtt.NewClientOptions().
        AddBroker(brokerURL).
        SetClientID(fmt.Sprintf("%s%d", clientID, id)).
        SetCleanSession(true).
        SetAutoReconnect(true).
        SetConnectionLostHandler(func(client mqtt.Client, err error) {
            log.Printf("Client %d lost connection: %v\n", id, err)
        }).
        SetOnConnectHandler(func(client mqtt.Client) {
            log.Printf("Client %d connected\n", id)
        })

    client := mqtt.NewClient(opts)
    if token := client.Connect(); token.Wait() && token.Error() != nil {
        log.Printf("Client %d failed to connect: %v\n", id, token.Error())
        return
    }

    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            message := fmt.Sprintf("Message from client %d at %s", id, time.Now().Format(time.RFC3339))
            token := client.Publish(topic, 0, false, message)
            token.Wait()
            if token.Error() != nil {
                log.Printf("Client %d failed to publish: %v\n", id, token.Error())
            } else {
                log.Printf("Client %d published message: %s\n", id, message)
            }
        }
    }
}

func main() {
    var wg sync.WaitGroup

    for i := 0; i < numClients; i++ {
        wg.Add(1)
        go createMQTTClient(i, &wg)
    }

    wg.Wait()
    log.Println("All clients have been shut down")
}

