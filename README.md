MQTT 3.1.1 implementataion.
cd mqttserver
go build
./mqttserver &

cd tests
go run topic* // TCP
go run topic* -b ws://localhost:1885/mqtt-ws  

go run conc*

mqtt-stresser -num-clients 100 -num-messages 10000 --broker  localhost:1883  

#Configuration
Concurrent Clients: 100
Messages / Client:  1000000

#Results
Published Messages: 1000000 (100%)
Received Messages:  1000000 (100%)
Completed:          100 (100%)
Errors:             0 (0%)

#Publishing Throughput
Fastest: 59566 msg/sec
Slowest: 3118 msg/sec
Median: 7308 msg/sec

  < 8763 msg/sec  62%
  < 14408 msg/sec  81%
  < 20053 msg/sec  84%
  < 25698 msg/sec  87%
  < 31342 msg/sec  91%
  < 42632 msg/sec  96%
  < 48277 msg/sec  97%
  < 53922 msg/sec  99%
  < 65211 msg/sec  100%

#Receiving Throughput
Fastest: 146044 msg/sec
Slowest: 3224 msg/sec
Median: 15835 msg/sec

  < 17506 msg/sec  55%
  < 31788 msg/sec  68%
  < 46070 msg/sec  81%
  < 60352 msg/sec  84%
  < 74634 msg/sec  90%
  < 88916 msg/sec  94%
  < 103198 msg/sec  95%
  < 117480 msg/sec  98%
  < 131762 msg/sec  99%
  < 160326 msg/sec  100%

mqtt-stresser -num-clients 100 -num-messages 20000 --broker  localhost:1883  

#Configuration
Concurrent Clients: 100
Messages / Client:  2000000

#Results
Published Messages: 2000000 (100%)
Received Messages:  2000000 (100%)
Completed:          100 (100%)
Errors:             0 (0%)

#Publishing Throughput
Fastest: 46714 msg/sec
Slowest: 3356 msg/sec
Median: 5484 msg/sec

  < 7692 msg/sec  69%
  < 12028 msg/sec  76%
  < 16364 msg/sec  81%
  < 20699 msg/sec  84%
  < 25035 msg/sec  92%
  < 29371 msg/sec  96%
  < 33707 msg/sec  99%
  < 51050 msg/sec  100%

#Receiving Throughput
Fastest: 40112 msg/sec
Slowest: 2766 msg/sec
Median: 6722 msg/sec

  < 6500 msg/sec  44%
  < 10235 msg/sec  85%
  < 13970 msg/sec  92%
  < 17704 msg/sec  94%
  < 21439 msg/sec  96%
  < 32643 msg/sec  99%
  < 43847 msg/sec  100%


mqtt-stresser -num-clients 100 -num-messages 5000 --broker  localhost:1883  -publisher-qos 2 -subscriber-qos 2

#Configuration
Concurrent Clients: 100
Messages / Client:  500000

#Results
Published Messages: 500000 (100%)
Received Messages:  500000 (100%)
Completed:          100 (100%)
Errors:             0 (0%)


#Publishing Throughput
Fastest: 388 msg/sec
Slowest: 305 msg/sec
Median: 344 msg/sec

  < 313 msg/sec  7%
  < 322 msg/sec  25%
  < 330 msg/sec  32%
  < 338 msg/sec  42%
  < 347 msg/sec  56%
  < 355 msg/sec  65%
  < 363 msg/sec  72%
  < 372 msg/sec  81%
  < 380 msg/sec  91%
  < 388 msg/sec  99%
  < 397 msg/sec  100%

#Receiving Throughput
Fastest: 57171 msg/sec
Slowest: 9065 msg/sec
Median: 27251 msg/sec

  < 13875 msg/sec  12%
  < 18686 msg/sec  21%
  < 23497 msg/sec  35%
  < 28307 msg/sec  55%
  < 33118 msg/sec  71%
  < 37929 msg/sec  79%
  < 42739 msg/sec  91%
  < 47550 msg/sec  98%
  < 52360 msg/sec  99%
  < 61982 msg/sec  100%


mqtt-stresser -num-clients 100 -num-messages 5000 --broker  localhost:1883  -publisher-qos 1 -subscriber-qos 1

#Configuration
Concurrent Clients: 100
Messages / Client:  500000

#Results
Published Messages: 500000 (100%)
Received Messages:  500000 (100%)
Completed:          100 (100%)
Errors:             0 (0%)

#Publishing Throughput
Fastest: 662 msg/sec
Slowest: 550 msg/sec
Median: 605 msg/sec

  < 561 msg/sec  10%
  < 572 msg/sec  20%
  < 583 msg/sec  31%
  < 595 msg/sec  41%
  < 606 msg/sec  53%
  < 617 msg/sec  65%
  < 628 msg/sec  73%
  < 639 msg/sec  82%
  < 650 msg/sec  90%
  < 662 msg/sec  99%
  < 673 msg/sec  100%

#Receiving Throughput
Fastest: 71904 msg/sec
Slowest: 9913 msg/sec
Median: 34958 msg/sec

  < 16112 msg/sec  9%
  < 22311 msg/sec  23%
  < 28510 msg/sec  41%
  < 34709 msg/sec  49%
  < 40908 msg/sec  66%
  < 47107 msg/sec  79%
  < 53306 msg/sec  93%
  < 59506 msg/sec  97%
  < 65705 msg/sec  98%
  < 71904 msg/sec  100%

Native Client: 200 Clients, 100000 messages/client:

go run *go -m 100000

Final Statistics:
Total Published: 20000000 (120512.30/s)
Total Subscribed: 20000000 (120329.98/s)
Publish Time: 165.96 seconds
Subscribe Time: 166.21 seconds
Published/Subscribed 20000000 messages with QoS 0

go run *go -m 100000 -p 1 -s 1

Total Published: 20000000 (15398.88/s)
Total Subscribed: 20000000 (15395.92/s)
Publish   Time: 1298.80 seconds
Subscribe Time: 1299.05 seconds
Published/Subscribed 20000000 messages with QoS 1
