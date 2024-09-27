MQTT 3.1.1/5 implementataion.
cd mqttserver
go run mqttserver.go &

Run each of the tests in examples and client dirs:

cd client
go run *go -c 100 -m 10000
Total Published Messages: 1000000, Total Time: 81.86 seconds, Overall Publish Rate: 12216.28 messages/second
Total Subscribed Messages: 1000000, Total Time: 82.18 seconds, Overall Subscription Rate: 12169.00 messages/second

go run *go -c 100 -m 10000 -s 1 -p 1
Total Published Messages: 1000000, Total Time: 29.75 seconds, Overall Publish Rate: 33610.06 messages/second
Total Subscribed Messages: 1000000, Total Time: 30.09 seconds, Overall Subscription Rate: 33238.05 messages/second

go run *go -c 100 -m 10000 -s 0 -p 0
Total Published Messages: 1000000, Total Time: 1.72 seconds, Overall Publish Rate: 581057.52 messages/second
Total Subscribed Messages: 1000000, Total Time: 7.77 seconds, Overall Subscription Rate: 128716.69 messages/second

cd examples
#Topic filter test
go run topic* // TCP
go run topic* -b ws://localhost:1885/mqtt-ws  //Web socket

go run conc*
