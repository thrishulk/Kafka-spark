
Kafka setup on windows:
1. Download kafka (binary) from the below kafka url
	https://kafka.apache.org/downloads
2. Extract/Unzip the contents
3. Update the server.properties file under config directory
	log.dirs=C:\Projects\kafka_2.11-2.1.0\logs
	zookeeper.connect=localhost:2181
	broker.id=0
	
Comands to start services:

1. Start the Zookeeper service
	bin\windows\zookeeper-server-start.bat config\zookeeper.properties
2. Start the Kafka Broker server
	bin\windows\kafka-server-start.bat config\server.properties
	
Kafka usage from Console:

Console-Producer:
	C:\Projects\kafka_2.11-2.1.0>bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic consoletest
	>Hi I am Testing Console
	[2019-07-17 10:55:09,935] WARN [Producer clientId=console-producer] Error while fetching metadata with correlation id 1 : {consoletest=LEADER_NOT_AVAILABLE} (org.apache.kafka.clients.NetworkClient)
	[2019-07-17 10:55:10,045] WARN [Producer clientId=console-producer] Error while fetching metadata with correlation id 3 : {consoletest=LEADER_NOT_AVAILABLE} (org.apache.kafka.clients.NetworkClient)
	>This is my second message
	>Third message is also in its way
	>	

Console-Consumer:
	--without mention the offset ---
	C:\Projects\kafka_2.11-2.1.0>bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic consoletest
	Fourth message

	--indicated to consume from begining ---
	C:\Projects\kafka_2.11-2.1.0>bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic consoletest --from-beginning
	Hi I am Testing Console
	This is my second message
	Third message is also in its way
	Fourth message

 
Stream API + Kafka Console Producer:
  
  G:\Projects\kafka_2.11-2.1.0\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic meets
  
  curl -i http://stream.meetup.com/2/rsvps | kafka-console-producer.bat --broker-list localhost:9092 --topic meets
  
  
 