# SparkMeetupKakfka

###Meetup RSVP

http://stream.meetup.com/2/rsvps

### Streaming Meetup.com with Spark
1. Write Producer that consume message from meetup and produce to Kakfa Topic
2. Write Spark Streaming Application that consume message from Kakfa Topic and calculate number of RSVP in real time

### START KAFKA PRODUCER TO GET MESSAGES FROM MEETUP.COM ###
Run KafkaProducerAvro

### LISTEN TO KAFKA TOPIC (RSVPs) ###
kafka-console-consumer --bootstrap-server localhost:9092 --topic meetup-topic-serial


### START SPARK STREAMING TO PROCESS RSVPs FROM KAFKA ###
Run Meetup Scala Streaming

### LISTEN TO KAFKA TOPIC (STREAM OUTPUT) ###
kafka-console-consumer --bootstrap-server localhost:9092 --topic meetup-sink