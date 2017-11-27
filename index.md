---
title: "Kafka"
addon: "Kafka"
repo: "https://github.com/seedstack/jpa-addon"
author: Redouane LOULOU
description: "Integration with Apache Kafka, the distributed streaming platform."
tags:
    - communication
zones:
    - Addons
noMenu: true    
---
Apache Kafka is a scalable distributed streaming platform. It's best suited for handling real-time data streams. The 
Kafka add-on provides an integration of both streams and pub/sub clients, using the Kafka API.

## Dependencies

To add the Kafka add-on to your project, add the following dependency: 

{{< dependency g="org.seedstack.addons.kafka" a="kafka" >}}

You must also add the Apache KAFKA implementation for streams or clients: 

{{< dependency g="org.apache.kafka" a="kafka-streams" v="0.11.0.0" >}}
{{< dependency g="org.apache.kafka" a="kafka-clients" v="0.11.0.0" >}}

## Configuration

Configuration is done by declaring one or more MQTT clients:

{{% config p="kafka" %}}
```yaml
kafka:
  # Configured streams with the name of the stream as key
  streams:
    stream1:
      # List of listening topics
      topics: (List<Class<?>>)
      # Listening topic pattern (topics parameter is ignored when set)
      topicPattern: (String)
      # Needed kafka-streams properties
      properties:
        propertie1: value1
  consumers:
    # Configured kafka-client consumer with the name of the consumer as key
    consumer1:
      # List of listening topics
      topics: (List<Class<?>>)
      # Listening topic pattern (topics parameter is ignored when set)
      topicPattern: (String)
      # Needed kafka-client properties
      properties:
        propertie1: value1
  producers:
    # Configured client producer with the name of the producer as key
    producer1:
      # Needed kafka-client producer properties
      properties:
        propertie1: value1

```
{{% /config %}}
    
## Consuming messages

To receive Kafka messages, create a consumer class which implements the interface {{< java "org.seedstack.kafka.spi.MessageConsumer" >}}   
interface and is annotated with {{< java "org.seedstack.kafka.spi.Consumer" "@" >}}:

```java
@Consumer("consumer1")
public class MyMessageConsumer implements MessageConsumer<Integer, String> {
    @Logging
    private Logger logger;

    @Override
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
        logger.debug("Key [{}], Value [{}]", consumerRecord.key(), consumerRecord.value());
    }

    @Override
    public void onException(Throwable cause) {
        logger.error(cause.getMessage(), cause);
    }
}
```

The {{< java "org.seedstack.kafka.spi.Consumer" "@" >}} annotation takes a value matching to a configured consumer name.

## Publishing messages

In any class, just inject a Kafka Producer  {{< java "org.apache.kafka.clients.producer.Producer" >}} interface
qualified with a matching configured producer name:

```java
public class SomeClass {
    @Inject
    @Named("producer1")
    private Producer<Integer, String> producer;
}
```

To publish a message, use the `send()` method:
 
```java
public class SomeClass {
    @Inject
    @Named("producer1")
    private Producer<Integer, String> producer;
    
    public void someMethod() throws InterruptedException, IOException {
        producer.send(new ProducerRecord<>("topic", "test"));
        producer.close();
    }
}
```

### Streaming messages

To stream Kafka messages, create a stream class which implements the {{< java "org.seedstack.kafka.spi.MessageStream" >}}
interface and is annotated with {{< java "org.seedstack.kafka.spi.Stream" "@" >}}:

```java
@Stream("stream1")
public class MyMessageStream implements MessageStream<Integer, String> {
    @Logging
    private Logger logger;

    @Override
    public void onStream(KStream<Integer, String> stream) {
        stream.peek((key, value) -> {
            logger.info("Stream test: Key [{}], Value [{}]", key, value);
        }).to("topic");
    }

    @Override
    public void onException(Throwable cause) {
        logger.error(cause.getMessage(), cause);
    }
}
```
