---
title: "Kafka"
addon: "Kafka"
repo: "https://github.com/seedstack/kafka-addon"
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

You must also add the Apache KAFKA implementation for basic clients at least, and optionally for streams: 

{{< dependency g="org.apache.kafka" a="kafka-streams">}}

{{< dependency g="org.apache.kafka" a="kafka-clients">}}

## Configuration

Configuration is done by declaring one or more MQTT clients:

{{% config p="kafka" %}}
```yaml
kafka:
  # Configured Kafka streams with the name of the stream as key
  streams:
    stream1:
      # Kafka properties for configuring the stream
      properties:
        property1: value1
  consumers:
    # Configured Kafka consumer with the name of the consumer as key
    consumer1:
      # Kafka properties for configuring the consumer
      properties:
        property1: value1
  producers:
    # True if the producer is to be used with transactions, false otherwise
    transactional: (boolean)
    # Configured Kafka producer with the name of the producer as key
    producer1:
      # Kafka properties for configuring the producer
      properties:
        property1: value1

```
{{% /config %}}
    

## Publishing 

To publish messages, inject the {{< java "org.apache.kafka.clients.producer.Producer" >}} interface qualified with a 
configured producer name:

```java
public class SomeClass {
    @Inject
    @Named("producer1")
    private Producer<Integer, String> producer;
}
```

Use the Kafka API to send messages. If the produced is configured as transactional, you must enclose your calls to the 
`send()` method with the programmatic Kafka transaction methods:

```java
public class SomeClass {
    @Inject
    @Named("producer1")
    private Producer<Integer, String> producer;
    
    public void someMethod() throws InterruptedException, IOException {
        producer.send(new ProducerRecord<>("topic", "test"));
    }
}
```

{{% callout warning %}}
Do not explicitly close the producer, it will be automatically closed on application shutdown.
{{% /callout %}}

## Receiving

To receive Kafka records, create a class implementing the {{< java "org.seedstack.kafka.ConsumerListener" >}}
interface and annotated with {{< java "org.seedstack.kafka.KafkaListener" "@" >}}:

```java
@KafkaListener(value = "consumer1", topics = "someTopic")
public class MyConsumerListener implements ConsumerListener<Integer, String> {
    @Logging
    private Logger logger;

    @Override
    public void onConsumerRecord(ConsumerRecord<Integer, String> r) {
        logger.info("Received {}:{}", r.key(), r.value());
    }

    @Override
    public void onException(Exception e) throws Exception {
        // process any exception and re-throw an exception if reception must be temporary stopped 
    }
}
```

{{% callout info %}}
If an exception is re-thrown from the `onException()` method, the reception will temporarily stop and the underlying
consumer will be gracefully shutdown. A new attempt, with new consumer and listener instances, will be scheduled after
the retry delay. 
{{% /callout %}}

Using the annotation, you can specify:

* The name of the consumer in configuration that will be used to create the underlying consumer.
* The topic or the topic regular expression pattern to subscribe to.
* The delay to wait before retry in milliseconds.

## Streaming

To build a Kafka stream subscribed to one or more topic(s), create a class implementing the {{< java "org.seedstack.kafka.StreamBuilder" >}}
interface and annotated with {{< java "org.seedstack.kafka.KafkaListener" "@" >}}:

```java
@KafkaListener(value = "stream1", topics = "someTopic")
public class MyStreamBuilder implements StreamBuilder<Integer, String> {
    @Logging
    private Logger logger;

    @Override
    public void buildStream(KStream<Integer, String> stream) {
        stream.foreach((key, value) -> {
            logger.info("Processed: {}:{}", key, value);
        });
    }

    @Override
    public void onException(Exception e) {
        // process any exception and re-throw an exception if reception must be temporary stopped 
    }
}

```

{{% callout info %}}
If an exception is re-thrown from the `onException()` method, the streaming will temporarily stop and the underlying
stream client will be gracefully shutdown. A new attempt, with new stream client and builder instances, will be 
scheduled after the retry delay. 
{{% /callout %}}

Using the annotation, you can specify:

* The name of the stream in configuration that will be used to create the underlying stream client.
* The topic or the topic regular expression pattern to subscribe to.
* The delay to wait before retry in milliseconds.
