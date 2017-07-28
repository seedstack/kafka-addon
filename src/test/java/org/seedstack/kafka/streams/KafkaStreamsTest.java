/**
 * Copyright (c) 2013-2016, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.seedstack.kafka.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.fest.assertions.Assertions;
import org.fest.assertions.Fail;
import org.junit.Test;
import org.seedstack.seed.it.AbstractSeedIT;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class KafkaStreamsTest extends AbstractSeedIT {

    private static final String TOPIC = "testStreams";
    public static final String BROKER_HOST = "localhost:9092";

    public static CountDownLatch count = new CountDownLatch(2);


    @Test
    public void streamTest() {
        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_HOST);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkaStreamTest");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
            producer.send(new ProducerRecord<>(TOPIC, 3, "test3"));
            producer.send(new ProducerRecord<>(TOPIC, 4, "test4"));
            Assertions.assertThat(count.await(10, TimeUnit.SECONDS)).isTrue();
            producer.close();
        } catch (Exception e) {
            Fail.fail(e.getMessage(), e);
        }
    }
}
