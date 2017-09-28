/**
 * Copyright (c) 2013-2016, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.seedstack.kafka.streams;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.fest.assertions.Assertions;
import org.fest.assertions.Fail;
import org.junit.Test;
import org.seedstack.seed.it.AbstractSeedIT;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class KafkaStreamsTest extends AbstractSeedIT {

    private static final String TOPIC = "testStreams";
    @Inject
    @Named("kafkaProducerTest")
    Producer<Integer, String> producer;
    public static CountDownLatch count = new CountDownLatch(2);


    @Test
    public void streamTest() {
        try {
            producer.send(new ProducerRecord<>(TOPIC, 3, "test3"));
            producer.send(new ProducerRecord<>(TOPIC, 4, "test4"));
            Assertions.assertThat(count.await(10, TimeUnit.SECONDS)).isTrue();
            producer.close();
        } catch (Exception e) {
            Fail.fail(e.getMessage(), e);
        }
    }
}
