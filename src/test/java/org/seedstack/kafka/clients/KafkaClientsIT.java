/*
 * Copyright © 2013-2020, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.seedstack.kafka.clients;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.seedstack.seed.testing.junit4.SeedITRunner;

@RunWith(SeedITRunner.class)
public class KafkaClientsIT {
    private static final String TOPIC = "testClients";
    static CountDownLatch count = new CountDownLatch(2);
    @Inject
    @Named("producerTest")
    private Producer<Integer, String> producer;
    @Inject
    @Named("consumerTest")
    private Consumer<Integer, String> consumer;
    @Inject
    @Named("consumerTest")
    private Provider<Consumer<Integer, String>> consumerProvider;

    @Test
    public void producerIsInjectable() {
        assertThat(producer).isNotNull();
    }

    @Test
    public void consumerIsInjectable() {
        assertThat(consumer).isNotNull();
    }

    @Test
    public void multipleConsumerCanBeInjected() {
        Set<Consumer<Integer, String>> consumers = new HashSet<>();
        for (int i = 0; i < 4; i++) {
            Consumer<Integer, String> newConsumer = consumerProvider.get();
            assertThat(consumers).doesNotContain(newConsumer);
            consumers.add(newConsumer);
        }
        assertThat(consumers).hasSize(4);
    }

    @Test
    public void consumerTest() throws InterruptedException {
        producer.beginTransaction();
        try {
            producer.send(new ProducerRecord<>(TOPIC, 1, "test"));
            producer.send(new ProducerRecord<>(TOPIC, 2, "test2"));
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
        }
        Assertions.assertThat(count.await(20, TimeUnit.SECONDS)).isTrue();
    }
}
