/*
 * Copyright © 2013-2019, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka.streams;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.seedstack.seed.testing.junit4.SeedITRunner;

@RunWith(SeedITRunner.class)
public class KafkaStreamsIT {
    private static final String TOPIC = "testStreams";
    static CountDownLatch count = new CountDownLatch(2);
    @Inject
    @Named("producerTest")
    private Producer<Integer, String> producer;

    @Test
    public void streamTest() throws InterruptedException {
        producer.beginTransaction();
        try {
            producer.send(new ProducerRecord<>(TOPIC, 3, "test3"));
            producer.send(new ProducerRecord<>(TOPIC, 4, "test4"));
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
        }
        Assertions.assertThat(count.await(20, TimeUnit.SECONDS)).isTrue();
    }
}
