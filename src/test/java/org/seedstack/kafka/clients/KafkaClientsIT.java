/**
 * Copyright (c) 2013-2016, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka.clients;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.fest.assertions.Assertions;
import org.junit.Test;
import org.seedstack.seed.Logging;
import org.seedstack.seed.it.AbstractSeedIT;
import org.slf4j.Logger;

public class KafkaClientsIT extends AbstractSeedIT {
    private static final String TOPIC = "testClients";
    static CountDownLatch count = new CountDownLatch(2);
    @Inject
    @Named("kafkaProducerTest")
    private Producer<Integer, String> producer;
    @Logging
    private Logger logger;

    @Test
    public void consumerTest() throws InterruptedException, IOException {
        producer.send(new ProducerRecord<>(TOPIC, 1, "test"));
        producer.send(new ProducerRecord<>(TOPIC, 2, "test2"));
        producer.close();
        Assertions.assertThat(count.await(10, TimeUnit.SECONDS)).isTrue();
    }
}
