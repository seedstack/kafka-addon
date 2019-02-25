/*
 * Copyright © 2013-2019, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka.clients;

import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.seedstack.kafka.ConsumerListener;
import org.seedstack.kafka.KafkaListener;
import org.seedstack.seed.Logging;
import org.slf4j.Logger;

@KafkaListener(value = "consumerTest", topics = "testClients", retryDelay = 1000)
public class MyConsumerListener implements ConsumerListener<Integer, String>, ConsumerRebalanceListener,
        OffsetCommitCallback {
    @Logging
    private Logger logger;

    @Override
    public void onConsumerRecord(ConsumerRecord<Integer, String> r) {
        logger.info("Received {}:{}", r.key(), r.value());
        KafkaClientsIT.count.countDown();
    }

    @Override
    public synchronized void onException(Exception e) throws Exception {
        logger.warn("Consumer exception", e);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("Partitions revoked: " + partitions.toString());
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("Partitions assigned: " + partitions.toString());
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        logger.info("Async commit complete");
    }
}
