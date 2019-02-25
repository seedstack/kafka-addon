/*
 * Copyright © 2013-2019, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * <p>
 * Implement this interface to listen asynchronously to records using a Kafka consumer. By annotating the implementing
 * class with {@link KafkaListener}, the underlying consumer will be automatically subscribed to the specified topics.
 * </p>
 *
 * <p>
 * The underlying Kafka consumer will be automatically started and stopped with the application. A unhandled
 * failure will gracefully stop the consumer and a later attempt will be rescheduled.
 * </p>
 *
 * <p>
 * If the implementing class also implements {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener}, it
 * will be used as the rebalance listener for the underlying consumer. If the implementing class also implements
 * {@link org.apache.kafka.clients.consumer.OffsetCommitCallback}, it will be used as a callback during the
 * asynchronous commits.
 * </p>
 */
public interface ConsumerListener<K, V> {
    /**
     * This method is called when a {@link ConsumerRecords} is available.
     *
     * @param record the record.
     */
    default void onConsumerRecord(ConsumerRecord<K, V> record) {
        // no-op
    }

    /**
     * This method is called when some {@link ConsumerRecords} are available.
     *
     * @param records the just-polled consumer records.
     */
    default void onConsumerRecords(ConsumerRecords<K, V> records) {
        records.forEach(this::onConsumerRecord);
    }

    /**
     * This method is called when an exception occurs during consumer polling or message processing in
     * {@link #onConsumerRecords(ConsumerRecords)}.
     *
     * @param e the exception that occurred.
     * @throws Exception rethrow an exception to trigger a graceful stop and a retry after the specified delay.
     */
    default void onException(Exception e) throws Exception {
        throw e;
    }
}
