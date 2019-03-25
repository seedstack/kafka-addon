/*
 * Copyright © 2013-2019, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka;

import org.apache.kafka.streams.kstream.KStream;

/**
 * <p>
 * Implement this interface to build a Kafka stream topology. By annotating the implementing class with
 * {@link KafkaListener}, the stream will be automatically subscribed to the specified topics.
 * </p>
 *
 * <p>
 * The underlying Kafka stream client will be automatically started and stopped with the application. A unhandled
 * failure will gracefully stop the stream and a later attempt will be rescheduled.
 * </p>
 */
public interface StreamBuilder<K, V> {
    /**
     * This method is called upon just after stream creation and subscription. It allows to define the specific
     * topology of the stream.
     *
     * @param stream the just subscribed stream.
     */
    void buildStream(KStream<K, V> stream);

    /**
     * This method is called when an exception occurs during stream processing.
     *
     * @param e the exception that occurred.
     * @throws Exception rethrow an exception to trigger a graceful stop and a retry after the specified delay.
     */
    default void onException(Exception e) throws Exception {
        throw e;
    }
}
