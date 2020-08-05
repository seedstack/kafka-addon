/*
 * Copyright © 2013-2020, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.seedstack.kafka.streams;

import org.apache.kafka.streams.kstream.KStream;
import org.seedstack.kafka.KafkaListener;
import org.seedstack.kafka.StreamBuilder;
import org.seedstack.seed.Logging;
import org.slf4j.Logger;

@KafkaListener(value = "streamTest", topics = "testStreams", retryDelay = 1000)
public class MyStreamBuilder implements StreamBuilder<Integer, String> {
    @Logging
    private Logger logger;

    @Override
    public void buildStream(KStream<Integer, String> stream) {
        stream.foreach((key, value) -> {
            KafkaStreamsIT.count.countDown();
            logger.debug("Processed: {}:{}", key, value);
        });
    }

    @Override
    public void onException(Exception e) {
        logger.error(e.getMessage(), e);
    }
}
