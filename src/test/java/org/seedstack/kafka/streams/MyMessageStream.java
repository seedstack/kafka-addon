/**
 * Copyright (c) 2013-2016, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka.streams;

import org.apache.kafka.streams.kstream.KStream;
import org.seedstack.kafka.spi.MessageStream;
import org.seedstack.kafka.spi.Stream;
import org.seedstack.seed.Logging;
import org.slf4j.Logger;

@Stream("kafkaStreamTest")
public class MyMessageStream implements MessageStream<Integer, String> {
    @Logging
    private Logger logger;

    @Override
    public void onStream(KStream<Integer, String> stream) {

        stream.foreach((key, value) -> {
            KafkaStreamsIT.count.countDown();
            logger.debug("Stream test: Key [{}], Value [{}]", key, value);
        });
    }

    @Override
    public void onException(Throwable throwable) {
        logger.error(throwable.getMessage(), throwable);
    }
}
