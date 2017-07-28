/**
 * Copyright (c) 2013-2016, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.seedstack.kafka.clients;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.seedstack.kafka.spi.Consumer;
import org.seedstack.kafka.spi.MessageConsumer;
import org.seedstack.seed.Logging;
import org.slf4j.Logger;

@Consumer("kafkaConsumerTest")
public class MyMessageConsumer implements MessageConsumer<Integer, String> {


    @Logging
    Logger logger;

    @Override
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
        KafkaClientsTest.count.countDown();
        logger.debug("Key [{}], Value [{}]", consumerRecord.key(), consumerRecord.value());
    }

    @Override
    public void onException(Throwable cause) {
        logger.error(cause.getMessage(), cause);
    }
}
