/**
 * Copyright (c) 2013-2016, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.seedstack.kafka.spi;


import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A <CODE>MessageConsumer</CODE> is used to receive asynchronously
 * delivered messages.
 */
public interface MessageConsumer<K, V> {

    void onMessage(ConsumerRecord<K, V> consumerRecord);

    void onException(Throwable cause);

}
