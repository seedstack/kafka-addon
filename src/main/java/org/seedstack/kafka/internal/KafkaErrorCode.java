/**
 * Copyright (c) 2013-2016, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka.internal;

import org.seedstack.shed.exception.ErrorCode;

enum KafkaErrorCode implements ErrorCode {
    UNABLE_TO_CREATE_MESSAGE_CONSUMER_POLLER,
    CONFIG_NOT_FOUND_FOR_MESSAGE_CONSUMER,
    CONFIG_NOT_FOUND_FOR_MESSAGE_STREAM,
    UNABLE_TO_CREATE_MESSAGE_STREAM_HANDLER,
    KAFKA_PRODUCER_SERIALIZER_NOT_FOUND_IN_CLASSPATH,
    KAFKA_PRODUCER_SERIALIZER_NOT_FOUND_IN_CONFIG,
}
