/*
 * Copyright © 2013-2019, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka.internal;

import org.seedstack.shed.exception.ErrorCode;

enum KafkaErrorCode implements ErrorCode {
    KAFKA_DESERIALIZER_NOT_FOUND_IN_CLASSPATH,
    KAFKA_DESERIALIZER_NOT_FOUND_IN_CONFIG,
    KAFKA_SERIALIZER_NOT_FOUND_IN_CLASSPATH,
    KAFKA_SERIALIZER_NOT_FOUND_IN_CONFIG,
    MISSING_KAFKA_CLIENT_LIBRARY,
}
