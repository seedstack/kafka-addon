/*
 * Copyright © 2013-2019, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka.internal;

import org.seedstack.kafka.KafkaConfig;

class ClientDef<K, V, T extends KafkaConfig.ClientConfig> {
    private final String name;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final T config;

    ClientDef(String name, Class<K> keyClass, Class<V> valueClass,
            T config) {
        this.name = name;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.config = config;
    }

    String getName() {
        return name;
    }

    Class<K> getKeyClass() {
        return keyClass;
    }

    Class<V> getValueClass() {
        return valueClass;
    }

    T getConfig() {
        return config;
    }
}
