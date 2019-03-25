/*
 * Copyright © 2013-2019, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.seedstack.coffig.Config;

@Config("kafka")
public class KafkaConfig {
    private Map<String, ClientConfig> consumers = new HashMap<>();
    private Map<String, ProducerConfig> producers = new HashMap<>();
    private Map<String, ClientConfig> streams = new HashMap<>();

    public Map<String, ClientConfig> getConsumers() {
        return Collections.unmodifiableMap(consumers);
    }

    public Map<String, ProducerConfig> getProducers() {
        return Collections.unmodifiableMap(producers);
    }

    public Map<String, ClientConfig> getStreams() {
        return Collections.unmodifiableMap(streams);
    }

    public KafkaConfig addConsumer(String name, ClientConfig clientConfig) {
        this.consumers.put(name, clientConfig);
        return this;
    }

    public KafkaConfig addProducer(String name, ProducerConfig producerConfig) {
        this.producers.put(name, producerConfig);
        return this;
    }

    public KafkaConfig addStream(String name, ClientConfig clientConfig) {
        this.streams.put(name, clientConfig);
        return this;
    }

    public static class ClientConfig {
        private Properties properties = new Properties();

        public Properties getProperties() {
            return properties;
        }

        public ClientConfig setProperties(Properties properties) {
            this.properties = properties;
            return this;
        }
    }

    public static class ProducerConfig extends ClientConfig {
        private boolean transactional = false;

        public boolean isTransactional() {
            return transactional;
        }

        public ProducerConfig setTransactional(boolean transactional) {
            this.transactional = transactional;
            return this;
        }
    }
}
