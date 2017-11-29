/**
 * Copyright (c) 2013-2016, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.seedstack.coffig.Config;
import org.seedstack.coffig.SingleValue;

@Config("kafka")
public class KafkaConfig {
    private Map<String, StreamConfig> streams = new HashMap<>();
    private Map<String, ConsumerConfig> consumers = new HashMap<>();
    private Map<String, ProducerConfig> producers = new HashMap<>();

    public Map<String, StreamConfig> getStreams() {
        return Collections.unmodifiableMap(streams);
    }

    public KafkaConfig addStream(String name, StreamConfig streamConfig) {
        this.streams.put(name, streamConfig);
        return this;
    }

    public Map<String, ConsumerConfig> getConsumers() {
        return Collections.unmodifiableMap(consumers);
    }

    public KafkaConfig addConsumer(String name, ConsumerConfig consumerConfig) {
        this.consumers.put(name, consumerConfig);
        return this;
    }

    public Map<String, ProducerConfig> getProducers() {
        return Collections.unmodifiableMap(producers);
    }

    public KafkaConfig addProducer(String name, ProducerConfig producerConfig) {
        this.producers.put(name, producerConfig);
        return this;
    }

    public static class ConsumerConfig {
        private List<String> topics = new ArrayList<>();
        private Properties properties = new Properties();
        private PoolConfig poolConfig = new PoolConfig();
        private String topicPattern;

        public List<String> getTopics() {
            return Collections.unmodifiableList(topics);
        }

        public ConsumerConfig addTopic(String topic) {
            topics.add(topic);
            return this;
        }

        public Properties getProperties() {
            return properties;
        }

        public ConsumerConfig setProperties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public PoolConfig getPoolConfig() {
            return poolConfig;
        }

        public ConsumerConfig setPoolConfig(PoolConfig poolConfig) {
            this.poolConfig = poolConfig;
            return this;
        }

        public String getTopicPattern() {
            return topicPattern;
        }

        public ConsumerConfig setTopicPattern(String topicPattern) {
            this.topicPattern = topicPattern;
            return this;
        }
    }

    public static class PoolConfig {
        @SingleValue
        private boolean enabled = true;
        private int coreSize = 1;
        private int maxSize = 2;
        private int queueSize = 500;
        private int keepAlive = 60;
        private RejectedExecutionPolicy rejectedExecutionPolicy = RejectedExecutionPolicy.CALLER_RUNS;

        public boolean isEnabled() {
            return enabled;
        }

        public PoolConfig setEnabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public int getCoreSize() {
            return coreSize;
        }

        public PoolConfig setCoreSize(int coreSize) {
            this.coreSize = coreSize;
            return this;
        }

        public int getMaxSize() {
            return maxSize;
        }

        public PoolConfig setMaxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public int getQueueSize() {
            return queueSize;
        }

        public PoolConfig setQueueSize(int queueSize) {
            this.queueSize = queueSize;
            return this;
        }

        public int getKeepAlive() {
            return keepAlive;
        }

        public PoolConfig setKeepAlive(int keepAlive) {
            this.keepAlive = keepAlive;
            return this;
        }

        public RejectedExecutionPolicy getRejectedExecutionPolicy() {
            return rejectedExecutionPolicy;
        }

        public PoolConfig setRejectedExecutionPolicy(RejectedExecutionPolicy rejectedExecutionPolicy) {
            this.rejectedExecutionPolicy = rejectedExecutionPolicy;
            return this;
        }

        /**
         * Supported RejectedExecutionHandler policies :
         * <ul>
         * <li></li>
         * <li>ABORT:  {@link java.util.concurrent.ThreadPoolExecutor.AbortPolicy} is called.</li>
         * <li>DISCARD:  {@link java.util.concurrent.ThreadPoolExecutor.DiscardPolicy} is called.</li>
         * <li>CALLER_RUNS: {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy} is called.</li>
         * <li>DISCARD_OLDEST: {@link java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy} is called.</li>
         * </ul>
         */
        public enum RejectedExecutionPolicy {
            ABORT,
            DISCARD,
            CALLER_RUNS,
            DISCARD_OLDEST
        }
    }

    public static class StreamConfig {
        private List<String> topics = new ArrayList<>();
        private Properties properties = new Properties();
        private String topicPattern;

        public List<String> getTopics() {
            return Collections.unmodifiableList(topics);
        }

        public StreamConfig addTopic(String topic) {
            topics.add(topic);
            return this;
        }

        public Properties getProperties() {
            return properties;
        }

        public StreamConfig setProperties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public String getTopicPattern() {
            return topicPattern;
        }

        public StreamConfig setTopicPattern(String topicPattern) {
            this.topicPattern = topicPattern;
            return this;
        }
    }

    public static class ProducerConfig {
        private Properties properties = new Properties();

        public Properties getProperties() {
            return properties;
        }

        public ProducerConfig setProperties(Properties properties) {
            this.properties = properties;
            return this;
        }
    }
}
