/**
 * Copyright (c) 2013-2016, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import javax.inject.Inject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.seedstack.kafka.KafkaConfig;
import org.seedstack.kafka.spi.MessageConsumer;
import org.seedstack.seed.SeedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConsumerPoller<K, V> implements Runnable {
    private static final String STOPPING_TO_POLL = "Stopping to poll messages for Kafka consumer {}";
    private static final String STARTING_TO_POLL_MESSAGES_FOR_MESSAGE_CONSUMER_LISTENER = "Starting to poll messages " +
            "for MessageConsumer listener {}";
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumerPoller.class);
    private final AtomicBoolean active = new AtomicBoolean(false);
    private final MessageConsumer<K, V> messageConsumer;
    private final KafkaConfig.ConsumerConfig consumerConfig;
    private final String messageConsumerName;
    private Thread thread;
    private Consumer<K, V> consumer;
    private ReentrantReadWriteLock consumerLock = new ReentrantReadWriteLock();
    @Inject
    private Injector injector;

    MessageConsumerPoller(KafkaConfig.ConsumerConfig consumerConfig, MessageConsumer<K, V> messageConsumer,
            String messageConsumerName) {
        this.messageConsumer = messageConsumer;
        this.consumerConfig = consumerConfig;
        this.messageConsumerName = messageConsumerName;
    }

    @Override
    public void run() {
        LOGGER.debug(STARTING_TO_POLL_MESSAGES_FOR_MESSAGE_CONSUMER_LISTENER, messageConsumerName);
        consumerLock.writeLock().lock();
        try {
            consumer = new KafkaConsumer<>(consumerConfig.getProperties());
            if (consumerConfig.getTopicPattern() != null) {
                consumer.subscribe(Pattern.compile(consumerConfig.getTopicPattern()), getConsumerBalancerListener());
            } else {
                consumer.subscribe(consumerConfig.getTopics(), getConsumerBalancerListener());
            }
            while (active.get()) {
                try {
                    long receiveTimeout = 0;
                    ConsumerRecords<K, V> records = consumer.poll(receiveTimeout);
                    if (!records.isEmpty()) {
                        records.forEach(messageConsumer::onMessage);
                        consumer.commitAsync();
                    }
                } catch (Exception e) {
                    messageConsumer.onException(e);
                }
            }
        } catch (Exception e) {
            if (consumer != null) {
                consumer.close();
            }
            throw SeedException.wrap(e, KafkaErrorCode.UNABLE_TO_CREATE_MESSAGE_CONSUMER_POLLER)
                    .put("messageConsumer", messageConsumerName);
        } finally {
            consumerLock.writeLock().unlock();
        }
    }

    synchronized void start() {
        if (!active.getAndSet(true)) {
            checkNotNull(this.messageConsumer);
            checkNotNull(this.consumerConfig);
            startThread();
        }
    }

    synchronized void stop() {
        LOGGER.debug(STOPPING_TO_POLL, messageConsumerName);
        if (active.getAndSet(false)) {
            Consumer consumer = getConsumer();
            if (consumer != null) {
                getConsumer().close();
            }
            thread.interrupt();
        }
    }

    private Consumer getConsumer() {
        consumerLock.readLock().lock();
        try {
            return consumer;
        } finally {
            consumerLock.readLock().unlock();
        }
    }

    private void startThread() {
        thread = new Thread(this);
        thread.setName("kafka-consumer-poller-" + thread.getId());
        thread.start();
    }

    private ConsumerRebalanceListener getConsumerBalancerListener() {
        return injector.getInstance(Key.get(ConsumerRebalanceListener.class, Names.named(messageConsumerName)));
    }
}
