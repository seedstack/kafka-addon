/*
 * Copyright © 2013-2020, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.seedstack.kafka.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.inject.Injector;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.seedstack.kafka.ConsumerListener;
import org.seedstack.kafka.KafkaConfig;
import org.seedstack.kafka.KafkaListener;
import org.seedstack.seed.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConsumerListenerHandler<K, V> implements Runnable, ListenerHandler {
    private static final Duration POLLING_DELAY = Duration.ofMillis(1000);
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerListenerHandler.class);
    private final AtomicBoolean active = new AtomicBoolean(false);
    private final Timer timer = new Timer();
    private final Class<? extends ConsumerListener<K, V>> listenerClass;
    private final KafkaListener annotation;
    @Inject
    private Application application;
    @Inject
    private Injector injector;
    private volatile Thread thread;
    private volatile Consumer<K, V> consumer;

    ConsumerListenerHandler(Class<? extends ConsumerListener<K, V>> listenerClass) {
        this.listenerClass = checkNotNull(listenerClass, "listenerClass cannot be null");
        this.annotation = checkNotNull(listenerClass.getAnnotation(KafkaListener.class),
                "@KafkaConsumerListener annotation must be present on listenerClass");
    }

    @Override
    public Class<?> getListenerClass() {
        return listenerClass;
    }

    @Override
    public void run() {
        String consumerName = annotation.value();
        Duration retryDelay = Duration.ofMillis(annotation.retryDelay());

        try {
            ConsumerListener<K, V> listener = injector.getInstance(listenerClass);

            consumer = createConsumer(consumerName);
            subscribe(consumer, listener);

            LOGGER.info("Starting to poll Kafka consumer: {}", consumerName);
            while (active.get()) {
                try {
                    ConsumerRecords<K, V> records = consumer.poll(POLLING_DELAY);
                    if (!records.isEmpty()) {
                        listener.onConsumerRecords(records);
                        if (listener instanceof OffsetCommitCallback) {
                            consumer.commitAsync(((OffsetCommitCallback) listener));
                        } else {
                            consumer.commitAsync();
                        }
                    }
                } catch (Exception e1) {
                    // Ignore exception processing if we are stopping
                    if (active.get()) {
                        try {
                            listener.onException(e1);
                        } catch (Exception e2) {
                            LOGGER.error("An exception occurred in onException() for Kafka consumer: {}",
                                    consumerName,
                                    e2);
                        }
                    }
                    break;
                }
            }
        } finally {
            synchronized (this) {
                if (consumer != null) {
                    LOGGER.info("Synchronously committing Kafka consumer: {}", consumerName);
                    consumer.commitSync();
                    LOGGER.info("Closing Kafka consumer: {}", consumerName);
                    try {
                        consumer.close();
                    } catch (Exception e) {
                        LOGGER.warn("Unable to properly close Kafka consumer: {}", e);
                    }
                }
            }
        }

        if (active.get()) {
            try {
                timer.schedule(new RetryTask(), retryDelay.toMillis());
                LOGGER.warn("Polling failed of Kafka consumer: {}. A retry is scheduled in {} ms",
                        consumerName,
                        retryDelay.toMillis());
            } catch (Exception e) {
                LOGGER.error("Unable to schedule a polling retry of Kafka consumer: {}", consumerName, e);
            }
        }
    }

    @Override
    public void start() {
        if (!active.getAndSet(true)) {
            startPolling();
        }
    }

    @Override
    public void stop() {
        if (active.getAndSet(false)) {
            timer.cancel();
            synchronized (this) {
                if (consumer != null) {
                    consumer.wakeup();
                }
            }
            try {
                thread.join(POLLING_DELAY.multipliedBy(2).toMillis());
            } catch (Exception e) {
                LOGGER.warn("Unable to properly stop poller for Kafka consumer: {}", annotation.value(), e);
            }
        }
    }

    private void startPolling() {
        Thread thread = new Thread(this);
        thread.setName(String.format("%s-kafka-poller", annotation.value()));
        thread.start();
        this.thread = thread;
    }

    private KafkaConsumer<K, V> createConsumer(String consumerName) {
        LOGGER.info("Creating Kafka consumer for message polling: {}", consumerName);
        KafkaConfig kafkaConfig = application.getConfiguration().get(KafkaConfig.class);
        KafkaConfig.ClientConfig clientConfig = checkNotNull(kafkaConfig.getConsumers()
                .get(consumerName), "Missing " + consumerName + " Kafka client configuration");
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(clientConfig.getProperties());
    }

    private void subscribe(Consumer<K, V> consumer, ConsumerListener<K, V> listener) {
        if (!annotation.topicPattern().isEmpty()) {
            String pattern = application.substituteWithConfiguration(annotation.topicPattern());
            LOGGER.debug("Subscribing to topic pattern: {}", pattern);
            if (listener instanceof ConsumerRebalanceListener) {
                LOGGER.debug("Using listener as ConsumerRebalanceListener");
                consumer.subscribe(Pattern.compile(pattern), ((ConsumerRebalanceListener) listener));
            } else {
                LOGGER.debug("ConsumerRebalanceListener not implemented by the listener");
                consumer.subscribe(Pattern.compile(pattern));
            }
        } else {
            List<String> topics = Arrays.stream(annotation.topics())
                    .map(application::substituteWithConfiguration)
                    .collect(Collectors.toList());
            LOGGER.debug("Subscribing to topics: {}", String.join(", ", topics));
            if (listener instanceof ConsumerRebalanceListener) {
                LOGGER.debug("Using listener as ConsumerRebalanceListener");
                consumer.subscribe(topics, ((ConsumerRebalanceListener) listener));
            } else {
                LOGGER.debug("ConsumerRebalanceListener not implemented by the listener");
                consumer.subscribe(topics);
            }
        }
    }

    private class RetryTask extends TimerTask {
        @Override
        public void run() {
            startPolling();
        }
    }
}
