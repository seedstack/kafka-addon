/*
 * Copyright © 2013-2019, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Strings;
import com.google.inject.Injector;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.seedstack.kafka.KafkaConfig;
import org.seedstack.kafka.KafkaListener;
import org.seedstack.kafka.StreamBuilder;
import org.seedstack.seed.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamListenerHandler<K, V> implements ListenerHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamListenerHandler.class);
    private static final int CLOSING_DELAY = 2000;
    private final AtomicBoolean active = new AtomicBoolean(false);
    private final Timer timer = new Timer();
    private final Class<? extends StreamBuilder<K, V>> listenerClass;
    private final KafkaListener annotation;
    @Inject
    private Application application;
    @Inject
    private Injector injector;
    private KafkaStreams kafkaStreams;

    StreamListenerHandler(Class<? extends StreamBuilder<K, V>> listenerClass) {
        this.listenerClass = checkNotNull(listenerClass, "handlerClass cannot be null");
        this.annotation = checkNotNull(listenerClass.getAnnotation(KafkaListener.class),
                "@KafkaStreamListener annotation must be present on handlerClass");
    }

    @Override
    public Class<?> getListenerClass() {
        return this.listenerClass;
    }

    @Override
    public synchronized void start() {
        if (!active.getAndSet(true)) {
            startStream();
        }
    }

    @Override
    public synchronized void stop() {
        if (active.getAndSet(false)) {
            timer.cancel();
            stopStream();
        }
    }

    private synchronized void startStream() {
        StreamBuilder<K, V> handler = injector.getInstance(listenerClass);

        KStream<K, V> kStream;
        StreamsBuilder builder = new StreamsBuilder();
        if (!Strings.isNullOrEmpty(annotation.topicPattern())) {
            String pattern = application.substituteWithConfiguration(annotation.topicPattern());
            LOGGER.debug("Building stream subscribed to topic pattern: {}", pattern);
            kStream = builder.stream(pattern);
        } else {
            List<String> topics = Arrays.stream(annotation.topics())
                    .map(application::substituteWithConfiguration)
                    .collect(Collectors.toList());
            LOGGER.debug("Building stream subscribed to topic(s): {}", String.join(", ", topics));
            kStream = builder.stream(topics);
        }

        String streamName = annotation.value();
        try {
            handler.buildStream(kStream);
        } catch (Exception e) {
            LOGGER.error("An exception occurred during {} Kafka stream buildStream() method", streamName, e);
        }

        KafkaConfig kafkaConfig = application.getConfiguration().get(KafkaConfig.class);
        KafkaConfig.ClientConfig clientConfig = checkNotNull(kafkaConfig.getStreams().get(streamName),
                "Missing " + streamName + " Kafka client configuration");

        kafkaStreams = new KafkaStreams(builder.build(), clientConfig.getProperties());
        Thread.setDefaultUncaughtExceptionHandler((Thread thread, Throwable t) -> {
            if (active.get()) {
                if (t instanceof Exception) {
                    // Only process exception if handler is still active
                    try {
                        handler.onException((Exception) t);
                    } catch (Exception e2) {
                        LOGGER.error("An exception occurred during {} Kafka stream exception handling",
                                streamName,
                                e2);
                    }
                } else {
                    thread.getThreadGroup().uncaughtException(thread, t);
                }

                stopStream();

                try {
                    timer.schedule(new RetryTask(), annotation.retryDelay());
                    LOGGER.info("{} Kafka stream failed, a retry is scheduled in {} ms",
                            streamName,
                            annotation.retryDelay());
                } catch (Exception e) {
                    LOGGER.error("Unable to schedule a retry of {} Kafka stream", streamName, e);
                }
            }
        });
        LOGGER.info("Starting {} Kafka stream", streamName);
        kafkaStreams.start();
    }

    private synchronized void stopStream() {
        if (kafkaStreams != null) {
            String streamName = annotation.value();
            LOGGER.info("Closing {} Kafka stream", streamName);
            try {
                if (!kafkaStreams.close(Duration.ofMillis(CLOSING_DELAY))) {
                    LOGGER.warn("Unable to close {} Kafka stream in time", annotation.value());
                }
            } catch (Exception e) {
                LOGGER.warn("Error while closing {} Kafka stream", streamName, e);
            }
        }
    }

    private class RetryTask extends TimerTask {
        @Override
        public void run() {
            startStream();
        }
    }
}
