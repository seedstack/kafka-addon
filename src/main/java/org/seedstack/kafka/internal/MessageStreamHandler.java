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
import java.util.regex.Pattern;
import javax.inject.Inject;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.seedstack.kafka.KafkaConfig;
import org.seedstack.kafka.spi.MessageStream;
import org.seedstack.kafka.spi.Stream;
import org.seedstack.seed.SeedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MessageStreamHandler<K, V> implements Runnable {
    private static final String STOPPING_TO_POLL = "Stopping to poll messages for Kafka kStreams {}";
    private static final String STARTING_TO_POLL_MESSAGES_FOR_MESSAGE_CONSUMER_LISTENER = "Starting to poll messages " +
            "for MessageStream listener {}";
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageStreamHandler.class);
    private final KafkaConfig.StreamConfig streamConfig;
    private final Class<?> messageStreamClass;
    private Thread thread;
    private KafkaStreams kStreams;
    private MessageStream<K, V> messageStream;
    @Inject
    private Injector injector;

    MessageStreamHandler(KafkaConfig.StreamConfig streamConfig, Class<?> messageStreamClass) {
        this.streamConfig = streamConfig;
        this.messageStreamClass = messageStreamClass;
    }

    @Override
    public void run() {
        LOGGER.debug(STARTING_TO_POLL_MESSAGES_FOR_MESSAGE_CONSUMER_LISTENER, getMessageStreamName());
        try {
            KStreamBuilder builder = new KStreamBuilder();
            KStream<K, V> kStream;
            if (streamConfig.getTopicPattern() != null) {
                kStream = builder.stream(Pattern.compile(streamConfig.getTopicPattern()));
            } else {
                kStream = builder.stream(streamConfig.getTopics().toArray(new String[streamConfig.getTopics().size()]));
            }
            messageStream.onStream(kStream);
            kStreams = new KafkaStreams(builder, streamConfig.getProperties());
            kStreams.start();
        } catch (Exception e) {
            if (kStreams != null) {
                kStreams.close();
            }
            if (messageStream != null) {
                messageStream.onException(e);
            }
            throw SeedException.wrap(e, KafkaErrorCode.UNABLE_TO_CREATE_MESSAGE_STREAM_HANDLER)
                    .put("messageStream", getMessageStreamName());
        }
    }

    synchronized void start() {
        messageStream = getMessageStream();
        checkNotNull(this.messageStream);
        checkNotNull(this.streamConfig);
        startThread();
    }

    synchronized void stop() {
        LOGGER.debug(STOPPING_TO_POLL, getMessageStreamName());
        if (kStreams != null) {
            kStreams.close();
        }
        thread.interrupt();
    }

    private void startThread() {
        thread = new Thread(this);
        thread.setName("kafka-stream-handler-" + thread.getId());
        thread.start();
    }

    @SuppressWarnings("unchecked")
    private MessageStream<K, V> getMessageStream() {
        return injector.getInstance(Key.get(MessageStream.class, Names.named(getMessageStreamName())));
    }

    private String getMessageStreamName() {
        return messageStreamClass.getAnnotation(Stream.class).value();
    }
}
