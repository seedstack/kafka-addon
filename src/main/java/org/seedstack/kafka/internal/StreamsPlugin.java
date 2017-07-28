/**
 * Copyright (c) 2013-2016, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.seedstack.kafka.internal;


import com.google.inject.AbstractModule;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.nuun.kernel.api.plugin.InitState;
import io.nuun.kernel.api.plugin.context.Context;
import io.nuun.kernel.api.plugin.context.InitContext;
import io.nuun.kernel.api.plugin.request.ClasspathScanRequest;
import org.kametic.specifications.Specification;
import org.seedstack.kafka.KafkaConfig;
import org.seedstack.kafka.spi.MessageStream;
import org.seedstack.kafka.spi.Stream;
import org.seedstack.seed.SeedException;
import org.seedstack.seed.core.internal.AbstractSeedPlugin;
import org.seedstack.shed.reflect.Classes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public class StreamsPlugin extends AbstractSeedPlugin {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamsPlugin.class);

    private final Specification<Class<?>> messageStreamSpec = and(classImplements(MessageStream.class), classAnnotatedWith(Stream.class));

    private final Collection<MessageStreamHandler> handlers = new ArrayList<>();

    private final Collection<MessageStream> messageStreams = new ArrayList<>();

    private final Collection<Class> messageStreamClasses = new ArrayList<>();

    private static final Optional<Class<Object>> KAFKA_OPTIONAL = Classes.optional("org.apache.kafka.streams.KafkaStreams");

    @Override
    public String name() {
        return "kafka-streams";
    }

    @Override
    public InitState initialize(InitContext initContext) {
        if (KAFKA_OPTIONAL.isPresent()) {
            KafkaConfig kafkaStreamsConfig = getConfiguration(KafkaConfig.class);
            Collection<Class<?>> messageStreamClasses = initContext.scannedTypesBySpecification().get(messageStreamSpec);
            messageStreamClasses.forEach(messageStreamClass -> initStream(messageStreamClass, kafkaStreamsConfig.getStreams()));
        } else {
            LOGGER.debug("kafka-streams is not present in the classpath, kafka-streams support disabled");
        }
        return InitState.INITIALIZED;
    }

    private void initStream(Class<?> messageStreamClass, Map<String, KafkaConfig.StreamConfig> streams) {
        String messageStreamName = messageStreamClass.getAnnotation(Stream.class).value();
        if (streams.containsKey(messageStreamName)) {
            KafkaConfig.StreamConfig streamConfig = streams.get(messageStreamName);
            messageStreamClasses.add(messageStreamClass);
            handlers.add(new MessageStreamHandler(streamConfig, messageStreamClass));
        } else {
            throw SeedException.createNew(KafkaErrorCode.CONFIG_NOT_FOUND_FOR_MESSAGE_STREAM).put("messageStream", messageStreamName);
        }

    }

    private Named getNamed(Class<?> messageStreamClass) {
        return Names.named(messageStreamClass.getAnnotation(Stream.class).value());
    }


    @Override
    public Collection<ClasspathScanRequest> classpathScanRequests() {
        return classpathScanRequestBuilder()
                .specification(messageStreamSpec)
                .build();
    }

    @Override
    public void start(Context context) {
        super.start(context);
        if (KAFKA_OPTIONAL.isPresent()) {
            handlers.forEach(MessageStreamHandler::start);
        }
    }

    @Override
    public Object nativeUnitModule() {
        return new AbstractModule() {
            @Override
            protected void configure() {
                handlers.forEach(handler -> requestInjection(handler));
                messageStreamClasses.forEach(messageStreamClass -> bind(MessageStream.class)
                        .annotatedWith(getNamed(messageStreamClass)).to(messageStreamClass));
                messageStreams.forEach(messageStream -> requestInjection(messageStream));
            }
        };
    }

    @Override
    public void stop() {
        super.stop();
        if (KAFKA_OPTIONAL.isPresent()) {
            handlers.forEach(MessageStreamHandler::stop);
        }
    }
}
