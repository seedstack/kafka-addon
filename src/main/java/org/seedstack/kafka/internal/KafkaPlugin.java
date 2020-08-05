/*
 * Copyright © 2013-2020, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.seedstack.kafka.internal;

import com.google.common.base.Strings;
import io.nuun.kernel.api.plugin.InitState;
import io.nuun.kernel.api.plugin.context.Context;
import io.nuun.kernel.api.plugin.context.InitContext;
import io.nuun.kernel.api.plugin.request.ClasspathScanRequest;
import net.jodah.typetools.TypeResolver;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.seedstack.kafka.ConsumerListener;
import org.seedstack.kafka.KafkaConfig;
import org.seedstack.kafka.StreamBuilder;
import org.seedstack.seed.SeedException;
import org.seedstack.seed.core.SeedRuntime;
import org.seedstack.seed.core.internal.AbstractSeedPlugin;
import org.seedstack.shed.reflect.Classes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.seedstack.seed.SeedException.createNew;
import static org.seedstack.shed.reflect.Classes.cast;

public class KafkaPlugin extends AbstractSeedPlugin {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPlugin.class);
    private final Map<String, ClientDef<?, ?, KafkaConfig.ClientConfig>> messageConsumers = new HashMap<>();
    private final Map<String, ClientDef<?, ?, KafkaConfig.ProducerConfig>> messageProducers = new HashMap<>();
    private final Set<ListenerHandler> listenerHandlers = new HashSet<>();

    @Override
    public String name() {
        return "kafka-clients";
    }

    @Override
    protected void setup(SeedRuntime seedRuntime) {
        if (!Classes.optional("org.apache.kafka.clients.consumer.Consumer").isPresent()) {
            throw SeedException.createNew(KafkaErrorCode.MISSING_KAFKA_CLIENT_LIBRARY);
        }
    }

    @Override
    public Collection<ClasspathScanRequest> classpathScanRequests() {
        return classpathScanRequestBuilder()
                .predicate(KafkaListenerPredicate.INSTANCE)
                .build();
    }

    @Override
    public InitState initialize(InitContext initContext) {
        KafkaConfig kafkaConfig = getConfiguration(KafkaConfig.class);

        // Register producers
        kafkaConfig.getProducers()
                .entrySet()
                .stream()
                .map(e -> createProducerDef(e.getKey(), e.getValue()))
                .forEach(p -> messageProducers.put(p.getName(), p));

        // Register consumers
        kafkaConfig.getConsumers()
                .entrySet()
                .stream()
                .map(e -> createConsumerDef(e.getKey(), e.getValue()))
                .forEach(c -> messageConsumers.put(c.getName(), c));

        // Detect consumer record listeners
        initContext.scannedTypesByPredicate().get(KafkaListenerPredicate.INSTANCE)
                .stream()
                .filter(ConsumerListener.class::isAssignableFrom)
                .map(c -> new ConsumerListenerHandler<>(cast(c)))
                .forEach(listenerHandlers::add);

        if (Classes.optional("org.apache.kafka.streams.kstream.KStream").isPresent()) {
            // Detect stream handlers
            initContext.scannedTypesByPredicate().get(KafkaListenerPredicate.INSTANCE)
                    .stream()
                    .filter(StreamBuilder.class::isAssignableFrom)
                    .map(c -> new StreamListenerHandler<>(cast(c)))
                    .forEach(listenerHandlers::add);
        } else {
            LOGGER.info("The kafka-streams library is not present in the classpath, stream support disabled");
        }

        return InitState.INITIALIZED;
    }

    private <K, V> ClientDef<K, V, KafkaConfig.ProducerConfig> createProducerDef(String name,
            KafkaConfig.ProducerConfig config) {
        String keySerializerClassName = config.getProperties().getProperty("key.serializer");
        String valueSerializerClassName = config.getProperties().getProperty("value.serializer");

        if (!Strings.isNullOrEmpty(keySerializerClassName) && !Strings.isNullOrEmpty(valueSerializerClassName)) {
            Class<K> keyClass = resolveSerializerClass(name, keySerializerClassName);
            Class<V> valueClass = resolveSerializerClass(name, valueSerializerClassName);
            return new ClientDef<>(name, keyClass, valueClass, config);
        } else {
            throw createNew(KafkaErrorCode.KAFKA_SERIALIZER_NOT_FOUND_IN_CONFIG)
                    .put("producer", name);
        }
    }

    private <K, V> ClientDef<K, V, KafkaConfig.ClientConfig> createConsumerDef(String name,
            KafkaConfig.ClientConfig config) {
        String keyDeserializerClassName = config.getProperties().getProperty("key.deserializer");
        String valueDeserializerClassName = config.getProperties().getProperty("value.deserializer");

        if (!Strings.isNullOrEmpty(keyDeserializerClassName) && !Strings.isNullOrEmpty(valueDeserializerClassName)) {
            Class<K> keyClass = resolveDeserializerClass(name, keyDeserializerClassName);
            Class<V> valueClass = resolveDeserializerClass(name, valueDeserializerClassName);
            return new ClientDef<>(name, keyClass, valueClass, config);
        } else {
            throw createNew(KafkaErrorCode.KAFKA_DESERIALIZER_NOT_FOUND_IN_CONFIG)
                    .put("consumer", name);
        }
    }

    @Override
    public Object nativeUnitModule() {
        return new KafkaModule(messageConsumers, messageProducers, listenerHandlers);
    }

    @Override
    public void start(Context context) {
        listenerHandlers.forEach(ListenerHandler::start);
    }

    @Override
    public void stop() {
        listenerHandlers.forEach(ListenerHandler::stop);
    }

    @SuppressWarnings("unchecked")
    private <T> Class<T> resolveSerializerClass(String producerName, String serializerClassName) {
        return Optional
                .ofNullable(serializerClassName)
                .flatMap(Classes::<Serializer<T>>optional)
                .map(c -> (Class<T>) TypeResolver.resolveRawArgument(Serializer.class, c))
                .orElseThrow(() -> createNew(KafkaErrorCode.KAFKA_SERIALIZER_NOT_FOUND_IN_CLASSPATH)
                        .put("producer", producerName)
                        .put("serializer", serializerClassName));
    }

    @SuppressWarnings("unchecked")
    private <T> Class<T> resolveDeserializerClass(String consumerName, String deserializerClassName) {
        return Optional
                .ofNullable(deserializerClassName)
                .flatMap(Classes::<Deserializer<T>>optional)
                .map(c -> (Class<T>) TypeResolver.resolveRawArgument(Deserializer.class, c))
                .orElseThrow(() -> createNew(KafkaErrorCode.KAFKA_DESERIALIZER_NOT_FOUND_IN_CLASSPATH)
                        .put("consumer", consumerName)
                        .put("deserializer", deserializerClassName));
    }
}
