/**
 * Copyright (c) 2013-2016, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka.internal;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Types;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import net.jodah.typetools.TypeResolver;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;
import org.seedstack.kafka.KafkaConfig;
import org.seedstack.kafka.spi.Consumer;
import org.seedstack.kafka.spi.MessageConsumer;
import org.seedstack.seed.SeedException;
import org.seedstack.shed.reflect.Classes;

class ClientsModule extends AbstractModule {
    private final Collection<MessageConsumerPoller> pollers;
    private final Collection<MessageConsumer> messageConsumers;
    private final Collection<Class<MessageConsumer>> messageConsumerClasses;
    private final Map<String, Class<?>> consumerRebalancerListenerClassesMap;
    private final Map<String, KafkaConfig.ProducerConfig> producerConfigs;

    ClientsModule(Collection<MessageConsumerPoller> pollers, Collection<MessageConsumer> messageConsumers,
            Collection<Class<MessageConsumer>> messageConsumerClasses,
            Map<String, Class<?>> consumerRebalancerListenerClassesMap,
            Map<String, KafkaConfig.ProducerConfig> producerConfigs) {
        this.pollers = pollers;
        this.messageConsumers = messageConsumers;
        this.messageConsumerClasses = messageConsumerClasses;
        this.consumerRebalancerListenerClassesMap = consumerRebalancerListenerClassesMap;
        this.producerConfigs = producerConfigs;
    }

    @Override
    protected void configure() {

        producerConfigs.forEach((producerName, producerConfig) -> bindProducer(producerName, producerConfig));
        messageConsumerClasses.forEach(messageConsumerClass -> {
            bind(MessageConsumer.class)
                    .annotatedWith(getNamed(messageConsumerClass)).to(messageConsumerClass);
            bindConsumerRebalancerListener(messageConsumerClass);

        });
        messageConsumers.forEach(messageConsumer -> requestInjection(messageConsumer));
        pollers.forEach(messageConsumerPoller -> requestInjection(messageConsumerPoller));
    }

    private void bindConsumerRebalancerListener(Class<MessageConsumer> messageConsumerClass) {
        String consumerName = messageConsumerClass.getAnnotation(Consumer.class).value();
        if (!consumerRebalancerListenerClassesMap.containsKey(consumerName)) {
            bind(ConsumerRebalanceListener.class).annotatedWith(Names.named(consumerName))
                    .to(NoOpConsumerRebalanceListener.class);

        } else {
            Class<?> consumerRebalancerListenerClass = consumerRebalancerListenerClassesMap.get(consumerName);
            bind(ConsumerRebalanceListener.class).annotatedWith(Names.named(consumerName))
                    .to((Class<? extends ConsumerRebalanceListener>) consumerRebalancerListenerClass);
        }
    }

    private void bindProducer(String producerName, KafkaConfig.ProducerConfig producerConfig) {
        Optional<String> keySerializerName = Optional.ofNullable(producerConfig.getProperties()
                .getProperty("key.serializer"));
        Optional<String> valueSerializerName = Optional.ofNullable(producerConfig.getProperties()
                .getProperty("value.serializer"));

        if (keySerializerName.isPresent() && valueSerializerName.isPresent()) {
            Optional<Class<Serializer>> keySerializerClass = Classes.optional(keySerializerName.get());
            Optional<Class<Serializer>> valueSerilizerClass = Classes.optional(valueSerializerName.get());
            if (keySerializerClass.isPresent() && valueSerilizerClass.isPresent()) {
                Class<?>[] lGeneric = TypeResolver.resolveRawArguments(Serializer.class, keySerializerClass.get());
                Class<?>[] rGeneric = TypeResolver.resolveRawArguments(Serializer.class, valueSerilizerClass.get());
                bind((TypeLiteral<Producer>) TypeLiteral.get(Types.newParameterizedType(Producer.class,
                        lGeneric[0],
                        rGeneric[0]))).annotatedWith(Names.named(producerName))
                        .toInstance(new KafkaProducer(producerConfig.getProperties()));
            } else {
                throw SeedException.createNew(KafkaErrorCode.KAFKA_PRODUCER_SERIALIZER_NOT_FOUND_IN_CLASSPATH)
                        .put("producer", producerName);
            }
        } else {
            throw SeedException.createNew(KafkaErrorCode.KAFKA_PRODUCER_SERIALIZER_NOT_FOUND_IN_CONFIG)
                    .put("producer", producerName);
        }

    }

    private Named getNamed(Class<?> messageConsumerClass) {
        return Names.named(messageConsumerClass.getAnnotation(Consumer.class).value());
    }
}
