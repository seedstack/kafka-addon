/*
 * Copyright © 2013-2020, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.seedstack.kafka.internal;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.util.Types;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.seedstack.kafka.KafkaConfig;

class KafkaModule extends AbstractModule {
    private final Map<String, ClientDef<?, ?, KafkaConfig.ClientConfig>> messageConsumers;
    private final Map<String, ClientDef<?, ?, KafkaConfig.ProducerConfig>> messageProducers;
    private final Set<ListenerHandler> listenerHandlers;

    KafkaModule(
            Map<String, ClientDef<?, ?, KafkaConfig.ClientConfig>> messageConsumers, Map<String, ClientDef<?, ?,
            KafkaConfig.ProducerConfig>> messageProducers,
            Set<ListenerHandler> listenerHandlers) {
        this.messageConsumers = messageConsumers;
        this.messageProducers = messageProducers;
        this.listenerHandlers = listenerHandlers;
    }

    @Override
    protected void configure() {
        messageProducers.values().forEach(this::bindProducer);
        messageConsumers.values().forEach(this::bindConsumer);
        listenerHandlers.stream()
                .peek(this::requestInjection)
                .map(ListenerHandler::getListenerClass)
                .forEach(this::bind);
    }

    private <K, V> void bindProducer(ClientDef<K, V, KafkaConfig.ProducerConfig> producerDef) {
        bind(getProducerTypeLiteral(producerDef.getKeyClass(), producerDef.getValueClass()))
                .annotatedWith(Names.named(producerDef.getName()))
                .toProvider(() -> createProducer(producerDef.getConfig()))
                .in(Scopes.SINGLETON);
    }

    private <K, V> KafkaProducer<K, V> createProducer(KafkaConfig.ProducerConfig config) {
        KafkaProducer<K, V> producer = new KafkaProducer<>(config.getProperties());
        if (config.getProperties().containsKey("transactional.id")) {
            producer.initTransactions();
        }
        return producer;
    }

    private <K, V> void bindConsumer(ClientDef<K, V, KafkaConfig.ClientConfig> consumerDef) {
        bind(getConsumerTypeLiteral(consumerDef.getKeyClass(), consumerDef.getValueClass()))
                .annotatedWith(Names.named(consumerDef.getName()))
                .toProvider(() -> createConsumer(consumerDef.getConfig()));
    }

    private <K, V> KafkaConsumer<K, V> createConsumer(KafkaConfig.ClientConfig consumerConfig) {
        return new KafkaConsumer<>(consumerConfig.getProperties());
    }

    @SuppressWarnings("unchecked")
    private <K, V> TypeLiteral<Producer<K, V>> getProducerTypeLiteral(Class<K> keyClass, Class<V> valueClass) {
        return (TypeLiteral<Producer<K, V>>) TypeLiteral.get(Types.newParameterizedType(Producer.class,
                keyClass,
                valueClass));
    }

    @SuppressWarnings("unchecked")
    private <K, V> TypeLiteral<Consumer<K, V>> getConsumerTypeLiteral(Class<K> keyClass, Class<V> valueClass) {
        return (TypeLiteral<Consumer<K, V>>) TypeLiteral.get(Types.newParameterizedType(Consumer.class,
                keyClass,
                valueClass));
    }
}
