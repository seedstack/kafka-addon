/**
 * Copyright (c) 2013-2016, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.seedstack.kafka.internal;


import com.google.inject.Key;
import com.google.inject.name.Names;
import io.nuun.kernel.api.plugin.InitState;
import io.nuun.kernel.api.plugin.context.Context;
import io.nuun.kernel.api.plugin.context.InitContext;
import io.nuun.kernel.api.plugin.request.ClasspathScanRequest;
import org.kametic.specifications.AbstractSpecification;
import org.kametic.specifications.Specification;
import org.seedstack.kafka.KafkaConfig;
import org.seedstack.kafka.spi.Consumer;
import org.seedstack.kafka.spi.MessageConsumer;
import org.seedstack.seed.SeedException;
import org.seedstack.seed.core.internal.AbstractSeedPlugin;
import org.seedstack.shed.reflect.Classes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ClientsPlugin extends AbstractSeedPlugin {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientsPlugin.class);

    private final Specification<Class<?>> messageConsumerSpec = and(classImplements(MessageConsumer.class), classAnnotatedWith(Consumer.class));

    private static final Optional<Class<Object>> KAFKA_OPTIONAL = Classes.optional("org.apache.kafka.clients.KafkaClient");

    private final Specification<Class<?>> consumerRebalancerListenerSpecs = getConsumerRebalancerListenerSpecs();

    private final Collection<MessageConsumerPoller> pollers = new ArrayList<>();

    private final Collection<MessageConsumer> messageConsumers = new ArrayList<>();

    private final Collection<Class<MessageConsumer>> messageConsumerClasses = new ArrayList<>();

    private final Map<String, Class<?>> consumerRebalancerListenerClassesMap = new HashMap<>();

    private final Map<String, KafkaConfig.ProducerConfig> producerConfigs = new HashMap<>();

    private Specification<Class<?>> getConsumerRebalancerListenerSpecs() {
        Optional<Class<Object>> clazz = Classes.optional("org.apache.kafka.clients.consumer.ConsumerRebalanceListener");
        if (clazz.isPresent()) {
            return and(classImplements(clazz.get()), classAnnotatedWith(Consumer.class));
        } else {
            return new AbstractSpecification<Class<?>>() {
                @Override
                public boolean isSatisfiedBy(Class<?> aClass) {
                    return false;
                }
            };
        }
    }

    @Override
    public String name() {
        return "kafka-clients";
    }

    @Override
    public InitState initialize(InitContext initContext) {
        if (KAFKA_OPTIONAL.isPresent()) {
            KafkaConfig kafkaConfig = getConfiguration(KafkaConfig.class);
            producerConfigs.putAll(kafkaConfig.getProducers());
            Collection<Class<?>> consumerRebalancerListenerClasses = initContext.scannedTypesBySpecification().get(consumerRebalancerListenerSpecs);
            if (consumerRebalancerListenerClasses != null) {
                consumerRebalancerListenerClasses.forEach(consumerRebalancerListenerClass -> consumerRebalancerListenerClassesMap.put(consumerRebalancerListenerClass.getAnnotation(Consumer.class).value(), consumerRebalancerListenerClass));
            }
            Collection<Class<?>> messageConsumerClasses = initContext.scannedTypesBySpecification().get(messageConsumerSpec);
            messageConsumerClasses.forEach(messageConsumerClass -> initConsumer((Class<MessageConsumer>) messageConsumerClass, kafkaConfig.getConsumers()));
        } else {
            LOGGER.debug("kafka-clients is not present in the classpath, kafka-clients support disabled");
        }
        return InitState.INITIALIZED;
    }

    private void initConsumer(Class<MessageConsumer> messageConsumerClass, Map<String, KafkaConfig.ConsumerConfig> consumerConfigs) {
        String messageConsumerName = messageConsumerClass.getAnnotation(Consumer.class).value();
        if (consumerConfigs.containsKey(messageConsumerName)) {
            KafkaConfig.ConsumerConfig consumerConfig = consumerConfigs.get(messageConsumerName);
            messageConsumerClasses.add(messageConsumerClass);
            MessageConsumer messageConsumer = new MessageConsumerAdapter(Key.get(MessageConsumer.class, Names.named(messageConsumerName)), consumerConfig.getPoolConfig());
            messageConsumers.add(messageConsumer);
            pollers.add(new MessageConsumerPoller(consumerConfig, messageConsumer, messageConsumerName));

        } else {
            throw SeedException.createNew(KafkaErrorCode.CONFIG_NOT_FOUND_FOR_MESSAGE_CONSUMER).put("messageConsumer", messageConsumerName);
        }

    }

    @Override
    public Collection<ClasspathScanRequest> classpathScanRequests() {
        return classpathScanRequestBuilder()
                .specification(messageConsumerSpec)
                .specification(consumerRebalancerListenerSpecs)
                .build();
    }

    @Override
    public void start(Context context) {
        super.start(context);
        if (KAFKA_OPTIONAL.isPresent()) {
            pollers.forEach(MessageConsumerPoller::start);
        }
    }

    @Override
    public Object nativeUnitModule() {
        return new ClientsModule(pollers, messageConsumers, messageConsumerClasses, consumerRebalancerListenerClassesMap, producerConfigs);
    }


    @Override
    public void stop() {
        super.stop();
        if (KAFKA_OPTIONAL.isPresent()) {
            pollers.forEach(MessageConsumerPoller::stop);
        }
    }
}
