/**
 * Copyright (c) 2013-2016, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.seedstack.kafka.internal;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.seedstack.kafka.KafkaConfig;
import org.seedstack.kafka.spi.MessageConsumer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MessageConsumerAdapter implements MessageConsumer {


    private ThreadPoolExecutor pool;
    private KafkaConfig.PoolConfig poolConfig;
    Key<MessageConsumer> consumerKey;
    @Inject
    private Injector injector;

    public MessageConsumerAdapter(Key consumerKey, KafkaConfig.PoolConfig poolConfig) {
        this.poolConfig = poolConfig;
        this.consumerKey = consumerKey;
        if (poolConfig.isEnabled()) {
            pool = getThreadPoolExecutor(poolConfig);
        }
    }

    @Override
    public void onMessage(ConsumerRecord value) {
        MessageConsumer messageConsumer = injector.getInstance(this.consumerKey);
        if (poolConfig.isEnabled()) {
            pool.submit(() -> messageConsumer.onMessage(value));
        } else {
            messageConsumer.onMessage(value);
        }
    }

    @Override
    public void onException(Throwable cause) {
        injector.getInstance(this.consumerKey).onException(cause);
    }

    ThreadPoolExecutor getThreadPoolExecutor(KafkaConfig.PoolConfig poolConfig) {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                poolConfig.getCoreSize(),
                poolConfig.getMaxSize(),
                poolConfig.getKeepAlive(), TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(poolConfig.getQueueSize())
        );
        threadPoolExecutor.setRejectedExecutionHandler(getRejectedExecutionHandler(poolConfig));
        return threadPoolExecutor;
    }

    private RejectedExecutionHandler getRejectedExecutionHandler(KafkaConfig.PoolConfig poolConfig) {
        switch (poolConfig.getRejectedExecutionPolicy()) {
            case ABORT:
                return new ThreadPoolExecutor.AbortPolicy();
            case DISCARD:
                return new ThreadPoolExecutor.DiscardPolicy();
            case DISCARD_OLDEST:
                return new ThreadPoolExecutor.DiscardOldestPolicy();
            case CALLER_RUNS:
                return new ThreadPoolExecutor.CallerRunsPolicy();
            default:
                return new ThreadPoolExecutor.CallerRunsPolicy();
        }
    }
}
