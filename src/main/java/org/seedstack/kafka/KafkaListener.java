/*
 * Copyright © 2013-2019, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * By annotating a class implementing {@link StreamBuilder} or {@link ConsumerListener} with this annotation, the
 * specified topics will be automatically subscribed to. The Kafka consumer will be automatically started and stopped.
 * If an exception is rethrown by the listener, the Kafka client will be stopped gracefully. A retry will be scheduled
 * after the specified retry delay.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Inherited
public @interface KafkaListener {
    /**
     * @return the name of the configured client (stream or consumer) to listen to.
     */
    String value();

    /**
     * Defines the regular expression pattern of the topic(s) to subscribe to. Configuration macros can be used.
     *
     * @return the pattern of topics to subscribe (exclusive with {@link #topics()}).
     */
    String topicPattern() default "";

    /**
     * @return the array of topics to subscribe. Configuration macros can be used.
     */
    String[] topics() default {};

    /**
     * @return the delay in milliseconds to observe after a failure and before retrying.
     */
    int retryDelay() default 30000;
}
