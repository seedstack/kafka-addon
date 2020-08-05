/*
 * Copyright © 2013-2020, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.seedstack.kafka.internal;

import org.seedstack.kafka.ConsumerListener;
import org.seedstack.kafka.KafkaListener;
import org.seedstack.kafka.StreamBuilder;

import java.util.function.Predicate;

import static org.seedstack.shed.reflect.AnnotationPredicates.elementAnnotatedWith;
import static org.seedstack.shed.reflect.ClassPredicates.classImplements;

class KafkaListenerPredicate implements Predicate<Class<?>> {
    static final KafkaListenerPredicate INSTANCE = new KafkaListenerPredicate();

    private KafkaListenerPredicate() {
        // no instantiation allowed
    }

    @Override
    public boolean test(Class<?> candidate) {
        return (classImplements(StreamBuilder.class).or(classImplements(ConsumerListener.class)))
                .and(elementAnnotatedWith(KafkaListener.class, true))
                .test(candidate);
    }
}
