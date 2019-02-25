/*
 * Copyright © 2013-2019, The SeedStack authors <http://seedstack.org>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package org.seedstack.kafka.internal;

import static org.seedstack.shed.reflect.AnnotationPredicates.elementAnnotatedWith;
import static org.seedstack.shed.reflect.ClassPredicates.classImplements;

import org.kametic.specifications.AbstractSpecification;
import org.seedstack.kafka.ConsumerListener;
import org.seedstack.kafka.KafkaListener;
import org.seedstack.kafka.StreamBuilder;

class KafkaListenerSpecification extends AbstractSpecification<Class<?>> {
    static final KafkaListenerSpecification INSTANCE = new KafkaListenerSpecification();

    private KafkaListenerSpecification() {
        // no instantiation allowed
    }

    @Override
    public boolean isSatisfiedBy(Class<?> candidate) {
        return (classImplements(StreamBuilder.class).or(classImplements(ConsumerListener.class)))
                .and(elementAnnotatedWith(KafkaListener.class, true))
                .test(candidate);
    }
}
