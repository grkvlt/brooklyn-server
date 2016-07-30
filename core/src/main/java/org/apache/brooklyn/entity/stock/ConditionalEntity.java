/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.entity.stock;

import java.util.List;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.AttributeSensorAndConfigKey;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.flags.SetFromFlag;

/**
 * An entity that creates an optional child, based on a configuration key value.
 * <p>
 * <pre>
 * - type: org.apache.brooklyn.entity.stock.ConditionalEntity
 *   brooklyn.config:
 *     conditional.entity.create: $brooklyn:scopeRoot().config("enable.loadBalancer")
 *     conditional.entity.spec:
 *       $brooklyn:entitySpec:
 *         type: load-balancer
 *         brooklyn.config:
 *           proxy.port: 8080
 *           loadbalancer.serverpool: $brooklyn:entity("servers")
 * </pre>
 */
@Beta
@ImplementedBy(ConditionalEntityImpl.class)
public interface ConditionalEntity extends BasicStartable {

    @SetFromFlag("entitySpec")
    ConfigKey<EntitySpec<?>> CONDITIONAL_ENTITY_SPEC = ConfigKeys.newConfigKey(new TypeToken<EntitySpec<?>>() { },
            "conditional.entity.spec",
            "The entity specification to be created");

    @SetFromFlag("entitySpecs")
    ConfigKey<List<EntitySpec<?>>> CONDITIONAL_ENTITY_SPEC_LIST = ConfigKeys.newConfigKey(new TypeToken<List<EntitySpec<?>>>() { },
            "conditional.entity.spec.list",
            "A list of entity specifications to be created",
            ImmutableList.<EntitySpec<?>>of());

    @SetFromFlag("create")
    AttributeSensorAndConfigKey<Boolean, Boolean> CREATE_CONDITIONAL_ENTITY = ConfigKeys.newSensorAndConfigKey(Boolean.class,
            "conditional.entity.create",
            "Whether the entity should be created");

    AttributeSensor<List<Entity>> CONDITIONAL_ENTITY_LIST = Sensors.newSensor(new TypeToken<List<Entity>>() { },
            "conditional.entity.list",
            "The list of created entities");

}
