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
package org.apache.brooklyn.core.effector;

import java.util.Map;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

/**
 * @since 0.11.0
 */
@Beta
public class CreateEntityEffector extends AddEffector {

    private static final Logger LOG = LoggerFactory.getLogger(CreateEntityEffector.class);

    public static final ConfigKey<EntitySpec<?>> ENTITY_SPEC = ConfigKeys.builder(new TypeToken<EntitySpec<?>>() { })
            .name("entitySpec")
            .description("The specification for the entity to be created")
            .build();

    public CreateEntityEffector(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public CreateEntityEffector(Map<String,String> params) {
        this(ConfigBag.newInstance(params));
    }

    public static EffectorBuilder<Entity> newEffectorBuilder(ConfigBag params) {
        EffectorBuilder<Entity> eff = (EffectorBuilder) AddEffector.newEffectorBuilder(Entity.class, params);
        eff.impl(new Body(eff.buildAbstract(), params));
        return eff;
    }

    protected static class Body extends EffectorBody<Entity> {
        protected final Effector<?> effector;
        protected final ConfigBag config;

        protected Object mutex = new Object[0];

        public Body(Effector<?> eff, ConfigBag config) {
            this.effector = eff;
            this.config = config;
            Preconditions.checkNotNull(config.getAllConfigRaw().get(ENTITY_SPEC.getName()), "EntitySpec must be supplied when defining this effector");
        }

        @Override
        public Entity call(final ConfigBag params) {
            synchronized (mutex) {
                EntitySpec<?> spec = EntityInitializers.resolve(config, ENTITY_SPEC);
                Entity child = entity().addChild(spec.configure(params.getAllConfigAsConfigKeyMap()));
                LOG.debug("{}: Added child to {}: {}", new Object[] { this, entity(), child });
                return child;
            }
        }
    }

}
