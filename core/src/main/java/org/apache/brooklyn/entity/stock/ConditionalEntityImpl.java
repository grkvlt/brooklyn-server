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

import java.util.Collection;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;

public class ConditionalEntityImpl extends BasicStartableImpl implements ConditionalEntity {

    @Override
    public void start(Collection<? extends Location> locations) {
        Entity child = sensors().get(CONDITIONAL_ENTITY);
        EntitySpec<?> spec = config().get(CONDITIONAL_ENTITY_SPEC);
        Boolean create = config().get(CREATE_CONDITIONAL_ENTITY);

        // Child not yet created; Entity spec is present; Create flag is true if set
        if (child == null && spec != null && (create == null || Boolean.TRUE.equals(create))) {
            Entity created = addChild(EntitySpec.create(spec));
            sensors().set(CONDITIONAL_ENTITY, created);
        }
        super.start(locations);
    }

}

