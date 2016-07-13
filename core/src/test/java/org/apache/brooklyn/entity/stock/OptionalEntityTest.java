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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;

public class OptionalEntityTest {

    private ManagementContext managementContext;
    private SimulatedLocation loc1;
    private TestApplication app;
    private OptionalEntity optional;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        managementContext = LocalManagementContextForTests.newInstance();
        loc1 = managementContext.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));
        app = TestApplication.Factory.newManagedInstanceForTests(managementContext);
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (managementContext != null) Entities.destroyAll(managementContext);
    }

    @Test
    public void testAddsOptionalWhenConfigured() throws Exception {
        optional = app.addChild(EntitySpec.create(OptionalEntity.class)
                .configure(OptionalEntity.CREATE_OPTIONAL_ENTITY, true)
                .configure(OptionalEntity.OPTIONAL_ENTITY_SPEC, EntitySpec.create(TestEntity.class)));
        app.start(ImmutableList.of(loc1));

        assertEquals(optional.getChildren().size(), 1);
        assertTrue(Iterables.getOnlyElement(optional.getChildren()) instanceof TestEntity);
    }

    @Test
    public void testDoesNotAddsOptionalWhenConfigured() throws Exception {
        optional = app.addChild(EntitySpec.create(OptionalEntity.class)
                .configure(OptionalEntity.CREATE_OPTIONAL_ENTITY, false)
                .configure(OptionalEntity.OPTIONAL_ENTITY_SPEC, EntitySpec.create(TestEntity.class)));
        app.start(ImmutableList.of(loc1));

        assertEquals(optional.getChildren().size(), 0);
    }

}
