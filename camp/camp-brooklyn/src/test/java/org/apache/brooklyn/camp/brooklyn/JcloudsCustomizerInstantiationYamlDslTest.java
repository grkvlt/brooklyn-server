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
package org.apache.brooklyn.camp.brooklyn;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.camp.brooklyn.spi.creation.CampTypePlanTransformer;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.entity.machine.MachineEntity;
import org.apache.brooklyn.location.jclouds.BasicJcloudsLocationCustomizer;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.apache.brooklyn.util.collections.MutableList;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.options.TemplateOptions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * The test is designed to ensure that when a customizer is configured in
 * yaml with fields that are configured via DSL (forcing brooklyn to
 * return a {@link org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.BrooklynDslCommon.DslObject})
 * that only one customizer is instantiated so that state may be maintained between customize calls.
 *
 * e.g.
 *
 * <pre>
 * {@code
 * brooklyn.config:
 *   provisioning.properties:
 *     customizers:
 *     - $brooklyn:object:
 *       type: org.apache.brooklyn.location.jclouds.networking.SharedLocationSecurityGroupCustomizer
 *       object.fields:
 *         - enabled: $brooklyn:config("kubernetes.sharedsecuritygroup.create")
 * }
 * </pre>
 */
@Test
public class JcloudsCustomizerInstantiationYamlDslTest extends AbstractJcloudsStubYamlTest {

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        RecordingLocationCustomizer.clear();
        super.setUp();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            RecordingLocationCustomizer.clear();
        }
    }
    
    @Test
    public void testCustomizers() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location: " + LOCATION_CATALOG_ID,
                "services:\n" +
                "- type: " + MachineEntity.class.getName(),
                "  brooklyn.config:",
                "    onbox.base.dir.skipResolution: true",
                "    sshMonitoring.enabled: false",
                "    metrics.usage.retrieve: false",
                "    enabled: true",
                "    provisioning.properties:",
                "      customizer:",
                "        $brooklyn:object:",
                "          type: " + RecordingLocationCustomizer.class.getName(),
                "          object.fields:",
                "            enabled: $brooklyn:config(\"enabled\")");

        EntitySpec<?> spec = managementContext.getTypeRegistry().createSpecFromPlan(CampTypePlanTransformer.FORMAT, yaml, RegisteredTypeLoadingContexts.spec(Application.class), EntitySpec.class);
        Entity app = managementContext.getEntityManager().createEntity(spec);

        app.invoke(Startable.START, ImmutableMap.<String, Object>of()).get();

        assertEquals(RecordingLocationCustomizer.calls.size(), 4,
                "Assert all customize functions called: size=" + RecordingLocationCustomizer.calls.size() + "; calls=" + RecordingLocationCustomizer.calls);

        // Assert same instance used for all calls
        RecordingLocationCustomizer firstInstance = RecordingLocationCustomizer.calls.get(0).instance;
        for (RecordingLocationCustomizer.CallParams call : RecordingLocationCustomizer.calls) {
            assertSame(call.instance, firstInstance);
        }

        assertCallsMade("customize1", "customize2", "customize3", "customize4");

        app.invoke(Startable.STOP, ImmutableMap.<String, Object>of()).get();
        // assert that pre and post release have now been called
        assertEquals(RecordingLocationCustomizer.calls.size(), 6,
            "assert that pre and post release have now been called: "
            + "size=" + RecordingLocationCustomizer.calls.size() + "; calls=" + RecordingLocationCustomizer.calls);
        assertCallsMade("customize1", "customize2", "customize3", "customize4", "preRelease", "postRelease");
    }

    private void assertCallsMade(String ...values) {
        List<String> expected = MutableList.of();
        expected.addAll(Arrays.asList(values));
        for (RecordingLocationCustomizer.CallParams parm : RecordingLocationCustomizer.calls) {
            assertTrue(expected.remove(parm.method));
        }
        assertEquals(expected.size(), 0);
    }

    public static class RecordingLocationCustomizer extends BasicJcloudsLocationCustomizer {

        public static final List<CallParams> calls = Lists.newCopyOnWriteArrayList();

        public static void clear() {
            calls.clear();
        }

        private Boolean enabled;

        public void setEnabled(Boolean val) {
            this.enabled = val;
        }
        
        @Override
        public void customize(JcloudsLocation location, ComputeService computeService, TemplateBuilder templateBuilder) {
            if (Boolean.TRUE.equals(enabled)) {
                calls.add(new CallParams(this, "customize1", MutableList.of(location, computeService, templateBuilder)));
            }
        }

        @Override
        public void customize(JcloudsLocation location, ComputeService computeService, Template template) {
            if (Boolean.TRUE.equals(enabled)) {
                calls.add(new CallParams(this, "customize2", MutableList.of(location, computeService, template)));
            }
        }

        @Override
        public void customize(JcloudsLocation location, ComputeService computeService, TemplateOptions templateOptions) {
            if (Boolean.TRUE.equals(enabled)) {
                calls.add(new CallParams(this, "customize3", MutableList.of(location, computeService, templateOptions)));
            }
        }

        @Override
        public void customize(JcloudsLocation location, ComputeService computeService, JcloudsMachineLocation machine) {
            if (Boolean.TRUE.equals(enabled)) {
                calls.add(new CallParams(this, "customize4", MutableList.of(location, computeService, machine)));
            }
        }

        @Override
        public void preRelease(JcloudsMachineLocation machine) {
            if (Boolean.TRUE.equals(enabled)) {
                calls.add(new CallParams(this, "preRelease", MutableList.of(machine)));
            }
        }

        @Override
        public void postRelease(JcloudsMachineLocation machine) {
            if (Boolean.TRUE.equals(enabled)) {
                calls.add(new CallParams(this, "postRelease", MutableList.of(machine)));
            }
        }

        public static class CallParams {
            RecordingLocationCustomizer instance;
            String method;
            List<?> args;

            public CallParams(RecordingLocationCustomizer instance, String method, List<?> args) {
                this.instance = instance;
                this.method = method;
                this.args = args;
            }
        }
    }
}