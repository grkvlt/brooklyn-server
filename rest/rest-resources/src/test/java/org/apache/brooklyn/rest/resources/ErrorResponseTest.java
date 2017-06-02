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
package org.apache.brooklyn.rest.resources;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.brooklyn.rest.domain.ApiError;
import org.apache.brooklyn.rest.domain.ApplicationSpec;
import org.apache.brooklyn.rest.domain.EntitySpec;
import org.apache.brooklyn.rest.domain.PolicySummary;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.rest.testing.mocks.RestMockSimpleEntity;
import org.apache.brooklyn.rest.testing.mocks.RestMockSimplePolicy;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@Test( // by using a different suite name we disallow interleaving other tests between the methods of this test class, which wrecks the test fixtures
        suiteName = "ErrorResponseTest")
public class ErrorResponseTest extends BrooklynRestResourceTest {

    private final ApplicationSpec simpleSpec = ApplicationSpec.builder().name("simple-app").entities(
            ImmutableSet.of(new EntitySpec("simple-ent", RestMockSimpleEntity.class.getName()))).locations(
            ImmutableSet.of("localhost")).build();
    private String policyId;

    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        startServer();
        Response aResponse = clientDeploy(simpleSpec);
        waitForApplicationToBeRunning(aResponse.getLocation());

        String policiesEndpoint = "/applications/simple-app/entities/simple-ent/policies";

        Response pResponse = client().path(policiesEndpoint)
                .query("type", RestMockSimplePolicy.class.getCanonicalName())
                .type(MediaType.APPLICATION_JSON_TYPE)
                .post(toJsonEntity(ImmutableMap.of()));
        PolicySummary response = pResponse.readEntity(PolicySummary.class);
        assertNotNull(response.getId());
        policyId = response.getId();
    }

    @Test
    public void testResponseToBadRequest() throws IOException {
        String resource = "/applications/simple-app/entities/simple-ent/policies/"+policyId+"/config/"
                + RestMockSimplePolicy.INTEGER_CONFIG.getName();

        Response response = client().path(resource)
                .post(toJsonEntity("notanumber"));

        assertEquals(response.getStatus(), Status.BAD_REQUEST.getStatusCode());
        assertEquals(response.getHeaders().getFirst("Content-Type"), MediaType.APPLICATION_JSON);

        ApiError error = response.readEntity(ApiError.class);
        assertTrue(error.getMessage().toLowerCase().contains("cannot coerce"));
        assertNotNull(error.getError());
        assertEquals(error.getError(), response.getStatus(), Status.BAD_REQUEST.getStatusCode());
    }
}
