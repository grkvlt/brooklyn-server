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
package org.apache.brooklyn.location.jclouds.awsec2.lambda;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.effector.ParameterType;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.BasicParameterType;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.awsec2.lambda.AwsLambdaUtils.InvocationType;
import org.apache.brooklyn.location.jclouds.awsec2.lambda.AwsLambdaUtils.LogType;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.commons.codec.binary.Base64;
import org.jclouds.aws.ec2.compute.AWSEC2ComputeServiceContext;
import org.jclouds.aws.filters.FormSigner;
import org.jclouds.http.HttpRequest;
import org.jclouds.http.HttpResponse;
import org.jclouds.util.Strings2;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

/**
 * An {@link Effector} that executes an AWS Lambda function.
 * <pre>{@code
 * POST /2015-03-31/functions/{FunctionName}/invocations?Qualifier={Qualifier} HTTP/1.1
 * X-Amz-Invocation-Type: {InvocationType}
 * X-Amz-Log-Type: {LogType}
 * X-Amz-Client-Context: {ClientContext}
 *
 * {Payload}
 * }</pre>
 * Use with a {@code jclouds:aws-ec2} location, as follows:
 * <pre>{@code
 * location: jclouds:aws-ec2:eu-central-1
 * services:
 *   - type: basic-startable
 *     name: "lamdbda"
 *     brooklyn.initializers:
 *       - type: org.apache.brooklyn.location.jclouds.awsec2.lambda.AwsLambdaEffector
 *         brooklyn.config:
 *           name: "demo"
 *           description: |
 *             AWS Lambda effector, named "demo" and executing "demo-function"
 *           functionName: "demo-function"
 *           invocationType: RequestResponse
 *           logType: Tail
 * }</pre>
 * <p>
 * TODO test endpoint across different regions
 * TODO integrate with AWS API gateway
 * TODO implement async invocation properly
 * TODO correctly handle different return types
 * TODO better error handling
 */
public final class AwsLambdaEffector extends AddEffector {

    public static final ConfigKey<String> FUNCTION_NAME = ConfigKeys.newStringConfigKey("functionName", "FunctionName");
    public static final ConfigKey<Map<String,Object>> CLIENT_CONTEXT = ConfigKeys.newConfigKey(new TypeToken<Map<String,Object>>() { }, "clientContext", "ClientContext", ImmutableMap.<String,Object>of());
    public static final ConfigKey<InvocationType> INVOCATION_TYPE = ConfigKeys.newConfigKey(InvocationType.class, "invocationType", "InvocationType", InvocationType.RequestResponse);
    public static final ConfigKey<LogType> LOG_TYPE = ConfigKeys.newConfigKey(LogType.class, "logType", "LogType", LogType.None);
    public static final ConfigKey<String> QUALIFIER = ConfigKeys.newStringConfigKey("qualifier", "Qualifier");
    public static final ConfigKey<Duration> TIMEOUT = ConfigKeys.newDurationConfigKey("timeout", "Timeout", Duration.FIVE_MINUTES);

    public static final String PAYLOAD = "payload";

    public static final String AWS_LAMBDA_INVOCATION_TYPE_HEADER = "X-Amz-Invocation-Type";
    public static final String AWS_LAMBDA_LOG_TYPE_HEADER = "X-Amz-Log-Type";
    public static final String AWS_LAMBDA_CLIENT_CONTEXT_HEADER = "X-Amz-Client-Context";

    public AwsLambdaEffector(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public AwsLambdaEffector(Map<String,String> params) {
        this(ConfigBag.newInstance(params));
    }

    public static EffectorBuilder<Map> newEffectorBuilder(ConfigBag params) {
        EffectorBuilder<Map> eff = (EffectorBuilder<Map>) AddEffector.newEffectorBuilder(Map.class, params);
        eff.impl(new Body(eff.buildAbstract(), params));
        eff.parameter(new BasicParameterType("payload", Map.class, "Payload", ImmutableMap.of(), true));
        return eff;
    }

    protected static class Body extends EffectorBody<Map> {
        private final Effector<?> effector;
        private final String functionName;
        private final Map<String, Object> clientContext;
        private final InvocationType invocationType;
        private final LogType logType;
        private final String qualifier;
        private final Duration timeout;

        private String endpoint;
        private AWSEC2ComputeServiceContext context;

        public FormSigner getFormSigner() {
            return context.utils().injector().getInstance(FormSigner.class);
        }

        public Body(Effector<Map> eff, ConfigBag params) {
            this.effector = eff;
            this.functionName = Preconditions.checkNotNull(params.get(FUNCTION_NAME), "The function name must be supplied when defining this effector");
            this.clientContext = params.get(CLIENT_CONTEXT);
            this.invocationType = params.get(INVOCATION_TYPE);
            this.logType = params.get(LOG_TYPE);
            this.qualifier = params.get(QUALIFIER);
            this.timeout = params.get(TIMEOUT);
        }

        @Override
        public Map<String,Object> call(ConfigBag params) {
            Maybe<JcloudsLocation> lookup = AwsLambdaUtils.jcloudsLocation(entity());
            if (lookup.isPresentAndNonNull() && lookup.get().getProvider().equals("aws-ec2")) {
                JcloudsLocation location = lookup.get();
                endpoint = location.getEndpoint();
                context = location.getComputeService().getContext().unwrap();
                Task<Map<String,Object>> task = queue(newTask(params, entity()));
                if (invocationType.equals(InvocationType.Event)) {
                    return null;
                } else {
                    if (task.blockUntilEnded(timeout)) {
                        return task.getUnchecked();
                    } else {
                        throw new IllegalStateException("Lambda function execution timed out: " + functionName);
                    }
                }
            } else {
                throw new IllegalStateException("Must be used in an AWS jclouds location: " + lookup);
            }
        }

        public Task<Map<String,Object>> newTask(ConfigBag params, Entity entity) {
            // Build the payload map
            List<ParameterType<?>> parameters = effector.getParameters();
            Optional<ParameterType<?>> lookup = Iterables.tryFind(parameters, named(PAYLOAD));
            MutableMap<String, Object> payload = lookup.isPresent() ? MutableMap.copyOf((Map) params.get(Effectors.asConfigKey(lookup.get()))) : MutableMap.of();
            for (ParameterType<?> param : Iterables.filter(parameters, Predicates.not(named(PAYLOAD)))) {
                payload.addIfNotNull(param.getName(), params.get(Effectors.asConfigKey(param)));
            }

            // Create the HTTP request
            HttpRequest.Builder builder = HttpRequest.builder()
                    .method("POST")
                    .endpoint(Urls.mergePaths(endpoint, "/2015-03-31/functions", functionName, "invocations"))
                    .addHeader(AWS_LAMBDA_INVOCATION_TYPE_HEADER, invocationType.toString())
                    .addHeader(AWS_LAMBDA_LOG_TYPE_HEADER, logType.toString());

            // Resolve and Base64 encode the client context
            if (clientContext.size() > 0) {
                String clientContextJson = Jsonya.newInstance().add(resolve(clientContext)).toString();
                String clientContextEncoded = Base64.encodeBase64String(clientContextJson.getBytes());
                builder.addHeader(AWS_LAMBDA_CLIENT_CONTEXT_HEADER, clientContextEncoded);
            }

            // Set qualifier
            if (Strings.isNonBlank(qualifier)) {
                builder.addQueryParam("Qualifier", qualifier);
            }

            // JSON encode resolved payload data
            if (payload.size() > 0) {
                String payloadjson = Jsonya.newInstance().add(resolve(payload)).toString();
                builder.payload(payloadjson);
            }
            HttpRequest request = builder.build();

            // Sign the request for invocation
            final HttpRequest signed = getFormSigner().filter(request);

            // Return task for queueing
            return Tasks.<Map<String,Object>>builder().body(new Callable<Map<String,Object>>() {
                @Override
                public Map<String,Object> call() throws Exception {
                    // Invoke the HTTP request
                    HttpResponse response = context.utils().http().invoke(signed);
                    try {
                        String responseJson = Strings2.toStringAndClose(response.getPayload().openStream());
                        Map responseData = context.utils().json().fromJson(responseJson, Map.class);
                        return responseData;
                    } catch (IOException e) {
                        throw Exceptions.propagate(e);
                    } finally {
                        try {
                            response.getPayload().close();
                        } catch (IOException e) {
                            throw Exceptions.propagate(e);
                        }
                    }
                }
            }).build();
        }

        @SuppressWarnings("unchecked")
        private Map<String, Object> resolve(Map<String, Object> map) {
            try {
                return (Map<String, Object>) Tasks.resolveDeepValue(map, Object.class, entity().getExecutionContext());
            } catch (InterruptedException | ExecutionException e) {
                throw Exceptions.propagate(e);
            }
        }
    }

    public static Predicate<ParameterType<?>> named(String name) {
        return new ParameterNamed(name);
    }

    public static class ParameterNamed implements Predicate<ParameterType<?>> {
        private final String name;

        public ParameterNamed(String name) {
            this.name = name;
        }

        @Override
        public boolean apply(ParameterType<?> input) {
            return input.getName().equals(name);
        }
    }
}
