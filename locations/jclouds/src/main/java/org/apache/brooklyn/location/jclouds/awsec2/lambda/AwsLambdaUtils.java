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

import static org.jclouds.aws.reference.FormParameters.ACTION;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.inject.Named;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent.Scope;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.jclouds.Fallbacks.EmptySetOnNotFoundOr404;
import org.jclouds.aws.ec2.AWSEC2Api;
import org.jclouds.aws.ec2.AWSEC2ApiMetadata;
import org.jclouds.aws.ec2.compute.AWSEC2ComputeServiceContext;
import org.jclouds.aws.ec2.domain.AWSRunningInstance;
import org.jclouds.aws.ec2.xml.AWSDescribeInstancesResponseHandler;
import org.jclouds.aws.ec2.xml.AWSRunInstancesResponseHandler;
import org.jclouds.aws.filters.FormSigner;
import org.jclouds.compute.ComputeService;
import org.jclouds.ec2.binders.BindFiltersToIndexedFormParams;
import org.jclouds.ec2.binders.BindInstanceIdsToIndexedFormParams;
import org.jclouds.ec2.binders.IfNotNullBindAvailabilityZoneToFormParam;
import org.jclouds.ec2.domain.Reservation;
import org.jclouds.ec2.features.InstanceApi;
import org.jclouds.ec2.options.RunInstancesOptions;
import org.jclouds.http.HttpRequest;
import org.jclouds.javax.annotation.Nullable;
import org.jclouds.location.functions.RegionToEndpointOrProviderIfNull;
import org.jclouds.providers.Providers.ApiMetadataFunction;
import org.jclouds.rest.annotations.BinderParam;
import org.jclouds.rest.annotations.EndpointParam;
import org.jclouds.rest.annotations.Fallback;
import org.jclouds.rest.annotations.FormParams;
import org.jclouds.rest.annotations.RequestFilters;
import org.jclouds.rest.annotations.VirtualHost;
import org.jclouds.rest.annotations.XMLResponseParser;

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.reflect.TypeToken;

public final class AwsLambdaUtils extends AddEffector {

    public static final ConfigKey<String> FUNCTION_NAME = ConfigKeys.newStringConfigKey("functionName");
    public static final ConfigKey<Map<String,Object>> CLIENT_CONTEXT = ConfigKeys.newConfigKey(new TypeToken<Map<String,Object>>() { }, "clientContext");
    public static final ConfigKey<String> INVOCATION_TYPE = ConfigKeys.newStringConfigKey("invocationType");
    public static final ConfigKey<String> LOG_TYPE = ConfigKeys.newStringConfigKey("logType");
    public static final ConfigKey<String> QUALIFIER = ConfigKeys.newStringConfigKey("qualifier");

    public enum InvocationType {
        Event,
        RequestResponse,
        DryRun
    }

    public enum LogType {
        None,
        Tail
    }


    public static enum Scope {
        GLOBAL,
        CHILD,
        PARENT,
        SIBLING,
        DESCENDANT,
        ANCESTOR,
        ROOT,
        SCOPE_ROOT,
        THIS;

        private static Converter<String, String> converter = CaseFormat.LOWER_CAMEL.converterTo(CaseFormat.UPPER_UNDERSCORE);

        public static Scope fromString(String name) {
            Maybe<Scope> parsed = tryFromString(name);
            return parsed.get();
        }

        public static Maybe<Scope> tryFromString(String name) {
            try {
                Scope scope = valueOf(converter.convert(name));
                return Maybe.of(scope);
            } catch (Exception cause) {
                return Maybe.absent(cause);
            }
        }

        public static boolean isValid(String name) {
            Maybe<Scope> check = tryFromString(name);
            return check.isPresentAndNonNull();
        }

        @Override
        public String toString() {
            return converter.reverse().convert(name());
        }
    }    
    public AwsLambdaUtils(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public AwsLambdaUtils(Map<String,String> params) {
        this(ConfigBag.newInstance(params));
    }

    public static EffectorBuilder<Object> newEffectorBuilder(ConfigBag params) {
        EffectorBuilder<Object> eff = AddEffector.newEffectorBuilder(Object.class, params);
        eff.impl(new Body(eff.buildAbstract(), params));
        return eff;
    }

    protected static class Body extends EffectorBody<Object> {
        private final Effector<?> effector;
        private final String functionName;
        private final Map<String, Object> clientContext;
        private final String invocationType;
        private final String logType;
        private final String qualifier;

        private AWSEC2ComputeServiceContext context;
        private AWSEC2ApiMetadata metadata;

        public Body(Effector<Object> eff, ConfigBag params) {
            this.effector = eff;
            this.functionName = Preconditions.checkNotNull(params.get(FUNCTION_NAME), "The function name must be supplied when defining this effector");
            this.clientContext = params.get(CLIENT_CONTEXT);
            this.invocationType = params.get(INVOCATION_TYPE);
            this.logType = params.get(LOG_TYPE);
            this.qualifier = params.get(QUALIFIER);
        }

        @Override
        public Object call(ConfigBag params) {
            Maybe<JcloudsLocation> lookup = jcloudsLocation(entity());
            if (lookup.isPresentAndNonNull() && lookup.get().getProvider().equals("aws-ec2")) {
                JcloudsLocation location = lookup.get();
                ComputeService compute = location.getComputeService();
                context = compute.getContext().unwrap();
                metadata = context.unwrap();
                return null;
            } else {
                throw new IllegalStateException("Must be used in an AWS jclouds location: " + location);
            }
        }

        public FormSigner getFormSigner() {
            return context.utils().injector().getInstance(FormSigner.class);
        }

        public Task<Object> task(ConfigBag params, Entity entity) {
            /*
                POST /2015-03-31/functions/{FunctionName}/invocations?Qualifier={Qualifier} HTTP/1.1
                X-Amz-Invocation-Type: {InvocationType}
                X-Amz-Log-Type: {LogType}
                X-Amz-Client-Context: {ClientContext}
                
                {Payload}
             */
            HttpRequest request = HttpRequest.builder()
                    .method("POST")
                    .endpoint(metadata.getDefaultEndpoint().get())
                    .addHeader("X-Amz-Invocation-Type", invocationType)
                    .addHeader("X-Amz-Log-Type", logType)
                    .addHeader("X-Amz-Client-Context", clientContext.toString())
                    .addQueryParam("Qualifier", qualifier)
                    .payload(payload)
                    .build();
            getFormSigner().filter(request);
            
            return null;
        }

        @SuppressWarnings("unchecked")
        private Map<String, Object> resolve(MutableMap<String, Object> map) throws ExecutionException, InterruptedException {
            return (Map<String, Object>) Tasks.resolveDeepValue(map, Object.class, entity().getExecutionContext());
        }
    }

    /** Determine jclouds provisioning location */
    public static Maybe<JcloudsLocation> jcloudsLocation(Entity entity) {
        Collection<? extends Location> locations = entity.getLocations();
        JcloudsLocation provisioner = null;
        Maybe<? extends Location> location = Machines.findUniqueElement(locations, JcloudsLocation.class);
        if (location.isPresent()) {
            provisioner = (JcloudsLocation) location.get();
        } else {
            location = Machines.findUniqueElement(locations, JcloudsMachineLocation.class);
            if (location.isPresent()) {
                Location parent = location.get().getParent();
                while (parent != null && ! (parent instanceof JcloudsLocation)) {
                    parent = parent.getParent();
                }
                provisioner = (JcloudsLocation) parent;
            }
        }
        if (provisioner == null) {
            return Maybe.absent(new IllegalStateException("Cannot determine jclouds location for entity: " + entity));
        }
        return Maybe.of(provisioner);
    }
}
