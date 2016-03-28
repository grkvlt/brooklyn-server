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
package org.apache.brooklyn.location.jclouds.softlayer;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Uninterruptibles;

import org.jclouds.compute.ComputeService;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.softlayer.SoftLayerApi;
import org.jclouds.softlayer.compute.options.SoftLayerTemplateOptions;
import org.jclouds.softlayer.domain.VirtualGuest;
import org.jclouds.softlayer.features.VirtualGuestApi;
import org.jclouds.softlayer.reference.SoftLayerConstants;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.location.jclouds.BasicJcloudsLocationCustomizer;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsLocationCustomizer;
import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.apache.brooklyn.location.jclouds.JcloudsSshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;

/**
 * Customizes {@link JcloudsSshMachineLocation machines} in SoftLayer to use
 * the same VLANs across an application, or other named scope
 * <p>
 * Define the scope by setting the {@link #SCOPE_UID scopeUid} ({@code softlayer.vlan.scopeUid})
 * option. Set {@link #SCOPE_TIMEOUT scopeTimeout} ({@code softlayer.vlan.timeout}) to change
 * the length of time the customizer will wait for VLAN details; normally 20 minutes.
 * <p>
 * The VLAN details are stored as {@link ConfigKey configuration} on
 * the {@link JcloudsLocation location} provisioning the machines, in a map keyed
 * on the scope.
 */
@ThreadSafe
public class SoftLayerSameVlanLocationCustomizer extends BasicJcloudsLocationCustomizer {

    private static final Logger LOG = LoggerFactory.getLogger(SoftLayerSameVlanLocationCustomizer.class);

    @SetFromFlag("scopeUid")
    public static final ConfigKey<String> SCOPE_UID = ConfigKeys.newStringConfigKey(
            "softlayer.vlan.scopeUid",
            "The unique identifier for a Softlayer location scope that will have machines created in the same VLAN");

    @SetFromFlag("scopeTimeout")
    public static final ConfigKey<Duration> SCOPE_TIMEOUT = ConfigKeys.newDurationConfigKey(
            "softlayer.vlan.timeout",
            "The length of time to wait for a machines VLAN details",
            Duration.minutes(20));

    // TODO see PortForwardManager as an example of using a Location for global state

    @SuppressWarnings("serial")
    public static final ConfigKey<Map<String, Integer>> PUBLIC_VLAN_ID_MAP = ConfigKeys.newConfigKey(
            new TypeToken<Map<String, Integer>>() { },
            "softLayerSameVlanLocationCustomizer.map.publicVlanIds",
            "A mapping from scope identifiers to public VLAN numbers");
    @SuppressWarnings("serial")
    public static final ConfigKey<Map<String, Integer>> PRIVATE_VLAN_ID_MAP = ConfigKeys.newConfigKey(
            new TypeToken<Map<String, Integer>>() { },
            "softLayerSameVlanLocationCustomizer.map.privateVlanIds",
            "A mapping from scope identifiers to private VLAN numbers");

    public static final AttributeSensor<Integer> PUBLIC_VLAN_ID = Sensors.newIntegerSensor(
            "softLayer.vlan.publicId", "The public VLAN ID for this entity");
    public static final AttributeSensor<Integer> PRIVATE_VLAN_ID = Sensors.newIntegerSensor(
            "softLayer.vlan.privateId", "The private VLAN ID for this entity");

    /* Global map of scope identifiers to latches. */
    private static final transient Map<String, CountDownLatch> latches = Maps.newConcurrentMap();

    /* Flags passed in on object creation. */
    private final Map<String, ?> flags;

    /* Lock object for global critical sections accessing shared state maps. */
    private static final transient Object lock = new Object[0];

    /** Convenience creation method. */
    public static SoftLayerSameVlanLocationCustomizer forScope(String scopeUid) {
        SoftLayerSameVlanLocationCustomizer customizer = new SoftLayerSameVlanLocationCustomizer(ImmutableMap.of(SCOPE_UID.getName(), scopeUid));
        return customizer;
    }

    public SoftLayerSameVlanLocationCustomizer() {
        this(ImmutableMap.<String, Object>of());
    }

    public SoftLayerSameVlanLocationCustomizer(Map<String, ?> flags) {
        this.flags = ImmutableMap.copyOf(flags);
    }

    /**
     * Update the {@link org.jclouds.compute.options.TemplateOptions} that will
     * be used by {@link JcloudsLocation} to obtain machines. Uses the VLAN
     * numbers configured on an existing machine, as saved in the configuration
     * maps for {@link #PUBLIC_VLAN_ID_MAP public} and {@link #PRIVATE_VLAN_ID_MAP private}
     * VLAN numbers.
     * <p>
     * If no such numbers, this either returns (if no one else is creating) or
     * blocks (waiting on someone else who is creating).
     *
     * @see {@link JcloudsLocationCustomizer#customize(JcloudsLocation, ComputeService, TemplateOptions)}
     */
    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, TemplateOptions templateOptions) {
        // Check we are customising a SoftLayer location
        String provider = location.getProvider();
        if (!(provider.equals(SoftLayerConstants.SOFTLAYER_PROVIDER_NAME) &&
                templateOptions instanceof SoftLayerTemplateOptions)) {
            String message = String.format("Invalid location provider or template options: %s/%s",
                    provider, templateOptions.getClass().getSimpleName());
            LOG.warn(message);
            throw new IllegalArgumentException(message);
        }

        // Check template options for VLAN configuration and return if already set
        String scopeUid = getScopeUid(location);
        SoftLayerTemplateOptions softLayerOptions = (SoftLayerTemplateOptions) templateOptions;
        Integer publicVlanId = softLayerOptions.getPrimaryNetworkComponentNetworkVlanId();
        Integer privateVlanId = softLayerOptions.getPrimaryBackendNetworkComponentNetworkVlanId();
        if (publicVlanId != null && privateVlanId != null) {
            LOG.debug("SoftLayer VLANs private {} and public {} already configured in template options for scope: {}",
                    new Object[] { privateVlanId, publicVlanId, scopeUid });
            return;
        }

        CountDownLatch latch = null;
        synchronized (lock) {
            // Look for the latch for this scope
            latch = lookupCountDownLatch(location, scopeUid);
            if (latch == null) {
                // First, check for saved VLAN details and use if present
                LOG.debug("Looking up saved VLAN details {}", scopeUid);
                publicVlanId = lookupPublicVlanId(location, scopeUid);
                privateVlanId = lookupPrivateVlanId(location, scopeUid);
                if (publicVlanId != null && privateVlanId != null) {
                    // Set VLAN details in template options
                    saveVlanTemplateOptions(scopeUid, softLayerOptions, publicVlanId, privateVlanId);
                } else {
                    // We are the first -- create a latch to block others
                    LOG.debug("Creating new latch for scope: {}", scopeUid);
                    latch = createCountDownLatch(location, scopeUid);
                }
                return;
            }
        }

        // Someone else has the latch, block until VLAN details available.
        Duration timeout = getTimeout(location);
        Tasks.setBlockingDetails("Waiting for VLAN details");
        try {
            LOG.debug("Waiting for VLAN details for scope: {}", scopeUid);
            if (!Uninterruptibles.awaitUninterruptibly(latch, timeout.toMilliseconds(), TimeUnit.MILLISECONDS)) {
                // Timeout -- release the latch to trigger others to unblock;
                // remove the recorded latch so the next fresh attempt will try to create again;
                // and throw because this location is not able to be provisioned with right VLAN details
                latch.countDown();
                removeCountDownLatch(location, scopeUid);
                String message = String.format("Timeout waiting on VLAN details in location %s for scope %s", location, scopeUid);
                LOG.warn(message);
                throw new IllegalStateException(message);
            }
        } finally {
            Tasks.resetBlockingDetails();
        }

        // Looking up saved VLAN details
        LOG.debug("Looking up saved VLAN details {}", scopeUid);
        publicVlanId = lookupPublicVlanId(location, scopeUid);
        privateVlanId = lookupPrivateVlanId(location, scopeUid);
        if (privateVlanId == null && publicVlanId == null) {
            // Saved VLAN IDs not found; something went wrong!
            // throw because this location is not able to be provisioned with right VLAN details
            // TODO if either public or private is set, we will live with that (not sure if that happens and if so what it would mean...)
            String message = String.format("Saved VLAN configuration not available in location %s for scope %s", location, scopeUid);
            LOG.warn(message);
            throw new IllegalArgumentException(message);
        }

        saveVlanTemplateOptions(scopeUid, softLayerOptions, publicVlanId, privateVlanId);
    }

    private void saveVlanTemplateOptions(String scopeUid, SoftLayerTemplateOptions softLayerOptions, Integer publicVlanId, Integer privateVlanId) {
        LOG.debug("Setting VLAN template options private {} and public {} for scope: {}",
            new Object[] { privateVlanId, publicVlanId, scopeUid });
        softLayerOptions.primaryNetworkComponentNetworkVlanId(publicVlanId);
        softLayerOptions.primaryBackendNetworkComponentNetworkVlanId(privateVlanId);
    }

    /**
     * After provisioning, if it's the first created {@link JcloudsMachineLocation machine}
     * then get and cache the VLAN details.
     *
     * @see {@link JcloudsLocationCustomizer#customize(JcloudsLocation, ComputeService, JcloudsMachineLocation)}
     */
    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, JcloudsMachineLocation machine) {
        // Check we are customising a SoftLayer location
        String provider = location.getProvider();
        if (!(provider.equals(SoftLayerConstants.SOFTLAYER_PROVIDER_NAME))) {
            String message = String.format("Invalid location provider: %s", provider);
            LOG.warn(message);
            throw new IllegalArgumentException(message);
        }

        // Lookup the latch for this scope
        String scopeUid = getScopeUid(location);
        CountDownLatch latch = lookupCountDownLatch(location, scopeUid);
        if (latch == null) {
            throw new IllegalStateException("No latch available for scope: " + scopeUid);
        }

        try {
            // Check if VLAN details have been set for this scope
            LOG.debug("Looking up saved VLAN details {}", scopeUid);
            Integer publicVlanId = lookupPublicVlanId(location, scopeUid);
            Integer privateVlanId = lookupPrivateVlanId(location, scopeUid);
            if (privateVlanId != null && publicVlanId != null) {
                LOG.warn("SoftLayer VLANs private {} and public {} already configured for location {} scope {}",
                        new Object[] { privateVlanId, publicVlanId, location, scopeUid });
                recordVlanDetails(machine, scopeUid, privateVlanId, publicVlanId);
                // TODO if there were a failure and race this machine might have different details to what we've recorded
                return;
            }

            // No VLAN details yet known, we are probably the first machine and
            // we should set the VLAN details for others to then learn about.

            // Ask SoftLayer API for the VLAN details for this machine
            LOG.debug("Requesting VLAN details from API for scope: {}", scopeUid);
            VirtualGuestApi api = computeService.getContext().unwrapApi(SoftLayerApi.class).getVirtualGuestApi();
            Long serverId = Long.parseLong(machine.getJcloudsId());
            VirtualGuest guest = api.getVirtualGuestFiltered(serverId,
                    "primaryNetworkComponent;" +
                    "primaryNetworkComponent.networkVlan;" +
                    "primaryBackendNetworkComponent;" +
                    "primaryBackendNetworkComponent.networkVlan");
            publicVlanId = guest.getPrimaryNetworkComponent().getNetworkVlan().getId();
            privateVlanId = guest.getPrimaryBackendNetworkComponent().getNetworkVlan().getId();

            // Save the VLAN details here and on the entity and machine
            LOG.debug("Saving VLAN details private {} and public {} for location {} scope {}",
                    new Object[] { privateVlanId, publicVlanId, location, scopeUid });
            savePublicVlanId(location, scopeUid, publicVlanId);
            savePrivateVlanId(location, scopeUid, privateVlanId);
            recordVlanDetails(machine, scopeUid, privateVlanId, publicVlanId);
        } finally {
            // Release the latch
            latch.countDown();
            removeCountDownLatch(location, scopeUid);
        }
    }

    /* Record the VLAN details as sensor data on the entity and set tag on entity and machine. */
    private void recordVlanDetails(JcloudsMachineLocation machine, String scopeUid, Integer privateVlanId, Integer publicVlanId) {
        Object context = flags.get(LocationConfigKeys.CALLER_CONTEXT.getName());
        if (context == null) {
            context = machine.config().get(LocationConfigKeys.CALLER_CONTEXT);
        }
        if (!(context instanceof Entity)) {
            throw new IllegalStateException("Invalid location context: " + context);
        }
        Entity entity = (Entity) context;
        entity.sensors().set(PUBLIC_VLAN_ID, publicVlanId);
        entity.sensors().set(PRIVATE_VLAN_ID, privateVlanId);
        String tag = "softlayer-vlan-scopeUid-" + scopeUid;
        entity.tags().addTag(tag);
        machine.tags().addTag(tag);
    }


    /**
     * Get the {@link #SCOPE_TIMEOUT timeout} {@link Duration duration} from the
     * location flags, or the location itself.
     */
    private Duration getTimeout(JcloudsLocation location) {
        Duration timeout = (Duration) flags.get(SCOPE_TIMEOUT.getName());
        if (timeout == null) {
            timeout = location.config().get(SCOPE_TIMEOUT);
        }
        return timeout;
    }

    /**
     * Get the {@link #SCOPE_UID scope} UID from the location flags, or the
     * location itself.
     */
    private String getScopeUid(JcloudsLocation location) {
        String scopeUid = (String) flags.get(SCOPE_UID.getName());
        if (Strings.isEmpty(scopeUid)) {
            scopeUid = location.config().get(SCOPE_UID);
        }
        return Preconditions.checkNotNull(scopeUid, "scopeUid");
    }

    /**
     * Look up the {@link CountDownLatch} object for a scope.
     *
     * @return {@code null} if the latch has not been created yet
     */
    protected CountDownLatch lookupCountDownLatch(JcloudsLocation location, String scopeUid) {
        synchronized (lock) {
            return latches.get(scopeUid);
        }
    }

    /**
     * Create a new {@link CountDownLatch} for a scope.
     *
     * @return The latch for the scope that is stored in the map.
     */
    protected CountDownLatch createCountDownLatch(JcloudsLocation location, String scopeUid) {
        synchronized (lock) {
            if (!latches.containsKey(scopeUid)) {
                latches.put(scopeUid, new CountDownLatch(1));
            }
            return latches.get(scopeUid);
        }
    }

    /**
     * Remove the {@link CountDownLatch} object for a scope.
     */
    protected void removeCountDownLatch(JcloudsLocation location, String scopeUid) {
        synchronized (lock) {
            latches.remove(scopeUid);
        }
    }

    /** Return the public VLAN number for a scope. */
    protected Integer lookupPublicVlanId(JcloudsLocation location, String scopeUid) {
        synchronized (lock) {
            Map<String, Integer> map = location.config().get(PUBLIC_VLAN_ID_MAP);
            if (map == null) {
                map = MutableMap.copyOf(map);
                saveAndPersist(location, PUBLIC_VLAN_ID_MAP, map);
            }
            return map.get(scopeUid);
        }
    }

    /** Save the public VLAN number for a scope. */
    protected void savePublicVlanId(JcloudsLocation location, String scopeUid, Integer publicVlanId) {
        synchronized (lock) {
            Map<String, Integer> map = MutableMap.copyOf(location.config().get(PUBLIC_VLAN_ID_MAP));

            map.put(scopeUid, publicVlanId);

            saveAndPersist(location, PUBLIC_VLAN_ID_MAP, map);
        }
    }

    /** Return the private VLAN number for a scope. */
    protected Integer lookupPrivateVlanId(JcloudsLocation location, String scopeUid) {
        synchronized (lock) {
            Map<String, Integer> map = location.config().get(PRIVATE_VLAN_ID_MAP);
            if (map == null) {
                map = MutableMap.copyOf(map);
                saveAndPersist(location, PRIVATE_VLAN_ID_MAP, map);
            }
            return map.get(scopeUid);
        }
    }

    /** Save the private VLAN number for a scope. */
    protected void savePrivateVlanId(JcloudsLocation location, String scopeUid, Integer privateVlanId) {
        synchronized (lock) {
            Map<String, Integer> map = MutableMap.copyOf(location.config().get(PRIVATE_VLAN_ID_MAP));

            map.put(scopeUid, privateVlanId);

            saveAndPersist(location, PRIVATE_VLAN_ID_MAP, map);
        }
    }

    private <T> void saveAndPersist(JcloudsLocation location, ConfigKey<Map<String, T>> key, Map<String, T> map) {
        location.config().set(key, ImmutableMap.copyOf(map));
        location.getManagementContext().getRebindManager().forcePersistNow(false, null);
    }

}