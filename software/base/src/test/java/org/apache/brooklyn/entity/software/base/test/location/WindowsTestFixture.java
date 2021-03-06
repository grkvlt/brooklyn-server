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
package org.apache.brooklyn.entity.software.base.test.location;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class WindowsTestFixture {

    /** Can be configured as `user:pass@host` to allow use of a pre-existing fixed winrm target;
     * if not, will provision in AWS */
    public static final String EXISTING_WINDOWS_TEST_USER_PASS_HOST_ENV_VAR = "EXISTING_WINDOWS_TEST_USER_PASS_HOST_ENV_VAR";
    
    public static MachineProvisioningLocation<WinRmMachineLocation> setUpWindowsLocation(ManagementContext mgmt) throws Exception {
        return setUpWindowsLocation(mgmt, ImmutableMap.<String, Object>of());
    }
    
    @SuppressWarnings("unchecked")
    public static MachineProvisioningLocation<WinRmMachineLocation> setUpWindowsLocation(ManagementContext mgmt, Map<String, ?> props) throws Exception {
        // option to set a pre-existing Windows VM for use in a bunch of different tests.
        // or leave blank, run once, interrupt and retrieve the user:pass@host for use subsequently
        // (but if you do that don't forget to delete it!)
        String userPassAtHost = System.getenv(EXISTING_WINDOWS_TEST_USER_PASS_HOST_ENV_VAR);
        if (Strings.isBlank(userPassAtHost)) {
            return (MachineProvisioningLocation<WinRmMachineLocation>) newJcloudsLocation((ManagementContextInternal) mgmt, props);
        } else {
            return (MachineProvisioningLocation<WinRmMachineLocation>) newByonLocation((ManagementContextInternal) mgmt,
                MutableMap.of(
                        "winrm", userPassAtHost.split("@")[1],
                        "password", userPassAtHost.split(":")[1].split("@")[0],
                        "user", userPassAtHost.split(":")[0]
                    ));
        }
    }
    
    private static MachineProvisioningLocation<?> newJcloudsLocation(ManagementContextInternal mgmt, Map<String, ?> props) {
        // Requires no userMetadata to be set, so that we use WinRmMachineLocation.getDefaultUserMetadataString()
        mgmt.getBrooklynProperties().remove("brooklyn.location.jclouds.aws-ec2.userMetadata");
        mgmt.getBrooklynProperties().remove("brooklyn.location.jclouds.userMetadata");
        mgmt.getBrooklynProperties().remove("brooklyn.location.userMetadata");
        
        return (JcloudsLocation) mgmt.getLocationRegistry().getLocationManaged("jclouds:aws-ec2:us-west-2", MutableMap.<String, Object>builder()
                .put("inboundPorts", ImmutableList.of(5985, 3389))
                .put("displayName", "AWS Oregon (Windows)")
                .put("imageOwner", "801119661308")
                .put("imageNameRegex", "Windows_Server-2012-R2_RTM-English-64Bit-Base-.*")
                .put("hardwareId", "m3.medium")
                .put("useJcloudsSshInit", false)
                .putAll(props)
                .build());
    }
    
    private static MachineProvisioningLocation<?> newByonLocation(ManagementContextInternal mgmt, Map<String, ?> props) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("useJcloudsSshInit", "false");
        config.put("byonIdentity", "123");
        config.put("osFamily", "windows");
        // these are overwritten by the map
        config.put("winrm", "52.12.211.123:5985");
        config.put("user", "Administrator");
        config.put("password", "pa55w0rd");
        config.putAll(props);
        
        return (MachineProvisioningLocation<?>) mgmt.getLocationRegistry().getLocationManaged("byon", ImmutableMap.of("hosts", ImmutableList.of(config)));
    }
}
