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
package org.apache.brooklyn.rest.util.json;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.server.BrooklynServiceAttributes;
import org.apache.brooklyn.rest.util.OsgiCompat;
import org.apache.brooklyn.util.core.json.BrooklynObjectsJsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

@Provider
public class BrooklynJacksonJsonProvider extends JacksonJsonProvider implements
        //CXF only looks at the interfaces of this class to determine if the Provider is a MessageBodyWriter/Reader
        MessageBodyWriter<Object>, MessageBodyReader<Object> {

    private static final Logger log = LoggerFactory.getLogger(BrooklynJacksonJsonProvider.class);

    public static final String BROOKLYN_REST_OBJECT_MAPPER = BrooklynServiceAttributes.BROOKLYN_REST_OBJECT_MAPPER;

    protected ObjectMapper ourMapper;
    protected boolean notFound = false;

    @Context
    private ContextResolver<ManagementContext> mgmt;

    @Override
    public ObjectMapper locateMapper(Class<?> type, MediaType mediaType) {
        if (ourMapper != null)
            return ourMapper;

        findSharedMapper();

        if (ourMapper != null)
            return ourMapper;

        if (!notFound) {
            log.warn("Management context not available; using default ObjectMapper in "+this);
            notFound = true;
        }

        return super.locateMapper(Object.class, MediaType.APPLICATION_JSON_TYPE);
    }

    protected synchronized ObjectMapper findSharedMapper() {
        if (ourMapper != null || notFound)
            return ourMapper;

        ourMapper = findSharedObjectMapper(mgmt());
        if (ourMapper == null) return null;

        if (notFound) {
            notFound = false;
        }
        log.debug("Found mapper "+ourMapper+" for "+this+", creating custom Brooklyn mapper");

        return ourMapper;
    }

    private ManagementContext mgmt() {
        return mgmt.getContext(ManagementContext.class);
    }

    /** @deprecated since 0.9.0, use {@link #findSharedObjectMapper(ManagementContext)} */
    @Deprecated
    public static ObjectMapper findSharedObjectMapper(ServletContext servletContext, ManagementContext mgmt) {
        return findSharedObjectMapper(getManagementContext(servletContext, mgmt));
    }

    @Deprecated
    private static ManagementContext getManagementContext(ServletContext servletContext, ManagementContext mgmt) {
        if (mgmt != null) {
            return mgmt;
        } else {
            return OsgiCompat.getManagementContext(servletContext);
        }
    }

    /**
     * Finds a shared {@link ObjectMapper} or makes a new one, stored against the servlet context;
     * returns null if a shared instance cannot be created.
     */
    public static ObjectMapper findSharedObjectMapper(ManagementContext mgmt) {
        checkNotNull(mgmt, "mgmt");
        synchronized (mgmt) {
            ConfigKey<ObjectMapper> key = ConfigKeys.newConfigKey(ObjectMapper.class, BROOKLYN_REST_OBJECT_MAPPER);
            ObjectMapper mapper = (ObjectMapper) mgmt.getScratchpad().get(key);
            if (mapper != null) return mapper;

            mapper = newPrivateObjectMapper(mgmt);
            log.debug("Storing new ObjectMapper against "+mgmt+" because no ServletContext available: "+mapper);
            mgmt.getScratchpad().put(key, mapper);
            return mapper;
        }
    }

    /** @deprecated since 0.9.0, use {@link #findAnyObjectMapper(ManagementContext) */
    @Deprecated
    public static ObjectMapper findAnyObjectMapper(ServletContext servletContext, ManagementContext mgmt) {
        return findAnyObjectMapper(getManagementContext(servletContext, mgmt));
    }

    /**
     * Like {@link #findSharedObjectMapper(ServletContext, ManagementContext)} but will create a private
     * ObjectMapper if it can, from the servlet context and/or the management context, or else fail
     */
    public static ObjectMapper findAnyObjectMapper(ManagementContext mgmt) {
        ObjectMapper mapper = findSharedObjectMapper(mgmt);
        if (mapper != null) return mapper;

        return newPrivateObjectMapper(mgmt);
    }

    /**
     * @return A new Brooklyn-specific ObjectMapper.
     *   Normally {@link #findSharedObjectMapper(ManagementContext)} is preferred
     */
    public static ObjectMapper newPrivateObjectMapper(ManagementContext mgmt) {
        if (mgmt == null) {
            throw new IllegalStateException("No management context available for creating ObjectMapper");
        }

        return BrooklynObjectsJsonMapper.newMapper(mgmt);
    }

}
