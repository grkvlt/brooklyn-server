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


import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.ws.rs.Path;

import io.swagger.annotations.Api;
import io.swagger.jaxrs.listing.ApiListingResource;

import javax.ws.rs.core.*;

import org.apache.brooklyn.rest.apidoc.RestApiResourceScanner;

/**
 * @author Ciprian Ciubotariu <cheepeero@gmx.net>
 */
@Api("API Documentation")
@Path("/apidoc/swagger.{type:json|yaml}")
public class ApidocResource extends ApiListingResource {

    @Context
    ServletContext servletContext;

    private void preprocess(Application app, ServletConfig servletConfig, HttpHeaders headers, UriInfo uriInfo) {
        RestApiResourceScanner.rescanIfNeeded(() -> process(app, servletContext, servletConfig, headers, uriInfo));
    }

    @Override
    public Response getListing(Application app, ServletConfig servletConfig, HttpHeaders headers, UriInfo uriInfo, String type) {
        preprocess(app, servletConfig, headers, uriInfo);
        return super.getListing(app, servletConfig, headers, uriInfo, type);
    }

    @Override
    public Response getListingJsonResponse(Application app, ServletContext servletContext, ServletConfig servletConfig, HttpHeaders headers, UriInfo uriInfo) {
        return super.getListingJsonResponse(app, servletContext, servletConfig, headers, uriInfo);
    }

    @Override
    public Response getListingYamlResponse(Application app, ServletContext servletContext, ServletConfig servletConfig, HttpHeaders headers, UriInfo uriInfo) {
        return super.getListingYamlResponse(app, servletContext, servletConfig, headers, uriInfo);
    }
}
