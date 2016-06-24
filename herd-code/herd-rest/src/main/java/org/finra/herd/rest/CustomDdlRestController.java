/*
* Copyright 2015 herd contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.finra.herd.rest;

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.CustomDdl;
import org.finra.herd.model.api.xml.CustomDdlCreateRequest;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.api.xml.CustomDdlKeys;
import org.finra.herd.model.api.xml.CustomDdlUpdateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.CustomDdlService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles custom DDL REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Custom DDL")
public class CustomDdlRestController extends HerdBaseController
{
    public static final String CUSTOM_DDLS_URI_PREFIX = "/customDdls";

    @Autowired
    private CustomDdlService customDdlService;

    /**
     * Creates a new custom DDL.
     * <p>Requires WRITE permission on namespace</p>
     *
     * @param request the information needed to create a custom DDL
     *
     * @return the newly created custom DDL information
     */
    @RequestMapping(value = CUSTOM_DDLS_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_CUSTOM_DDLS_POST)
    public CustomDdl createCustomDdl(@RequestBody CustomDdlCreateRequest request)
    {
        return customDdlService.createCustomDdl(request);
    }

    /**
     * Gets an existing custom DDL.
     * <p>Requires READ permission on namespace</p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param customDdlName the custom DDL name
     *
     * @return the custom DDL information
     */
    @RequestMapping(value = CUSTOM_DDLS_URI_PREFIX + "/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/customDdlNames/{customDdlName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_CUSTOM_DDLS_GET)
    public CustomDdl getCustomDdl(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("customDdlName") String customDdlName)
    {
        return customDdlService.getCustomDdl(
            new CustomDdlKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion,
                customDdlName));
    }

    /**
     * Updates an existing custom DDL.
     * <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param customDdlName the custom DDL name
     * @param request the request information needed to update the custom DDL
     *
     * @return the custom DDL information
     */
    @RequestMapping(value = CUSTOM_DDLS_URI_PREFIX + "/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/customDdlNames/{customDdlName}", method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_CUSTOM_DDLS_PUT)
    public CustomDdl updateCustomDdl(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("customDdlName") String customDdlName,
        @RequestBody CustomDdlUpdateRequest request)
    {
        return customDdlService.updateCustomDdl(
            new CustomDdlKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion,
                customDdlName), request);
    }

    /**
     * Deletes an existing custom DDL.
     * <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param customDdlName the custom DDL name
     *
     * @return the custom DDL that got deleted
     */
    @RequestMapping(value = CUSTOM_DDLS_URI_PREFIX + "/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/customDdlNames/{customDdlName}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_CUSTOM_DDLS_DELETE)
    public CustomDdl deleteCustomDdl(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("customDdlName") String customDdlName)
    {
        return customDdlService.deleteCustomDdl(
            new CustomDdlKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion,
                customDdlName));
    }

    /**
     * Gets a list of keys for all existing custom DDLs for a specific business object format.
     * <p>Requires READ permission on namespace</p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     *
     * @return the list of custom DDL keys
     */
    @RequestMapping(value = CUSTOM_DDLS_URI_PREFIX + "/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_CUSTOM_DDLS_ALL_GET)
    public CustomDdlKeys getCustomDdls(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion)
    {
        return customDdlService.getCustomDdls(
            new BusinessObjectFormatKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion));
    }
}
