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
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatAttributeDefinitionsUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatAttributesUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdl;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKeys;
import org.finra.herd.model.api.xml.BusinessObjectFormatParentsUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatRetentionInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatSchemaBackwardsCompatibilityUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatUpdateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.BusinessObjectFormatService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles business object format REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Business Object Format")
public class BusinessObjectFormatRestController extends HerdBaseController
{
    @Autowired
    private BusinessObjectFormatService businessObjectFormatService;

    /**
     * Creates a new business object format. <p>Requires WRITE permission on namespace</p>
     *
     * @param request the information needed to create the business object format.
     *
     * @return the created business object format.
     */
    @RequestMapping(value = "/businessObjectFormats", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_FORMATS_POST)
    public BusinessObjectFormat createBusinessObjectFormat(@RequestBody BusinessObjectFormatCreateRequest request)
    {
        return businessObjectFormatService.createBusinessObjectFormat(request);
    }

    /**
     * Updates an existing business object format by alternate key. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param request the information needed to update the business object format
     *
     * @return the updated business object format.
     */
    @RequestMapping(value = "/businessObjectFormats/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}", method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_FORMATS_PUT)
    public BusinessObjectFormat updateBusinessObjectFormat(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @RequestBody BusinessObjectFormatUpdateRequest request)
    {
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion);
        return businessObjectFormatService.updateBusinessObjectFormat(businessObjectFormatKey, request);
    }

    /**
     * Gets an existing business object format by alternate key.
     *
     * @param namespace the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     *
     * @return the retrieved business object format.
     */
    @RequestMapping(value = "/businessObjectFormats/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_FORMATS_GET)
    public BusinessObjectFormat getBusinessObjectFormat(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @RequestParam(value = "businessObjectFormatVersion", required = false) Integer businessObjectFormatVersion)
    {
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion);
        return businessObjectFormatService.getBusinessObjectFormat(businessObjectFormatKey);
    }

    /**
     * Deletes an existing business format. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     *
     * @return the business object format that was deleted
     */
    @RequestMapping(value = "/businessObjectFormats/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_FORMATS_DELETE)
    public BusinessObjectFormat deleteBusinessObjectFormat(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion)
    {
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion);
        return businessObjectFormatService.deleteBusinessObjectFormat(businessObjectFormatKey);
    }

    /**
     * Gets a list of business object formats for the specified business object definition name.
     *
     * @param namespace the namespace code
     * @param businessObjectDefinitionName the business object definition name
     *
     * @return the list of business object formats.
     */
    @RequestMapping(value = "/businessObjectFormats/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_FORMATS_ALL_GET)
    public BusinessObjectFormatKeys getBusinessObjectFormats(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @RequestParam(value = "latestBusinessObjectFormatVersion", required = false, defaultValue = "false") Boolean latestBusinessObjectFormatVersion)
    {
        return businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(namespace, businessObjectDefinitionName),
            BooleanUtils.isTrue(latestBusinessObjectFormatVersion));
    }

    /**
     * Gets a list of business object formats for the specified business object definition name and business object format usage.
     *
     * @param namespace the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param latestBusinessObjectFormatVersion latest business object format version
     *
     * @return the list of business object formats.
     */
    @RequestMapping(value = "/businessObjectFormats/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}/" +
        "businessObjectFormatUsages/{businessObjectFormatUsage}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_FORMATS_ALL_GET)
    public BusinessObjectFormatKeys getBusinessObjectFormatsWithFilters(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @RequestParam(value = "latestBusinessObjectFormatVersion", required = false, defaultValue = "false") Boolean latestBusinessObjectFormatVersion)
    {
        return businessObjectFormatService
            .getBusinessObjectFormatsWithFilters(new BusinessObjectDefinitionKey(namespace, businessObjectDefinitionName), businessObjectFormatUsage,
                BooleanUtils.isTrue(latestBusinessObjectFormatVersion));
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system (e.g. Hive) by creating a table for the requested business object format.
     *
     * @param businessObjectFormatDdlRequest the business object data DDL request
     *
     * @return the business object data DDL information
     */
    @RequestMapping(value = "/businessObjectFormats/generateDdl", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_FORMATS_GENERATE_DDL_POST)
    public BusinessObjectFormatDdl generateBusinessObjectFormatDdl(@RequestBody BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest)
    {
        return businessObjectFormatService.generateBusinessObjectFormatDdl(businessObjectFormatDdlRequest);
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system (e.g. Hive) by creating tables for a collection of business object formats.
     * <p>Requires READ permission on ALL namespaces</p>
     *
     * @param businessObjectFormatDdlCollectionRequest the business object format DDL collection request
     *
     * @return the business object format DDL information
     */
    @RequestMapping(value = "/businessObjectFormats/generateDdlCollection", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_FORMATS_GENERATE_DDL_COLLECTION_POST)
    public BusinessObjectFormatDdlCollectionResponse generateBusinessObjectFormatDdlCollection(
        @RequestBody BusinessObjectFormatDdlCollectionRequest businessObjectFormatDdlCollectionRequest)
    {
        return businessObjectFormatService.generateBusinessObjectFormatDdlCollection(businessObjectFormatDdlCollectionRequest);
    }

    /**
     * Updates an existing business object format parents by alternate key. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param request the information needed to update the business object format
     *
     * @return the updated business object format.
     */
    @RequestMapping(value = "/businessObjectFormatParents/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_FORMAT_PARENTS_PUT)
    public BusinessObjectFormat updateBusinessObjectFormatParents(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType, @RequestBody BusinessObjectFormatParentsUpdateRequest request)
    {
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType, null);
        return businessObjectFormatService.updateBusinessObjectFormatParents(businessObjectFormatKey, request);
    }

    /**
     * Updates an existing business object format attributes by alternate key. <p>Requires WRITE permission on namespace</p>
     *
     * <p>
     * This endpoint replaces the entire list of attributes on the business object format with the contents of the request. Observe this example:
     *   <ol>
     *       <li>Three attributes present on the existing business object format.</li>
     *       <li>This endpoint is called with a single attribute in the request with an updated value.</li>
     *       <li>After this operation the business object format will have only one attribute – which is probably not the desired outcome.</li>
     *       <li>Instead, supply all existing attributes and provide updated values and additional attributes as needed.
     *       The only case when an existing attribute should be left out is to remove the attribute.</li>
     *   </ol>
     * </p>
     *
     * @param namespace the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param request the information needed to update the business object format attributes
     *
     * @return the updated business object format.
     */
    @RequestMapping(value = "/businessObjectFormatAttributes/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}", method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_FORMAT_ATTRIBUTES_PUT)
    public BusinessObjectFormat updateBusinessObjectFormatAttributes(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @RequestBody BusinessObjectFormatAttributesUpdateRequest request)
    {
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion);
        return businessObjectFormatService.updateBusinessObjectFormatAttributes(businessObjectFormatKey, request);
    }

    /**
     * Replaces the list of attribute definitions for an existing business object format based on the specified usage, file type, version and a business object
     * definition. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param request the information needed to update the business object format attribute definitions
     *
     * @return the updated business object format attribute definitions.
     */
    @RequestMapping(value = "/businessObjectFormatAttributeDefinitions/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}", method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_FORMAT_ATTRIBUTE_DEFINITIONS_PUT)
    public BusinessObjectFormat updateBusinessObjectFormatAttributeDefinitions(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion,
        @RequestBody BusinessObjectFormatAttributeDefinitionsUpdateRequest request)
    {
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion);
        return businessObjectFormatService.updateBusinessObjectFormatAttributeDefinitions(businessObjectFormatKey, request);
    }

    /**
     * Updates an existing business object format retention information by alternate key. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param request the information needed to update the business object format retention information
     *
     * @return the updated business object format.
     */
    @RequestMapping(value = "/businessObjectFormatRetentionInformation/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_FORMAT_RETENTION_INFORMATION_PUT)
    public BusinessObjectFormat updateBusinessObjectFormatRetentionInformation(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @RequestBody BusinessObjectFormatRetentionInformationUpdateRequest request)
    {
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType, null);
        return businessObjectFormatService.updateBusinessObjectFormatRetentionInformation(businessObjectFormatKey, request);
    }

    /**
     * Updates an existing Business Object Format to allow non-backwards compatibility changes to the format schema based on the
     * 'allowNonBackwardsCompatibleChanges' flag. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param request the information needed to update the business object format to allow non-backwards compatibility changes to the format schema
     *
     * @return the updated business object format.
     */
    @RequestMapping(value = "/businessObjectFormatSchemaBackwardsCompatibility/namespaces/{namespace}/businessObjectDefinitionNames/" +
        "{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_FORMAT_SCHEMA_BACKWARDS_COMPATIBILITY_PUT)
    public BusinessObjectFormat updateBusinessObjectFormatSchemaBackwardsCompatibleChanges(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @RequestBody BusinessObjectFormatSchemaBackwardsCompatibilityUpdateRequest request)
    {
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType, null);
        return businessObjectFormatService.updateBusinessObjectFormatSchemaBackwardsCompatibilityChanges(businessObjectFormatKey, request);
    }
}
