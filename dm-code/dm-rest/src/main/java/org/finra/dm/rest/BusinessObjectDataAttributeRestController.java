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
package org.finra.dm.rest;

import java.util.ArrayList;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.dm.model.dto.SecurityFunctions;
import org.finra.dm.model.api.xml.BusinessObjectDataAttribute;
import org.finra.dm.model.api.xml.BusinessObjectDataAttributeCreateRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.dm.model.api.xml.BusinessObjectDataAttributeKeys;
import org.finra.dm.model.api.xml.BusinessObjectDataAttributeUpdateRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.service.BusinessObjectDataAttributeService;
import org.finra.dm.ui.constants.UiConstants;

/**
 * The REST controller that handles business object data attribute requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
public class BusinessObjectDataAttributeRestController extends DmBaseController
{
    public static final String BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX = "/businessObjectDataAttributes";

    @Autowired
    private BusinessObjectDataAttributeService businessObjectDataAttributeService;

    /**
     * Creates a new business object data attribute.
     *
     * @param request the information needed to create a business object data attribute
     *
     * @return the newly created business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_POST)
    public BusinessObjectDataAttribute createBusinessObjectDataAttribute(@RequestBody BusinessObjectDataAttributeCreateRequest request)
    {
        return businessObjectDataAttributeService.createBusinessObjectDataAttribute(request);
    }

    /**
     * Gets an existing attribute for the business object data without subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}" +
        "/businessObjectDataAttributeNames/{businessObjectDataAttributeName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_GET)
    public BusinessObjectDataAttribute getBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, new ArrayList<String>(), businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Gets an existing attribute for the business object data with 1 subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_GET)
    public BusinessObjectDataAttribute getBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value), businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Gets an existing attribute for the business object data with 2 subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_GET)
    public BusinessObjectDataAttribute getBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value), businessObjectDataVersion,
                businessObjectDataAttributeName));
    }

    /**
     * Gets an existing attribute for the business object data with 3 subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_GET)
    public BusinessObjectDataAttribute getBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value),
                businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Gets an existing attribute for the business object data with 4 subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param subPartition4Value the 4th subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}/subPartition4Values/{subPartition4Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_GET)
    public BusinessObjectDataAttribute getBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("subPartition4Value") String subPartition4Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value, subPartition4Value),
                businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Gets an existing attribute for the business object data without subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}" +
        "/businessObjectDataAttributeNames/{businessObjectDataAttributeName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_GET)
    public BusinessObjectDataAttribute getBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, new ArrayList<String>(), businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Gets an existing attribute for the business object data with 1 subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_GET)
    public BusinessObjectDataAttribute getBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value), businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Gets an existing attribute for the business object data with 2 subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_GET)
    public BusinessObjectDataAttribute getBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value), businessObjectDataVersion,
                businessObjectDataAttributeName));
    }

    /**
     * Gets an existing attribute for the business object data with 3 subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_GET)
    public BusinessObjectDataAttribute getBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value),
                businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Gets an existing attribute for the business object data with 4 subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param subPartition4Value the 4th subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}/subPartition4Values/{subPartition4Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_GET)
    public BusinessObjectDataAttribute getBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("subPartition4Value") String subPartition4Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value, subPartition4Value),
                businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Updates an existing attribute for the business object data without subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param request the request information needed to update the business object data attribute
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}" +
        "/businessObjectDataAttributeNames/{businessObjectDataAttributeName}", method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_PUT)
    public BusinessObjectDataAttribute updateBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName, @RequestBody BusinessObjectDataAttributeUpdateRequest request)
    {
        return businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, new ArrayList<String>(), businessObjectDataVersion, businessObjectDataAttributeName), request);
    }

    /**
     * Updates an existing attribute for the business object data with 1 subpartition value.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param request the request information needed to update the business object data attribute
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_PUT)
    public BusinessObjectDataAttribute updateBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName, @RequestBody BusinessObjectDataAttributeUpdateRequest request)
    {
        return businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value), businessObjectDataVersion, businessObjectDataAttributeName),
            request);
    }

    /**
     * Updates an existing attribute for the business object data with 2 subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param request the request information needed to update the business object data attribute
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_PUT)
    public BusinessObjectDataAttribute updateBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName, @RequestBody BusinessObjectDataAttributeUpdateRequest request)
    {
        return businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value), businessObjectDataVersion,
                businessObjectDataAttributeName), request);
    }

    /**
     * Updates an existing attribute for the business object data with 3 subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param request the request information needed to update the business object data attribute
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_PUT)
    public BusinessObjectDataAttribute updateBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName, @RequestBody BusinessObjectDataAttributeUpdateRequest request)
    {
        return businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value),
                businessObjectDataVersion, businessObjectDataAttributeName), request);
    }

    /**
     * Updates an existing attribute for the business object data with 4 subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param subPartition4Value the 4th subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param request the request information needed to update the business object data attribute
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}/subPartition4Values/{subPartition4Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_PUT)
    public BusinessObjectDataAttribute updateBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("subPartition4Value") String subPartition4Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName, @RequestBody BusinessObjectDataAttributeUpdateRequest request)
    {
        return businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value, subPartition4Value),
                businessObjectDataVersion, businessObjectDataAttributeName), request);
    }

    /**
     * Updates an existing attribute for the business object data without subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param request the request information needed to update the business object data attribute
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}" +
        "/businessObjectDataAttributeNames/{businessObjectDataAttributeName}", method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_PUT)
    public BusinessObjectDataAttribute updateBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName, @RequestBody BusinessObjectDataAttributeUpdateRequest request)
    {
        return businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, new ArrayList<String>(), businessObjectDataVersion, businessObjectDataAttributeName), request);
    }

    /**
     * Updates an existing attribute for the business object data with 1 subpartition value.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param request the request information needed to update the business object data attribute
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_PUT)
    public BusinessObjectDataAttribute updateBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName, @RequestBody BusinessObjectDataAttributeUpdateRequest request)
    {
        return businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value), businessObjectDataVersion, businessObjectDataAttributeName),
            request);
    }

    /**
     * Updates an existing attribute for the business object data with 2 subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param request the request information needed to update the business object data attribute
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_PUT)
    public BusinessObjectDataAttribute updateBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName, @RequestBody BusinessObjectDataAttributeUpdateRequest request)
    {
        return businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value), businessObjectDataVersion,
                businessObjectDataAttributeName), request);
    }

    /**
     * Updates an existing attribute for the business object data with 3 subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param request the request information needed to update the business object data attribute
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_PUT)
    public BusinessObjectDataAttribute updateBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName, @RequestBody BusinessObjectDataAttributeUpdateRequest request)
    {
        return businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value),
                businessObjectDataVersion, businessObjectDataAttributeName), request);
    }

    /**
     * Updates an existing attribute for the business object data with 4 subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param subPartition4Value the 4th subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param request the request information needed to update the business object data attribute
     *
     * @return the business object data attribute information
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}/subPartition4Values/{subPartition4Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_PUT)
    public BusinessObjectDataAttribute updateBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("subPartition4Value") String subPartition4Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName, @RequestBody BusinessObjectDataAttributeUpdateRequest request)
    {
        return businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value, subPartition4Value),
                businessObjectDataVersion, businessObjectDataAttributeName), request);
    }

    /**
     * Deletes an existing attribute for the business object data without subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}" +
        "/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_DELETE)
    public BusinessObjectDataAttribute deleteBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, new ArrayList<String>(), businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Deletes an existing attribute for the business object data with 1 subpartition value.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_DELETE)
    public BusinessObjectDataAttribute deleteBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value), businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Deletes an existing attribute for the business object data with 2 subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_DELETE)
    public BusinessObjectDataAttribute deleteBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value), businessObjectDataVersion,
                businessObjectDataAttributeName));
    }

    /**
     * Deletes an existing attribute for the business object data with 3 subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_DELETE)
    public BusinessObjectDataAttribute deleteBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value),
                businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Deletes an existing attribute for the business object data with 4 subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param subPartition4Value the 4th subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}/subPartition4Values/{subPartition4Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_DELETE)
    public BusinessObjectDataAttribute deleteBusinessObjectDataAttribute(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("subPartition4Value") String subPartition4Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value, subPartition4Value),
                businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Deletes an existing attribute for the business object data without subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}" +
        "/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_DELETE)
    public BusinessObjectDataAttribute deleteBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, new ArrayList<String>(), businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Deletes an existing attribute for the business object data with 1 subpartition value.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_DELETE)
    public BusinessObjectDataAttribute deleteBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value), businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Deletes an existing attribute for the business object data with 2 subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_DELETE)
    public BusinessObjectDataAttribute deleteBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value), businessObjectDataVersion,
                businessObjectDataAttributeName));
    }

    /**
     * Deletes an existing attribute for the business object data with 3 subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_DELETE)
    public BusinessObjectDataAttribute deleteBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value),
                businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Deletes an existing attribute for the business object data with 4 subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param subPartition4Value the 4th subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the business object data attribute that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}/subPartition4Values/{subPartition4Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}/businessObjectDataAttributeNames/{businessObjectDataAttributeName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_DELETE)
    public BusinessObjectDataAttribute deleteBusinessObjectDataAttribute(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("subPartition4Value") String subPartition4Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("businessObjectDataAttributeName") String businessObjectDataAttributeName)
    {
        return businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value, subPartition4Value),
                businessObjectDataVersion, businessObjectDataAttributeName));
    }

    /**
     * Gets a list of keys for all existing business object data attributes for a specific business object data without subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value of the business object data
     * @param businessObjectDataVersion the business object data version
     *
     * @return the list of business object data attribute keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_GET)
    public BusinessObjectDataAttributeKeys getBusinessObjectDataAttributes(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion,
                partitionValue, new ArrayList<String>(), businessObjectDataVersion));
    }

    /**
     * Gets a list of keys for all existing business object data attributes for a specific business object data with 1 subpartition value.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     *
     * @return the list of business object data attribute keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_GET)
    public BusinessObjectDataAttributeKeys getBusinessObjectDataAttributes(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion,
                partitionValue, Arrays.asList(subPartition1Value), businessObjectDataVersion));
    }

    /**
     * Gets a list of keys for all existing business object data attributes for a specific business object data with 2 subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     *
     * @return the list of business object data attribute keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_GET)
    public BusinessObjectDataAttributeKeys getBusinessObjectDataAttributes(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion,
                partitionValue, Arrays.asList(subPartition1Value, subPartition2Value), businessObjectDataVersion));
    }

    /**
     * Gets a list of keys for all existing business object data attributes for a specific business object data with 3 subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     *
     * @return the list of business object data attribute keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_GET)
    public BusinessObjectDataAttributeKeys getBusinessObjectDataAttributes(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion,
                partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value), businessObjectDataVersion));
    }

    /**
     * Gets a list of keys for all existing business object data attributes for a specific business object data with 4 subpartition values.
     *
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param subPartition4Value the 4th subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     *
     * @return the list of business object data attribute keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}/subPartition4Values/{subPartition4Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_GET)
    public BusinessObjectDataAttributeKeys getBusinessObjectDataAttributes(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("subPartition4Value") String subPartition4Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(null, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion,
                partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value, subPartition4Value), businessObjectDataVersion));
    }

    /**
     * Gets a list of keys for all existing business object data attributes for a specific business object data without subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value of the business object data
     * @param businessObjectDataVersion the business object data version
     *
     * @return the list of business object data attribute keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_GET)
    public BusinessObjectDataAttributeKeys getBusinessObjectDataAttributes(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, new ArrayList<String>(), businessObjectDataVersion));
    }

    /**
     * Gets a list of keys for all existing business object data attributes for a specific business object data with 1 subpartition value.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     *
     * @return the list of business object data attribute keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_GET)
    public BusinessObjectDataAttributeKeys getBusinessObjectDataAttributes(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value), businessObjectDataVersion));
    }

    /**
     * Gets a list of keys for all existing business object data attributes for a specific business object data with 2 subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     *
     * @return the list of business object data attribute keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_GET)
    public BusinessObjectDataAttributeKeys getBusinessObjectDataAttributes(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value), businessObjectDataVersion));
    }

    /**
     * Gets a list of keys for all existing business object data attributes for a specific business object data with 3 subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     *
     * @return the list of business object data attribute keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_GET)
    public BusinessObjectDataAttributeKeys getBusinessObjectDataAttributes(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value),
                businessObjectDataVersion));
    }

    /**
     * Gets a list of keys for all existing business object data attributes for a specific business object data with 4 subpartition values.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param subPartition2Value the 2nd subpartition value of the business object data
     * @param subPartition3Value the 3rd subpartition value of the business object data
     * @param subPartition4Value the 4th subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     *
     * @return the list of business object data attribute keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_ATTRIBUTES_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}/subPartition4Values/{subPartition4Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_GET)
    public BusinessObjectDataAttributeKeys getBusinessObjectDataAttributes(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("subPartition4Value") String subPartition4Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion)
    {
        return businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value, subPartition4Value),
                businessObjectDataVersion));
    }
}
