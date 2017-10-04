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

import java.util.ArrayList;
import java.util.Arrays;

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateResponse;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.service.BusinessObjectDataStorageUnitStatusService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.helper.StorageUnitHelper;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles business object data storage unit status requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Business Object Data Storage Unit Status")
public class BusinessObjectDataStorageUnitStatusRestController extends HerdBaseController
{
    public static final String BUSINESS_OBJECT_DATA_STORAGE_UNIT_STATUS_URI_PREFIX = "/businessObjectDataStorageUnitStatus";

    @Autowired
    private BusinessObjectDataStorageUnitStatusService businessObjectDataStorageUnitStatusService;

    @Autowired
    private NotificationEventService notificationEventService;

    @Autowired
    private StorageUnitHelper storageUnitHelper;

    /**
     * Updates status of a business object data storage unit without subpartition values. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param businessObjectDataVersion the business object data version
     * @param storageName the storage name
     * @param request the business object data status update request
     *
     * @return the business object data storage unit status update response
     */
    @RequestMapping(
        value = BUSINESS_OBJECT_DATA_STORAGE_UNIT_STATUS_URI_PREFIX + "/namespaces/{namespace}" +
            "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
            "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/businessObjectFormatVersions/{businessObjectFormatVersion}" +
            "/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}/storageNames/{storageName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_STORAGE_UNIT_STATUS_PUT)
    public BusinessObjectDataStorageUnitStatusUpdateResponse updateBusinessObjectDataStorageUnitStatus(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion, @PathVariable("storageName") String storageName,
        @RequestBody BusinessObjectDataStorageUnitStatusUpdateRequest request)
    {
        return updateBusinessObjectDataStorageUnitStatusHelper(
            new BusinessObjectDataStorageUnitKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, new ArrayList<>(), businessObjectDataVersion, storageName), request);
    }

    /**
     * Updates status of a business object data storage unit with 1 subpartition values. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param subPartition1Value the value of the first subpartition
     * @param businessObjectDataVersion the business object data version
     * @param storageName the storage name
     * @param request the business object data status update request
     *
     * @return the business object data storage unit status update response
     */
    @RequestMapping(
        value = BUSINESS_OBJECT_DATA_STORAGE_UNIT_STATUS_URI_PREFIX + "/namespaces/{namespace}" +
            "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
            "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/businessObjectFormatVersions/{businessObjectFormatVersion}" +
            "/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}/businessObjectDataVersions/{businessObjectDataVersion}" +
            "/storageNames/{storageName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_STORAGE_UNIT_STATUS_PUT)
    public BusinessObjectDataStorageUnitStatusUpdateResponse updateBusinessObjectDataStorageUnitStatus(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("storageName") String storageName, @RequestBody BusinessObjectDataStorageUnitStatusUpdateRequest request)
    {
        return updateBusinessObjectDataStorageUnitStatusHelper(
            new BusinessObjectDataStorageUnitKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value), businessObjectDataVersion, storageName), request);
    }

    /**
     * Updates status of a business object data storage unit with 2 subpartition values. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param subPartition1Value the value of the first subpartition
     * @param subPartition2Value the value of the second subpartition
     * @param businessObjectDataVersion the business object data version
     * @param storageName the storage name
     * @param request the business object data status update request
     *
     * @return the business object data storage unit status update response
     */
    @RequestMapping(
        value = BUSINESS_OBJECT_DATA_STORAGE_UNIT_STATUS_URI_PREFIX + "/namespaces/{namespace}" +
            "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
            "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/businessObjectFormatVersions/{businessObjectFormatVersion}" +
            "/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}/subPartition2Values/{subPartition2Value}" +
            "/businessObjectDataVersions/{businessObjectDataVersion}/storageNames/{storageName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_STORAGE_UNIT_STATUS_PUT)
    public BusinessObjectDataStorageUnitStatusUpdateResponse updateBusinessObjectDataStorageUnitStatus(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion, @PathVariable("storageName") String storageName,
        @RequestBody BusinessObjectDataStorageUnitStatusUpdateRequest request)
    {
        return updateBusinessObjectDataStorageUnitStatusHelper(
            new BusinessObjectDataStorageUnitKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value), businessObjectDataVersion, storageName),
            request);
    }

    /**
     * Updates status of a business object data storage unit with 3 subpartition values. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param subPartition1Value the value of the first subpartition
     * @param subPartition2Value the value of the second subpartition
     * @param subPartition3Value the value of the third subpartition
     * @param businessObjectDataVersion the business object data version
     * @param storageName the storage name
     * @param request the business object data status update request
     *
     * @return the business object data storage unit status update response
     */
    @RequestMapping(
        value = BUSINESS_OBJECT_DATA_STORAGE_UNIT_STATUS_URI_PREFIX + "/namespaces/{namespace}" +
            "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
            "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/businessObjectFormatVersions/{businessObjectFormatVersion}" +
            "/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}/subPartition2Values/{subPartition2Value}" +
            "/subPartition3Values/{subPartition3Value}/businessObjectDataVersions/{businessObjectDataVersion}/storageNames/{storageName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_STORAGE_UNIT_STATUS_PUT)
    public BusinessObjectDataStorageUnitStatusUpdateResponse updateBusinessObjectDataStorageUnitStatus(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @PathVariable("storageName") String storageName, @RequestBody BusinessObjectDataStorageUnitStatusUpdateRequest request)
    {
        return updateBusinessObjectDataStorageUnitStatusHelper(
            new BusinessObjectDataStorageUnitKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value),
                businessObjectDataVersion, storageName), request);
    }

    /**
     * Updates status of a business object data storage unit with 4 subpartition values. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param subPartition1Value the value of the first subpartition
     * @param subPartition2Value the value of the second subpartition
     * @param subPartition3Value the value of the third subpartition
     * @param subPartition4Value the value of the forth subpartition
     * @param businessObjectDataVersion the business object data version
     * @param storageName the storage name
     * @param request the business object data status update request
     *
     * @return the business object data storage unit status update response
     */
    @RequestMapping(
        value = BUSINESS_OBJECT_DATA_STORAGE_UNIT_STATUS_URI_PREFIX + "/namespaces/{namespace}" +
            "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
            "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/businessObjectFormatVersions/{businessObjectFormatVersion}" +
            "/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}/subPartition2Values/{subPartition2Value}" +
            "/subPartition3Values/{subPartition3Value}/subPartition4Values/{subPartition4Value}/businessObjectDataVersions/{businessObjectDataVersion}" +
            "/storageNames/{storageName}", method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_STORAGE_UNIT_STATUS_PUT)
    public BusinessObjectDataStorageUnitStatusUpdateResponse updateBusinessObjectDataStorageUnitStatus(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("subPartition4Value") String subPartition4Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion, @PathVariable("storageName") String storageName,
        @RequestBody BusinessObjectDataStorageUnitStatusUpdateRequest request)
    {
        return updateBusinessObjectDataStorageUnitStatusHelper(
            new BusinessObjectDataStorageUnitKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value, subPartition4Value),
                businessObjectDataVersion, storageName), request);
    }

    /**
     * Updates status of a business object data storage unit.
     *
     * @param businessObjectDataStorageUnitKey the business object data storage unit key
     * @param request the business object data status update request
     *
     * @return the business object data storage unit status update response
     */
    private BusinessObjectDataStorageUnitStatusUpdateResponse updateBusinessObjectDataStorageUnitStatusHelper(
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey, BusinessObjectDataStorageUnitStatusUpdateRequest request)
    {
        // Update status of the business object data storage unit.
        BusinessObjectDataStorageUnitStatusUpdateResponse response =
            businessObjectDataStorageUnitStatusService.updateBusinessObjectDataStorageUnitStatus(businessObjectDataStorageUnitKey, request);

        // Create storage unit notification.
        notificationEventService.processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG,
            storageUnitHelper.getBusinessObjectDataKey(response.getBusinessObjectDataStorageUnitKey()),
            response.getBusinessObjectDataStorageUnitKey().getStorageName(), response.getStatus(), response.getPreviousStatus());

        return response;
    }
}
