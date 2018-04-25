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

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributesUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailability;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdl;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDownloadCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKeys;
import org.finra.herd.model.api.xml.BusinessObjectDataRetentionInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataRetryStoragePolicyTransitionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchResult;
import org.finra.herd.model.api.xml.BusinessObjectDataUploadCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataVersions;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.api.xml.StorageUnitDownloadCredential;
import org.finra.herd.model.api.xml.StorageUnitUploadCredential;
import org.finra.herd.model.dto.BusinessObjectDataSearchResultPagingInfoDto;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.BusinessObjectDataService;
import org.finra.herd.service.StorageUnitService;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles business object data REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Business Object Data")
public class BusinessObjectDataRestController extends HerdBaseController
{
    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataService businessObjectDataService;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private StorageUnitService storageUnitService;

    /**
     * Performs a search and returns a list of business object data key values and relative statuses for a range of requested business object data. <p> Requires
     * READ permission on namespace </p>
     *
     * @param businessObjectDataAvailabilityRequest the business object data availability request
     *
     * @return the business object data availability information
     */
    @RequestMapping(value = "/businessObjectData/availability", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_AVAILABILITY_POST)
    public BusinessObjectDataAvailability checkBusinessObjectDataAvailability(
        @RequestBody BusinessObjectDataAvailabilityRequest businessObjectDataAvailabilityRequest)
    {
        return businessObjectDataService.checkBusinessObjectDataAvailability(businessObjectDataAvailabilityRequest);
    }

    /**
     * Performs an availability check for a collection of business object data. <p> Requires READ permission on ALL namespaces </p>
     *
     * @param businessObjectDataAvailabilityCollectionRequest the business object data availability collection request
     *
     * @return the business object data availability information
     */
    @RequestMapping(value = "/businessObjectData/availabilityCollection", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_AVAILABILITY_COLLECTION_POST)
    public BusinessObjectDataAvailabilityCollectionResponse checkBusinessObjectDataAvailabilityCollection(
        @RequestBody BusinessObjectDataAvailabilityCollectionRequest businessObjectDataAvailabilityCollectionRequest)
    {
        return businessObjectDataService.checkBusinessObjectDataAvailabilityCollection(businessObjectDataAvailabilityCollectionRequest);
    }

    /**
     * Creates (i.e. registers) business object data. You may pre-register business object data by setting the status to one of the pre-registration statuses
     * (UPLOADING, PENDING_VALID, and PROCESSING). <p> Requires WRITE permission on namespace </p>
     *
     * @param businessObjectDataCreateRequest the information needed to create the business object data
     *
     * @return the created business object data
     */
    @RequestMapping(value = "/businessObjectData", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_POST)
    public BusinessObjectData createBusinessObjectData(@RequestBody BusinessObjectDataCreateRequest businessObjectDataCreateRequest)
    {
        BusinessObjectData businessObjectData = businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);

        // TODO This should be enhanced such that the notification events are captured by probably as an advice, and these calls are not specified everywhere
        // in the code.

        // The calls to notifications is being done in REST layer so that the event transaction (e.g. in this case: create business object data) is committed
        // and the event data is available for when notification is processed.

        // With proposed designed, when we go to event publish mode(e.g. create a database record for the event that will be picked up by notification
        // processing engine), We would want the event transaction to also rollback if event publishing failed. These calls will be moved to service layer.

        // Trigger notifications.
        businessObjectDataDaoHelper.triggerNotificationsForCreateBusinessObjectData(businessObjectData);

        return businessObjectData;
    }

    /**
     * Deletes an existing business object data without subpartition values with namespace. <p> Requires WRITE permission on namespace </p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param businessObjectDataVersion the business object data version
     * @param deleteFiles whether files should be deleted
     *
     * @return the deleted business object data information
     */
    @RequestMapping(
        value = "/businessObjectData/namespaces/{namespace}" +
            "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
            "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/businessObjectFormatVersions/{businessObjectFormatVersion}" +
            "/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_DELETE)
    public BusinessObjectData deleteBusinessObjectData(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion, @RequestParam("deleteFiles") Boolean deleteFiles)
    {
        return businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, new ArrayList<String>(), businessObjectDataVersion), deleteFiles);
    }

    /**
     * Deletes an existing business object data with 1 subpartition value with namespace. <p> Requires WRITE permission on namespace </p>
     *
     * @param namespace the namespace.
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param subPartition1Value sub-partition value 1
     * @param businessObjectDataVersion the business object data version
     * @param deleteFiles whether files should be deleted
     *
     * @return the deleted business object data information
     */
    @RequestMapping(
        value = "/businessObjectData/namespaces/{namespace}" +
            "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
            "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/businessObjectFormatVersions/{businessObjectFormatVersion}" +
            "/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_DELETE)
    public BusinessObjectData deleteBusinessObjectData(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @RequestParam("deleteFiles") Boolean deleteFiles)
    {
        return businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value), businessObjectDataVersion), deleteFiles);
    }

    /**
     * Deletes an existing business object data with 2 subpartition values with namespace. <p> Requires WRITE permission on namespace </p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param subPartition1Value sub-partition value 1
     * @param subPartition2Value sub-partition value 2
     * @param businessObjectDataVersion the business object data version
     * @param deleteFiles whether files should be deleted
     *
     * @return the deleted business object data information
     */
    @RequestMapping(
        value = "/businessObjectData/namespaces/{namespace}" +
            "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
            "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/businessObjectFormatVersions/{businessObjectFormatVersion}" +
            "/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}/subPartition2Values/{subPartition2Value}" +
            "/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_DELETE)
    public BusinessObjectData deleteBusinessObjectData(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion, @RequestParam("deleteFiles") Boolean deleteFiles)
    {
        return businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value), businessObjectDataVersion), deleteFiles);
    }

    /**
     * Deletes an existing business object data with 3 subpartition values with namespace <p> Requires WRITE permission on namespace </p>
     *
     * @param namespace the namespace.
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param subPartition1Value sub-partition value 1
     * @param subPartition2Value sub-partition value 2
     * @param subPartition3Value sub-partition value 3
     * @param businessObjectDataVersion the business object data version
     * @param deleteFiles whether files should be deleted
     *
     * @return the deleted business object data information
     */
    @RequestMapping(
        value = "/businessObjectData/namespaces/{namespace}" +
            "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
            "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/businessObjectFormatVersions/{businessObjectFormatVersion}" +
            "/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}/subPartition2Values/{subPartition2Value}" +
            "/subPartition3Values/{subPartition3Value}/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_DELETE)
    public BusinessObjectData deleteBusinessObjectData(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @RequestParam("deleteFiles") Boolean deleteFiles)
    {
        return businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value),
                businessObjectDataVersion), deleteFiles);
    }

    /**
     * Deletes an existing business object data with 4 subpartition values with namespace. <p> Requires WRITE permission on namespace </p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param subPartition1Value sub-partition value 1
     * @param subPartition2Value sub-partition value 2
     * @param subPartition3Value sub-partition value 3
     * @param subPartition4Value sub-partition value 4
     * @param businessObjectDataVersion the business object data version
     * @param deleteFiles whether files should be deleted
     *
     * @return the deleted business object data information
     */
    @RequestMapping(
        value = "/businessObjectData/namespaces/{namespace}" +
            "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
            "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/businessObjectFormatVersions/{businessObjectFormatVersion}" +
            "/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}/subPartition2Values/{subPartition2Value}" +
            "/subPartition3Values/{subPartition3Value}/subPartition4Values/{subPartition4Value}/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_DELETE)
    public BusinessObjectData deleteBusinessObjectData(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("subPartition4Value") String subPartition4Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion, @RequestParam("deleteFiles") Boolean deleteFiles)
    {
        return businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value, subPartition4Value),
                businessObjectDataVersion), deleteFiles);
    }

    /**
     * Initiates destruction process for an existing business object data. This endpoint uses S3 tagging to mark the relative S3 files for deletion. The S3 data
     * then gets deleted by S3 bucket lifecycle policy that deletes data based on S3 tagging. This endpoint to be used to delete records that are selected and
     * approved for destruction as per retention information specified for the relative business object format. <p> Requires WRITE permission on namespace </p>
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param businessObjectFormatUsage the usage of the business object format
     * @param businessObjectFormatFileType the file type of the business object format
     * @param businessObjectFormatVersion the version of the business object format
     * @param partitionValue the primary partition value
     * @param subPartitionValues the list of sub-partition values delimited by "|" (delimiter can be escaped by "\")
     * @param businessObjectDataVersion the version of the business object data
     *
     * @return the business object data information
     */
    @RequestMapping(
        value = "/businessObjectData/destroy/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
            "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
            "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}" +
            "/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_DESTROY_POST)
    public BusinessObjectData destroyBusinessObjectData(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @RequestParam(value = "subPartitionValues", required = false) String subPartitionValues)
    {
        return businessObjectDataService.destroyBusinessObjectData(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, herdStringHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValues),
                businessObjectDataVersion));
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system to perform queries for a range of requested business object data in the
     * specified storage. <p> Requires READ permission on namespace </p>
     *
     * @param businessObjectDataDdlRequest the business object data DDL request
     *
     * @return the business object data DDL information
     */
    @RequestMapping(value = "/businessObjectData/generateDdl", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_GENERATE_DDL_POST)
    public BusinessObjectDataDdl generateBusinessObjectDataDdl(@RequestBody BusinessObjectDataDdlRequest businessObjectDataDdlRequest)
    {
        return businessObjectDataService.generateBusinessObjectDataDdl(businessObjectDataDdlRequest);
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system to perform queries for a collection of business object data in the specified
     * storage. <p> Requires READ permission on ALL namespaces </p>
     *
     * @param businessObjectDataDdlCollectionRequest the business object data DDL collection request
     *
     * @return the business object data DDL information
     */
    @RequestMapping(value = "/businessObjectData/generateDdlCollection", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_GENERATE_DDL_COLLECTION_POST)
    public BusinessObjectDataDdlCollectionResponse generateBusinessObjectDataDdlCollection(
        @RequestBody BusinessObjectDataDdlCollectionRequest businessObjectDataDdlCollectionRequest)
    {
        return businessObjectDataService.generateBusinessObjectDataDdlCollection(businessObjectDataDdlCollectionRequest);
    }

    /**
     * Retrieves a list of keys for all existing business object data up to the limit configured in the system per specified business object definition. <p>
     * Results are sorted alphabetically by primary and sub-partition values descending. </p> <p> The limit on how many records this endpoint returns is set by
     * "business.object.data.search.max.results.per.page" configuration value. </p> <p> Requires READ permission on namespace. </p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     *
     * @return the list of business object data keys
     */
    @RequestMapping(
        value = "/businessObjectData/namespaces/{namespace}" + "/businessObjectDefinitionNames/{businessObjectDefinitionName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_BY_BUSINESS_OBJECT_DEFINITION_GET)
    public BusinessObjectDataKeys getAllBusinessObjectDataByBusinessObjectDefinition(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName)
    {
        return businessObjectDataService
            .getAllBusinessObjectDataByBusinessObjectDefinition(new BusinessObjectDefinitionKey(namespace, businessObjectDefinitionName));
    }

    /**
     * Retrieves a list of keys for all existing business object data up to the limit configured in the system per specified business object format. <p> Results
     * are sorted alphabetically by primary and sub-partition values descending. </p> <p> The limit on how many records this endpoint returns is set by
     * "business.object.data.search.max.results.per.page" configuration value. </p> <p> Requires READ permission on namespace. </p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     *
     * @return the list of business object data keys
     */
    @RequestMapping(
        value = "/businessObjectData/namespaces/{namespace}" +
            "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
            "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/businessObjectFormatVersions/{businessObjectFormatVersion}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_BY_BUSINESS_OBJECT_FORMAT_GET)
    public BusinessObjectDataKeys getAllBusinessObjectDataByBusinessObjectFormat(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion)
    {
        return businessObjectDataService.getAllBusinessObjectDataByBusinessObjectFormat(
            new BusinessObjectFormatKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion));
    }

    /**
     * Retrieves existing business object data entry information. <p/> NOTE: When both business object format version and business object data version are not
     * specified, the business object format version has the precedence. The latest business object format version is determined by a sub-query, which does the
     * following: <p> <ul> <li>selects all available data for the specified business object format (disregarding business object format version), partition
     * values, and business object data status (default is "VALID") <li>gets the latest business object format version from the records selected in the previous
     * step </ul> <p> <p> Requires READ permission on namespace </p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatPartitionKey the partition key of the business object format. When specified, the partition key is validated against the
     * partition key associated with the relative business object format
     * @param partitionValue the partition value of the business object data
     * @param subPartitionValues the list of sub-partition values delimited by "|" (delimiter can be escaped by "\")
     * @param businessObjectFormatVersion the version of the business object format. When the business object format version is not specified, the business
     * object data with the latest business format version available for the specified partition values is returned
     * @param businessObjectDataVersion the version of the business object data. When business object data version is not specified, the latest version of
     * business object data of the specified business object data status is returned
     * @param businessObjectDataStatus the status of the business object data. When business object data version is specified, this parameter is ignored.
     * Default value is "VALID"
     * @param includeBusinessObjectDataStatusHistory specifies to include business object data status history in the response
     * @param includeStorageUnitStatusHistory specifies to include storage unit status history for each storage unit in the response
     *
     * @return the retrieved business object data information
     */
    @RequestMapping(
        value = "/businessObjectData/namespaces/{namespace}" +
            "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
            "/businessObjectFormatFileTypes/{businessObjectFormatFileType}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_GET)
    public BusinessObjectData getBusinessObjectData(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @RequestParam(value = "partitionKey", required = false) String businessObjectFormatPartitionKey, @RequestParam("partitionValue") String partitionValue,
        @RequestParam(value = "subPartitionValues", required = false) String subPartitionValues,
        @RequestParam(value = "businessObjectFormatVersion", required = false) Integer businessObjectFormatVersion,
        @RequestParam(value = "businessObjectDataVersion", required = false) Integer businessObjectDataVersion,
        @RequestParam(value = "businessObjectDataStatus", required = false) String businessObjectDataStatus,
        @RequestParam(value = "includeBusinessObjectDataStatusHistory", required = false) Boolean includeBusinessObjectDataStatusHistory,
        @RequestParam(value = "includeStorageUnitStatusHistory", required = false) Boolean includeStorageUnitStatusHistory)
    {
        return businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, herdStringHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValues),
                businessObjectDataVersion), businessObjectFormatPartitionKey, businessObjectDataStatus, includeBusinessObjectDataStatusHistory,
            includeStorageUnitStatusHistory);
    }

    /**
     * Gets the AWS credential to download to the specified business object data and storage. <p> Requires READ permission on namespace </p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param businessObjectDataVersion the business object data version
     * @param storageName the storage name
     * @param subPartitionValues the list of sub-partition values
     *
     * @return AWS credential
     */
    @RequestMapping(value = "/businessObjectData/download/credential/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
        "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/businessObjectFormatVersions/{businessObjectFormatVersion}" +
        "/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_DOWNLOAD_CREDENTIAL_GET)
    @ApiOperation(value = "Gets Business Object Data Download Credentials. This is not meant for public consumption.", hidden = true)
    public BusinessObjectDataDownloadCredential getBusinessObjectDataDownloadCredential(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion, @RequestParam(value = "storageName", required = true) String storageName,
        @RequestParam(value = "subPartitionValues", required = false) String subPartitionValues)
    {
        StorageUnitDownloadCredential storageUnitDownloadCredential = storageUnitService.getStorageUnitDownloadCredential(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, herdStringHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValues),
                businessObjectDataVersion), storageName);

        return new BusinessObjectDataDownloadCredential(storageUnitDownloadCredential.getAwsCredential());
    }

    /**
     * Gets the AWS credential to upload to the specified business object data and storage. <p> Requires WRITE permission on namespace </p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param businessObjectDataVersion the business object data version
     * @param createNewVersion flag to create new version
     * @param storageName the storage name
     * @param subPartitionValues the list of sub-partition values
     *
     * @return AWS credential
     */
    @RequestMapping(value = "/businessObjectData/upload/credential/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
        "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/businessObjectFormatVersions/{businessObjectFormatVersion}" +
        "/partitionValues/{partitionValue}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_UPLOAD_CREDENTIAL_GET)
    @ApiOperation(value = "Gets Business Object Data Upload Credentials. This is not meant for public consumption.", hidden = true)
    public BusinessObjectDataUploadCredential getBusinessObjectDataUploadCredential(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @RequestParam(value = "businessObjectDataVersion", required = false) Integer businessObjectDataVersion,
        @RequestParam(value = "createNewVersion", required = false) Boolean createNewVersion,
        @RequestParam(value = "storageName", required = true) String storageName,
        @RequestParam(value = "subPartitionValues", required = false) String subPartitionValues)
    {
        StorageUnitUploadCredential storageUnitUploadCredential = storageUnitService.getStorageUnitUploadCredential(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, herdStringHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValues),
                businessObjectDataVersion), createNewVersion, storageName);

        return new BusinessObjectDataUploadCredential(storageUnitUploadCredential.getAwsCredential(), storageUnitUploadCredential.getAwsKmsKeyId());
    }

    /**
     * Retrieves a list of existing business object data versions. <p> Requires READ permission on namespace </p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param partitionValue the partition value
     * @param subPartitionValues the list of sub-partition values
     * @param businessObjectFormatVersion the business object format version
     * @param businessObjectDataVersion the business object data version
     *
     * @return the retrieved business object data versions
     */
    @RequestMapping(
        value = "/businessObjectData/namespaces/{namespace}" +
            "/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages/{businessObjectFormatUsage}" +
            "/businessObjectFormatFileTypes/{businessObjectFormatFileType}/versions",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_VERSIONS_GET)
    public BusinessObjectDataVersions getBusinessObjectDataVersions(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType, @RequestParam("partitionValue") String partitionValue,
        @RequestParam(value = "subPartitionValues", required = false) String subPartitionValues,
        @RequestParam(value = "businessObjectFormatVersion", required = false) Integer businessObjectFormatVersion,
        @RequestParam(value = "businessObjectDataVersion", required = false) Integer businessObjectDataVersion)
    {
        return businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, herdStringHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValues),
                businessObjectDataVersion));
    }

    /**
     * <p> Gets the S3 key prefix for writing or accessing business object data. </p> <p> This endpoint requires a namespace. </p> <p> Requires READ permission
     * on namespace </p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionKey the partition key
     * @param partitionValue the partition value
     * @param subPartitionValues the list of sub-partition values
     * @param businessObjectDataVersion the business object data version
     * @param storageName the storage name
     * @param createNewVersion Whether a new business object data can be created
     * @param servletRequest the servlet request
     *
     * @return the S3 key prefix
     */
    @RequestMapping(
        value = "/businessObjectData/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}/businessObjectFormatUsages" +
            "/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
            "/businessObjectFormatVersions/{businessObjectFormatVersion}/s3KeyPrefix",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_S3_KEY_PREFIX_GET)
    @ApiOperation(value = "Gets the S3 key prefix information for a specified namespace")
    public S3KeyPrefixInformation getS3KeyPrefix(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion,
        @RequestParam(value = "partitionKey", required = false) String partitionKey, @RequestParam("partitionValue") String partitionValue,
        @RequestParam(value = "subPartitionValues", required = false) String subPartitionValues,
        @RequestParam(value = "businessObjectDataVersion", required = false) Integer businessObjectDataVersion,
        @RequestParam(value = "storageName", required = false) String storageName,
        @RequestParam(value = "createNewVersion", required = false, defaultValue = "false") Boolean createNewVersion, ServletRequest servletRequest)
    {
        return storageUnitService.getS3KeyPrefix(
            validateRequestAndCreateBusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, subPartitionValues, businessObjectDataVersion, servletRequest), partitionKey, storageName,
            createNewVersion);
    }

    /**
     * Registers data as INVALID for objects which exist in S3 but are not registered in herd. <p> Requires WRITE permission on namespace </p>
     *
     * @param businessObjectDataInvalidateUnregisteredRequest the business object data invalidate un-register request
     *
     * @return the business object data invalidate unregistered response
     */
    @RequestMapping(value = "/businessObjectData/unregistered/invalidation", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_UNREGISTERED_INVALIDATE)
    public BusinessObjectDataInvalidateUnregisteredResponse invalidateUnregisteredBusinessObjectData(
        @RequestBody BusinessObjectDataInvalidateUnregisteredRequest businessObjectDataInvalidateUnregisteredRequest)
    {
        BusinessObjectDataInvalidateUnregisteredResponse businessObjectDataInvalidateUnregisteredResponse =
            businessObjectDataService.invalidateUnregisteredBusinessObjectData(businessObjectDataInvalidateUnregisteredRequest);

        // Trigger notifications.
        businessObjectDataDaoHelper.triggerNotificationsForInvalidateUnregisteredBusinessObjectData(businessObjectDataInvalidateUnregisteredResponse);

        return businessObjectDataInvalidateUnregisteredResponse;
    }

    /**
     * Initiates a restore request for a currently archived business object data.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the version of the business object format
     * @param partitionValue the primary partition value of the business object data
     * @param businessObjectDataVersion the version of the business object data
     * @param subPartitionValues the list of sub-partition values delimited by "|" (delimiter can be escaped by "\")
     * @param expirationInDays the time, in days, between when the business object data is restored to the S3 bucket and when it expires
     *
     * @return the business object data information
     */
    @RequestMapping(
        value = "/businessObjectData/restore/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
            "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
            "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}" +
            "/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_RESTORE_POST)
    public BusinessObjectData restoreBusinessObjectData(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @RequestParam(value = "subPartitionValues", required = false) String subPartitionValues,
        @RequestParam(value = "expirationInDays", required = false) Integer expirationInDays)
    {
        return businessObjectDataService.restoreBusinessObjectData(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, herdStringHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValues),
                businessObjectDataVersion), expirationInDays);
    }

    /**
     * Retries a storage policy transition by forcing re-initiation of the archiving process for the specified business object data that is still in progress of
     * a valid archiving operation. This endpoint is designed to be run only after confirmation that the business object data is stuck due to an error during
     * archiving.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param businessObjectFormatUsage the usage of the business object format
     * @param businessObjectFormatFileType the file type of the business object format
     * @param businessObjectFormatVersion the version of the business object format
     * @param partitionValue the primary partition value of the business object data
     * @param businessObjectDataVersion the version of the business object data
     * @param subPartitionValues the optional list of sub-partition values delimited by "|" (delimiter can be escaped by "\")
     * @param request the information needed to retry a storage policy transition
     *
     * @return the business object data information
     */
    @RequestMapping(
        value = "/businessObjectData/retryStoragePolicyTransition/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
            "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
            "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}" +
            "/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_RETRY_STORAGE_POLICY_TRANSITION_POST)
    public BusinessObjectData retryStoragePolicyTransition(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @RequestParam(value = "subPartitionValues", required = false) String subPartitionValues,
        @RequestBody BusinessObjectDataRetryStoragePolicyTransitionRequest request)
    {
        return businessObjectDataService.retryStoragePolicyTransition(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, herdStringHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValues),
                businessObjectDataVersion), request);
    }

    /**
     * Searches business object data based on namespace, definition name, format usage, file type, and format version. <p> Namespace and definition name are
     * required. </p> <p> Requires READ permission on namespace </p> <p> The response contains the following HTTP headers: <ul> <li>Paging-MaxResultsPerPage -
     * the HTTP header for the maximum number of results that will be returned on any page of data. The "pageSize" query parameter should not be greater than
     * this value or an HTTP status of 400 (Bad Request) error would be returned</li> <li>Paging-PageCount - the HTTP header for the total number of pages that
     * exist assuming a page size limit and the total records returned in the query</li> <li>Paging-PageNum - the HTTP header for the current page number being
     * returned. For the first page, this value would be "1"</li> <li>Paging-PageSize - the HTTP header for the current page size limit. This is based on what
     * is specified in the request "pageSize" query parameter</li> <li>Paging-TotalRecordsOnPage - the HTTP header for the total number of records returned on
     * the current page. This could be less than the "pageSize" query parameter on the last page of data</li> <li>Paging-TotalRecordCount - the HTTP header for
     * the total number of records that would be returned across all pages. This is basically a "select count" query</li> </ul> </p>
     *
     * @param pageNum the page number. If this parameter is specified, results contain the appropriate page that is specified. Page numbers are one-based - that
     * is the first page number is one. Default value is 1
     * @param pageSize the page size. If pageSize parameter is specified, results contain that number of business object data (unless it is the end of the
     * result set). Default value is 1000
     * @param businessObjectDataSearchRequest the business object data search request
     * @param httpServletResponse the HTTP servlet response
     *
     * @return the business object data search result with HTTP headers that contain the pagination information
     */
    @RequestMapping(value = "/businessObjectData/search", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_SEARCH_POST)
    public BusinessObjectDataSearchResult searchBusinessObjectData(@RequestParam(value = "pageNum", required = false) Integer pageNum,
        @RequestParam(value = "pageSize", required = false) Integer pageSize, @RequestBody BusinessObjectDataSearchRequest businessObjectDataSearchRequest,
        HttpServletResponse httpServletResponse)
    {
        // Search business object data.
        BusinessObjectDataSearchResultPagingInfoDto businessObjectDataSearchResultPagingInfoDto =
            businessObjectDataService.searchBusinessObjectData(pageNum, pageSize, businessObjectDataSearchRequest);

        // Add HTTP headers to HTTP servlet response per paging information.
        addPagingHttpHeaders(httpServletResponse, businessObjectDataSearchResultPagingInfoDto);

        // Create and return the HTTP response.
        return businessObjectDataSearchResultPagingInfoDto.getBusinessObjectDataSearchResult();
    }

    /**
     * Updates attributes for the business object data without subpartition values. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param request the information needed to update the business object data attributes
     *
     * @return the updated business object data
     */
    @RequestMapping(value = "/businessObjectDataAttributes/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_PUT)
    public BusinessObjectData updateBusinessObjectDataAttributes(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion, @RequestBody BusinessObjectDataAttributesUpdateRequest request)
    {
        return businessObjectDataService.updateBusinessObjectDataAttributes(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, new ArrayList<>(), businessObjectDataVersion), request);
    }

    /**
     * Updates attributes for the business object data with 1 subpartition value. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value of the business object data
     * @param subPartition1Value the 1st subpartition value of the business object data
     * @param businessObjectDataVersion the business object data version
     * @param request the information needed to update the business object data attributes
     *
     * @return the updated business object data
     */
    @RequestMapping(value = "/businessObjectDataAttributes/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_PUT)
    public BusinessObjectData updateBusinessObjectDataAttributes(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @RequestBody BusinessObjectDataAttributesUpdateRequest request)
    {
        return businessObjectDataService.updateBusinessObjectDataAttributes(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value), businessObjectDataVersion), request);
    }

    /**
     * Updates attributes for the business object data with 2 subpartition values. <p>Requires WRITE permission on namespace</p>
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
     * @param request the information needed to update the business object data attributes
     *
     * @return the updated business object data
     */
    @RequestMapping(value = "/businessObjectDataAttributes/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_PUT)
    public BusinessObjectData updateBusinessObjectDataAttributes(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion, @RequestBody BusinessObjectDataAttributesUpdateRequest request)
    {
        return businessObjectDataService.updateBusinessObjectDataAttributes(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value), businessObjectDataVersion), request);
    }

    /**
     * Updates attributes for the business object data with 3 subpartition values. <p>Requires WRITE permission on namespace</p>
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
     * @param request the information needed to update the business object data attributes
     *
     * @return the updated business object data
     */
    @RequestMapping(value = "/businessObjectDataAttributes/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_PUT)
    public BusinessObjectData updateBusinessObjectDataAttributes(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @RequestBody BusinessObjectDataAttributesUpdateRequest request)
    {
        return businessObjectDataService.updateBusinessObjectDataAttributes(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value),
                businessObjectDataVersion), request);
    }

    /**
     * Updates attributes for the business object data with 4 subpartition values. <p>Requires WRITE permission on namespace</p>
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
     * @param request the information needed to update the business object data attributes
     *
     * @return the updated business object data
     */
    @RequestMapping(value = "/businessObjectDataAttributes/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/subPartition1Values/{subPartition1Value}" +
        "/subPartition2Values/{subPartition2Value}/subPartition3Values/{subPartition3Value}/subPartition4Values/{subPartition4Value}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_ATTRIBUTES_ALL_PUT)
    public BusinessObjectData updateBusinessObjectDataAttributes(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("subPartition1Value") String subPartition1Value, @PathVariable("subPartition2Value") String subPartition2Value,
        @PathVariable("subPartition3Value") String subPartition3Value, @PathVariable("subPartition4Value") String subPartition4Value,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion, @RequestBody BusinessObjectDataAttributesUpdateRequest request)
    {
        return businessObjectDataService.updateBusinessObjectDataAttributes(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, Arrays.asList(subPartition1Value, subPartition2Value, subPartition3Value, subPartition4Value),
                businessObjectDataVersion), request);
    }

    /**
     * Updates retention information for an existing business object data. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param businessObjectFormatUsage the usage of the business object format
     * @param businessObjectFormatFileType the file type of the business object format
     * @param businessObjectFormatVersion the version of the business object format
     * @param partitionValue the primary partition value
     * @param subPartitionValues the list of sub-partition values delimited by "|" (delimiter can be escaped by "\")
     * @param businessObjectDataVersion the version of the business object data
     * @param request the business object data retention information update request
     *
     * @return the business object data information
     */
    @RequestMapping(
        value = "/businessObjectDataRetentionInformation/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
            "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
            "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}" +
            "/businessObjectDataVersions/{businessObjectDataVersion}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_RETENTION_INFORMATION_PUT)
    public BusinessObjectData updateBusinessObjectDataRetentionInformation(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion,
        @RequestParam(value = "subPartitionValues", required = false) String subPartitionValues,
        @RequestBody BusinessObjectDataRetentionInformationUpdateRequest request)
    {
        return businessObjectDataService.updateBusinessObjectDataRetentionInformation(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, herdStringHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValues),
                businessObjectDataVersion), request);
    }

    /**
     * Validates the given {@code servletRequest} and constructs a new {@link BusinessObjectDataKey}. The {@code servletRequest} validation involves validations
     * of request parameters which Spring MVC may not implement out-of-the-box. In our case, the request is asserted to no contain duplicate parameters.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param subPartitionValues the sub-partition values
     * @param businessObjectDataVersion the business object data version
     * @param servletRequest the servlet request
     *
     * @return a new {@link BusinessObjectDataKey}
     */
    private BusinessObjectDataKey validateRequestAndCreateBusinessObjectDataKey(String namespace, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue,
        String subPartitionValues, Integer businessObjectDataVersion, ServletRequest servletRequest)
    {
        // Ensure there are no duplicate query string parameters.
        validateNoDuplicateQueryStringParams(servletRequest.getParameterMap(), "partitionKey", "partitionValue");

        // Invoke the service.
        return new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
            businessObjectFormatVersion, partitionValue, herdStringHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValues),
            businessObjectDataVersion);
    }
}
