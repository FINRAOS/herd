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
package org.finra.herd.service;

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
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKeys;
import org.finra.herd.model.api.xml.BusinessObjectDataRetentionInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataRetryStoragePolicyTransitionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataVersions;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.dto.BusinessObjectDataSearchResultPagingInfoDto;

/**
 * The business object data service.
 */
public interface BusinessObjectDataService
{
    public final String MAX_PARTITION_VALUE_TOKEN = "${maximum.partition.value}";

    public final String MIN_PARTITION_VALUE_TOKEN = "${minimum.partition.value}";

    /**
     * Creates a new business object data from the request information. Creates its own transaction.
     *
     * @param businessObjectDataCreateRequest the business object data create request
     *
     * @return the newly created and persisted business object data.
     */
    public BusinessObjectData createBusinessObjectData(BusinessObjectDataCreateRequest businessObjectDataCreateRequest);

    /**
     * Retrieves existing business object data entry information. This method starts a new transaction.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectFormatPartitionKey the business object format partition key
     * @param businessObjectDataStatus the business object data status, may be null
     * @param includeBusinessObjectDataStatusHistory specifies to include business object data status history in the response
     * @param includeStorageUnitStatusHistory specifies to include storage unit status history for each storage unit in the response
     *
     * @return the retrieved business object data information
     */
    public BusinessObjectData getBusinessObjectData(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey,
        String businessObjectDataStatus, Boolean includeBusinessObjectDataStatusHistory, Boolean includeStorageUnitStatusHistory);

    /**
     * Retrieves a list of existing business object data versions, if any.
     *
     * @param businessObjectDataKey the business object data key with possibly missing business object format and/or data version values
     *
     * @return the retrieved business object data versions
     */
    public BusinessObjectDataVersions getBusinessObjectDataVersions(BusinessObjectDataKey businessObjectDataKey);

    /**
     * Updates attributes for business object data.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataAttributesUpdateRequest the information needed to update the business object data attributes
     *
     * @return the updated business object data information
     */
    public BusinessObjectData updateBusinessObjectDataAttributes(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataAttributesUpdateRequest businessObjectDataAttributesUpdateRequest);

    /**
     * Updates retention information for an existing business object data.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataRetentionInformationUpdateRequest the business object data retention information update request
     *
     * @return the business object data information
     */
    public BusinessObjectData updateBusinessObjectDataRetentionInformation(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataRetentionInformationUpdateRequest businessObjectDataRetentionInformationUpdateRequest);

    /**
     * Deletes an existing business object data.
     *
     * @param businessObjectDataKey the business object data key
     * @param deleteFiles specifies if data files should be deleted or not
     *
     * @return the deleted business object data information
     */
    public BusinessObjectData deleteBusinessObjectData(BusinessObjectDataKey businessObjectDataKey, Boolean deleteFiles);

    /**
     * Initiates destruction process for an existing business object data by using S3 tagging to mark the relative S3 files for deletion and updating statuses
     * of the business object data and its storage unit. The S3 data then gets deleted by S3 bucket lifecycle policy that is based on S3 tagging.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the business object data information
     */
    public BusinessObjectData destroyBusinessObjectData(BusinessObjectDataKey businessObjectDataKey);

    /**
     * Performs a search and returns a list of business object data key values and relative statuses for a range of requested business object data. Creates its
     * own transaction.
     *
     * @param businessObjectDataAvailabilityRequest the business object data availability request
     *
     * @return the business object data availability information
     */
    public BusinessObjectDataAvailability checkBusinessObjectDataAvailability(BusinessObjectDataAvailabilityRequest businessObjectDataAvailabilityRequest);

    /**
     * Performs an availability check for a collection of business object data.
     *
     * @param request the business object data availability collection request
     *
     * @return the business object data availability information
     */
    public BusinessObjectDataAvailabilityCollectionResponse checkBusinessObjectDataAvailabilityCollection(
        BusinessObjectDataAvailabilityCollectionRequest request);

    /**
     * Retrieves the DDL to initialize the specified type of the database system to perform queries for a range of requested business object data in the
     * specified storage. This method starts a new transaction.
     *
     * @param businessObjectDataDdlRequest the business object data DDL request
     *
     * @return the business object data DDL information
     */
    public BusinessObjectDataDdl generateBusinessObjectDataDdl(BusinessObjectDataDdlRequest businessObjectDataDdlRequest);

    /**
     * Retrieves the DDL to initialize the specified type of the database system to perform queries for a collection of business object data in the specified
     * storages. This method starts a new transaction.
     *
     * @param businessObjectDataDdlCollectionRequest the business object data DDL collection request
     *
     * @return the business object data DDL information
     */
    public BusinessObjectDataDdlCollectionResponse generateBusinessObjectDataDdlCollection(
        BusinessObjectDataDdlCollectionRequest businessObjectDataDdlCollectionRequest);

    /**
     * Creates business object data registrations in INVALID status if the S3 object exists, but no registration exists.
     *
     * @param businessObjectDataInvalidateUnregisteredRequest {@link BusinessObjectDataInvalidateUnregisteredRequest}
     *
     * @return {@link BusinessObjectDataInvalidateUnregisteredResponse}
     */
    public BusinessObjectDataInvalidateUnregisteredResponse invalidateUnregisteredBusinessObjectData(
        BusinessObjectDataInvalidateUnregisteredRequest businessObjectDataInvalidateUnregisteredRequest);

    /**
     * Retries a storage policy transition by forcing re-initiation of the archiving process for the specified business object data that is still in progress of
     * a valid archiving operation.
     *
     * @param businessObjectDataKey the business object data key
     * @param request the information needed to retry a storage policy transition
     *
     * @return the business object data information
     */
    public BusinessObjectData retryStoragePolicyTransition(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataRetryStoragePolicyTransitionRequest request);

    /**
     * Initiates a restore request for a currently archived business object data.
     *
     * @param businessObjectDataKey the business object data key
     * @param expirationInDays the the time, in days, between when the business object data is restored to the S3 bucket and when it expires
     *
     * @return the business object data information
     */
    public BusinessObjectData restoreBusinessObjectData(BusinessObjectDataKey businessObjectDataKey, Integer expirationInDays);

    /**
     * Search business object data based on the request
     *
     * @param pageNum if pageNum parameter is specified, results contain the appropriate page specified. Page numbers are one-based - that is the first page
     * number is one.
     * @param pageSize if pageSize parameter is specified, results contain that number of business object data (unless it is the end of the result set).
     * @param request search request
     *
     * @return business data search result with the paging information
     */
    public BusinessObjectDataSearchResultPagingInfoDto searchBusinessObjectData(Integer pageNum, Integer pageSize, BusinessObjectDataSearchRequest request);

    /**
     * Retrieves a list of keys for all existing business object data up to the limit configured in the system per specified business object definition.
     *
     * @param businessObjectDefinitionKey the business object definition key (case-insensitive)
     *
     * @return the list of business object data keys
     */
    public BusinessObjectDataKeys getAllBusinessObjectDataByBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey);

    /**
     * Retrieves a list of keys for all existing business object data up to the limit configured in the system per specified business object format.
     *
     * @param businessObjectFormatKey the business object format key (case-insensitive)
     *
     * @return the list of business object data keys
     */
    public BusinessObjectDataKeys getAllBusinessObjectDataByBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey);
}
