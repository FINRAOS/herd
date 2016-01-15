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
import org.finra.herd.model.api.xml.BusinessObjectDataStatusInformation;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataUploadCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataVersions;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;

/**
 * test for crlf The business object data service.
 */
public interface BusinessObjectDataService
{
    public final String MAX_PARTITION_VALUE_TOKEN = "${maximum.partition.value}";
    public final String MIN_PARTITION_VALUE_TOKEN = "${minimum.partition.value}";

    public S3KeyPrefixInformation getS3KeyPrefix(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey,
        Boolean createNewVersion);

    public BusinessObjectData createBusinessObjectData(BusinessObjectDataCreateRequest businessObjectDataCreateRequest);

    public BusinessObjectData getBusinessObjectData(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey);

    public BusinessObjectDataVersions getBusinessObjectDataVersions(BusinessObjectDataKey businessObjectDataKey);

    public BusinessObjectData deleteBusinessObjectData(BusinessObjectDataKey businessObjectDataKey, Boolean deleteFiles);

    public BusinessObjectDataAvailability checkBusinessObjectDataAvailability(BusinessObjectDataAvailabilityRequest businessObjectDataAvailabilityRequest);

    public BusinessObjectDataAvailabilityCollectionResponse checkBusinessObjectDataAvailabilityCollection(
        BusinessObjectDataAvailabilityCollectionRequest request);

    public BusinessObjectDataDdl generateBusinessObjectDataDdl(BusinessObjectDataDdlRequest businessObjectDataDdlRequest);

    public BusinessObjectDataDdlCollectionResponse generateBusinessObjectDataDdlCollection(
        BusinessObjectDataDdlCollectionRequest businessObjectDataDdlCollectionRequest);

    /**
     * Retrieves status information for an existing business object data.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectFormatPartitionKey the business object format partition key
     *
     * @return the retrieved business object data status information
     */
    public BusinessObjectDataStatusInformation getBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey,
        String businessObjectFormatPartitionKey);

    /**
     * Updates status of the business object data.
     *
     * @param businessObjectDataKey the business object data key
     * @param request the business object data status update request
     *
     * @return the business object data status update response
     */
    public BusinessObjectDataStatusUpdateResponse updateBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataStatusUpdateRequest request);

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
     * Gets the AWS credential to upload to the specified business object data and storage.
     * 
     * @param businessObjectDataKey The business object data key
     * @param createNewVersion Flag to indicate whether to return the next version or not
     * @param storageName The storage name
     * @return AWS credential
     */
    public BusinessObjectDataUploadCredential getBusinessObjectDataUploadCredential(BusinessObjectDataKey businessObjectDataKey, Boolean createNewVersion,
        String storageName);

    /**
     * Gets the AWS credential to download to the specified business object data and storage.
     * 
     * @param businessObjectDataKey The business object data key
     * @param storageName The storage name
     * @return AWS credential
     */
    public BusinessObjectDataDownloadCredential getBusinessObjectDataDownloadCredential(BusinessObjectDataKey businessObjectDataKey, String storageName);
}
