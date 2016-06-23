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

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.api.xml.StorageUnitDownloadCredential;
import org.finra.herd.model.api.xml.StorageUnitUploadCredential;

public interface StorageUnitService
{

    /**
     * Gets the AWS credential to upload to the specified storage unit.
     *
     * @param businessObjectDataKey The business object data key
     * @param createNewVersion Flag to indicate whether to return the next version or not
     * @param storageName The storage name
     *
     * @return AWS credential
     */
    public StorageUnitUploadCredential getStorageUnitUploadCredential(BusinessObjectDataKey businessObjectDataKey, Boolean createNewVersion,
        String storageName);

    /**
     * Gets the AWS credential to download to the specified storage unit.
     *
     * @param businessObjectDataKey The business object data key
     * @param storageName The storage name
     *
     * @return AWS credential
     */
    public StorageUnitDownloadCredential getStorageUnitDownloadCredential(BusinessObjectDataKey businessObjectDataKey, String storageName);

    /**
     * Gets the S3 key prefix. This method starts a new transaction.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectFormatPartitionKey the business object format partition key
     * @param storageName the storage name
     * @param createNewVersion specifies if it is OK to return an S3 key prefix for a new business object data version that is not an initial version. This
     * parameter is ignored, when the business object data version is specified.
     *
     * @return the retrieved business object data information
     */
    public S3KeyPrefixInformation getS3KeyPrefix(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey, String storageName,
        Boolean createNewVersion);
}
