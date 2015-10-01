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
package org.finra.dm.service;

import java.util.List;

import org.finra.dm.model.api.xml.BusinessObjectDataKey;

/**
 * The file upload cleanup service.
 */
public interface FileUploadCleanupService
{
    /**
     * Marks as DELETED any dangling business object data records having storage files associated with the specified storage.  Only tbe business object data
     * records that are older than threshold minutes and with no actual S3 files left in the storage will be "deleted".
     *
     * @param storageName the storage name
     * @param thresholdMinutes the expiration time in minutes to select dangling business object data for deletion
     *
     * @return the list of keys for business object data that got marked as DELETED
     */
    public List<BusinessObjectDataKey> deleteBusinessObjectData(String storageName, int thresholdMinutes);

    /**
     * Aborts any multipart uploads that were initiated in the specified S3 storage more than threshold minutes ago.
     *
     * @param storageName the storage name
     * @param thresholdMinutes the expiration time in minutes indicating which multipart uploads should be aborted
     *
     * @return the total number of aborted multipart uploads
     */
    public int abortMultipartUploads(String storageName, int thresholdMinutes);
}
