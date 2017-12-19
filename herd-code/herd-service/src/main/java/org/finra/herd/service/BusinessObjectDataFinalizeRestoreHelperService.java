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

import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.dto.BusinessObjectDataRestoreDto;

/**
 * The helper service class for the business object data finalize restore functionality.
 */
public interface BusinessObjectDataFinalizeRestoreHelperService
{
    /**
     * Completes the finalize restore operation.
     *
     * @param businessObjectDataRestoreDto the DTO that holds various parameters needed to perform a business object data restore
     */
    public void completeFinalizeRestore(BusinessObjectDataRestoreDto businessObjectDataRestoreDto);

    /**
     * Executes S3 specific steps for the business object data finalize restore.
     *
     * @param businessObjectDataRestoreDto the DTO that holds various parameters needed to perform a business object data restore
     */
    public void executeS3SpecificSteps(BusinessObjectDataRestoreDto businessObjectDataRestoreDto);

    /**
     * Prepares for the business object data finalize restore by validating the S3 storage unit along with other related database entities. The method also
     * creates and returns a business object data restore DTO.
     *
     * @param storageUnitKey the storage unit key
     *
     * @return the DTO that holds various parameters needed to perform a business object data restore
     */
    public BusinessObjectDataRestoreDto prepareToFinalizeRestore(BusinessObjectDataStorageUnitKey storageUnitKey);
}
