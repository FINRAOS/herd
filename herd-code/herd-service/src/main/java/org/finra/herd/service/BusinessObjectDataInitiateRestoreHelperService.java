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
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.BusinessObjectDataRestoreDto;

/**
 * The helper service class for the business object data initiate a restore request functionality.
 */
public interface BusinessObjectDataInitiateRestoreHelperService
{
    /**
     * Executes an after step for the initiation of a business object data restore request.
     *
     * @param businessObjectDataRestoreDto the DTO that holds various parameters needed to perform a business object data restore
     *
     * @return the business object data information
     */
    BusinessObjectData executeInitiateRestoreAfterStep(BusinessObjectDataRestoreDto businessObjectDataRestoreDto);

    /**
     * Executes S3 specific steps for the initiation of a business object data restore request. The method also updates the specified DTO.
     *
     * @param businessObjectDataRestoreDto the DTO that holds various parameters needed to perform a business object data restore
     */
    void executeS3SpecificSteps(BusinessObjectDataRestoreDto businessObjectDataRestoreDto);

    /**
     * Prepares for the business object data initiate a restore request by validating the business object data along with other related database entities. The
     * method also creates and returns a business object data restore DTO.
     *
     * @param businessObjectDataKey the business object data key
     * @param expirationInDays the the time, in days, between when the business object data is restored to the S3 bucket and when it expires
     * @param archiveRetrievalOption the archive retrieval option when restoring an archived object.
     *
     * @return the DTO that holds various parameters needed to perform a business object data restore
     */
    BusinessObjectDataRestoreDto prepareToInitiateRestore(BusinessObjectDataKey businessObjectDataKey, Integer expirationInDays, String archiveRetrievalOption,
        Boolean batchMode);
}
