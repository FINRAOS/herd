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
     * Prepares for the business object data initiate a restore request by validating the business object data along with other related database entities. The
     * method also creates and returns a business object data restore DTO.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the DTO that holds various parameters needed to perform a business object data restore
     */
    public BusinessObjectDataRestoreDto prepareToInitiateRestore(BusinessObjectDataKey businessObjectDataKey);

    /**
     * Executes S3 specific steps for the initiation of a business object data restore request. The method also updates the specified DTO.
     *
     * @param businessObjectDataRestoreDto the DTO that holds various parameters needed to perform a business object data restore
     */
    public void executeS3SpecificSteps(BusinessObjectDataRestoreDto businessObjectDataRestoreDto);

    /**
     * Executes an after step for the initiation of a business object data restore request.
     *
     * @param businessObjectDataRestoreDto the DTO that holds various parameters needed to perform a business object data restore
     *
     * @return the business object data information
     */
    public BusinessObjectData executeInitiateRestoreAfterStep(BusinessObjectDataRestoreDto businessObjectDataRestoreDto);
}
