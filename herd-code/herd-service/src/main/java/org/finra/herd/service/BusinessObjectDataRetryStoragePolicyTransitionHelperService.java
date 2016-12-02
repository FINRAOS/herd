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
import org.finra.herd.model.api.xml.BusinessObjectDataRetryStoragePolicyTransitionRequest;
import org.finra.herd.model.dto.BusinessObjectDataRetryStoragePolicyTransitionDto;

/**
 * The helper service class for the business object data retry storage policy transition functionality.
 */
public interface BusinessObjectDataRetryStoragePolicyTransitionHelperService
{
    /**
     * Executes AWS specific steps needed to retry a storage policy transition.
     *
     * @param businessObjectDataRetryStoragePolicyTransitionDto the DTO that holds various parameters needed to retry a storage policy transition
     */
    public void executeAwsSpecificSteps(BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto);

    /**
     * Executes the after step for the retry a storage policy transition and return the business object data information.
     *
     * @param businessObjectDataRetryStoragePolicyTransitionDto the DTO that holds various parameters needed to retry a storage policy transition
     *
     * @return the business object data information
     */
    public BusinessObjectData executeRetryStoragePolicyTransitionAfterStep(
        BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto);

    /**
     * Prepares for the business object data retry storage policy transition by validating the input parameters along with the related database entities. The
     * method also creates and returns a business object data retry storage policy transition DTO.
     *
     * @param businessObjectDataKey the business object data key
     * @param request the information needed to retry a storage policy transition
     *
     * @return the DTO that holds various parameters needed to retry a storage policy transition
     */
    public BusinessObjectDataRetryStoragePolicyTransitionDto prepareToRetryStoragePolicyTransition(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataRetryStoragePolicyTransitionRequest request);
}
