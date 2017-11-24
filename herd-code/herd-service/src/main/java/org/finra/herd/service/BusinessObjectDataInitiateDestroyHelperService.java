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
import org.finra.herd.model.dto.BusinessObjectDataDestroyDto;

public interface BusinessObjectDataInitiateDestroyHelperService
{
    /**
     * Executes an after step for initiation of a business object data destroy. This method also updates business object data destroy DTO passed as a
     * parameter.
     *
     * @param businessObjectDataDestroyDto the DTO that holds various parameters needed to initiate a business object data destroy
     *
     * @return the business object data information
     */
    public BusinessObjectData executeInitiateDestroyAfterStep(BusinessObjectDataDestroyDto businessObjectDataDestroyDto);

    /**
     * Executes S3 specific steps required for initiation of a business object data destroy.
     *
     * @param businessObjectDataDestroyDto the DTO that holds various parameters needed to initiate a business object data destroy
     */
    public void executeS3SpecificSteps(BusinessObjectDataDestroyDto businessObjectDataDestroyDto);

    /**
     * Prepares to initiate a business object data destroy process by validating specified business object data along with other related database entities. The
     * method also initializes business object data destroy DTO passed as a parameter.
     *
     * @param businessObjectDataDestroyDto the DTO that holds various parameters needed to initiate a business object data destroy
     * @param businessObjectDataKey the business object data key
     */
    public void prepareToInitiateDestroy(BusinessObjectDataDestroyDto businessObjectDataDestroyDto, BusinessObjectDataKey businessObjectDataKey);
}
