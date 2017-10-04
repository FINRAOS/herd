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
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateResponse;

/**
 * The business object data storage unit status service.
 */
public interface BusinessObjectDataStorageUnitStatusService
{
    /**
     * Updates status of a business object data storage unit.
     *
     * @param businessObjectDataStorageUnitKey the business object data storage unit key
     * @param request the business object data storage unit status update request
     *
     * @return the business object data storage unit status update response
     */
    public BusinessObjectDataStorageUnitStatusUpdateResponse updateBusinessObjectDataStorageUnitStatus(
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey, BusinessObjectDataStorageUnitStatusUpdateRequest request);
}
