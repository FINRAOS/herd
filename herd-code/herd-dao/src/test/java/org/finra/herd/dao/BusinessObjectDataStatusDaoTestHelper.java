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
package org.finra.herd.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;

@Component
public class BusinessObjectDataStatusDaoTestHelper
{
    @Autowired
    private BusinessObjectDataStatusDao businessObjectDataStatusDao;

    /**
     * Creates and persists a new business object data status entity.
     *
     * @param statusCode the code of the business object data status
     *
     * @return the newly created business object data status entity
     */
    public BusinessObjectDataStatusEntity createBusinessObjectDataStatusEntity(String statusCode)
    {
        return createBusinessObjectDataStatusEntity(statusCode, AbstractDaoTest.DESCRIPTION, AbstractDaoTest.NO_BDATA_STATUS_PRE_REGISTRATION_FLAG_SET);
    }

    /**
     * Creates and persists a new business object data status entity.
     *
     * @param statusCode the code of the business object data status
     * @param description the description of the business object data status
     * @param preRegistrationStatus specifies if this business object data status is flagged as a pre-registration status
     *
     * @return the newly created business object data status entity
     */
    public BusinessObjectDataStatusEntity createBusinessObjectDataStatusEntity(String statusCode, String description, Boolean preRegistrationStatus)
    {
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(statusCode);
        businessObjectDataStatusEntity.setDescription(description);
        businessObjectDataStatusEntity.setPreRegistrationStatus(preRegistrationStatus);
        return businessObjectDataStatusDao.saveAndRefresh(businessObjectDataStatusEntity);
    }
}
