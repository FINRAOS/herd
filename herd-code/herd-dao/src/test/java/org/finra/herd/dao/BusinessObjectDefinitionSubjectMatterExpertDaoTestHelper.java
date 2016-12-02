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

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionSubjectMatterExpertEntity;

@Component
public class BusinessObjectDefinitionSubjectMatterExpertDaoTestHelper
{
    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDaoTestHelper businessObjectDefinitionDaoTestHelper;

    @Autowired
    private BusinessObjectDefinitionSubjectMatterExpertDao businessObjectDefinitionSubjectMatterExpertDao;

    /**
     * Creates and persists a new business object definition subject matter expert entity.
     *
     * @param key the business object definition subject matter expert key
     *
     * @return the newly created business object definition subject matter expert entity
     */
    public BusinessObjectDefinitionSubjectMatterExpertEntity createBusinessObjectDefinitionSubjectMatterExpertEntity(
        BusinessObjectDefinitionSubjectMatterExpertKey key)
    {
        // Get a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(key.getNamespace(), key.getBusinessObjectDefinitionName());

        // Create a business object definition entity if needed.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);
        if (businessObjectDefinitionEntity == null)
        {
            businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(businessObjectDefinitionKey, AbstractDaoTest.DATA_PROVIDER_NAME, AbstractDaoTest.DESCRIPTION);
        }

        return createBusinessObjectDefinitionSubjectMatterExpertEntity(businessObjectDefinitionEntity, key.getUserId());
    }

    /**
     * Creates and persists a new business object definition subject matter expert entity.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param userId the user id of the subject matter expert
     *
     * @return the newly created business object definition subject matter expert entity
     */
    public BusinessObjectDefinitionSubjectMatterExpertEntity createBusinessObjectDefinitionSubjectMatterExpertEntity(
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity, String userId)
    {
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity =
            new BusinessObjectDefinitionSubjectMatterExpertEntity();

        businessObjectDefinitionSubjectMatterExpertEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionSubjectMatterExpertEntity.setUserId(userId);

        return businessObjectDefinitionSubjectMatterExpertDao.saveAndRefresh(businessObjectDefinitionSubjectMatterExpertEntity);
    }
}
