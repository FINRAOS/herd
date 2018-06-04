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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

public class BusinessObjectDefinitionDescriptionSuggestionDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionByKey()
    {
        // Create and persist the relative database entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceEntity.getCode(), BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            businessObjectDefinitionDescriptionSuggestionStatusDaoTestHelper
                .createBusinessObjectDefinitionDescriptionSuggestionStatusEntity(BDEF_DESCRIPTION_SUGGESTION_STATUS);

        // Create a new business object definition description suggestion entity and persist the new entity.
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(USER_ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);
        businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(businessObjectDefinitionDescriptionSuggestionEntity);

        // Get a business object definition description suggestion and validate the result.
        assertEquals(businessObjectDefinitionDescriptionSuggestionEntity, businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionEntityAndUserId(businessObjectDefinitionEntity, USER_ID));

        // Test case insensitivity of user id.
        assertEquals(businessObjectDefinitionDescriptionSuggestionEntity, businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionEntityAndUserId(businessObjectDefinitionEntity, USER_ID.toUpperCase()));

        // Try to retrieve a business object definition description suggestion using invalid input parameters.
        assertNull(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionEntityAndUserId(businessObjectDefinitionEntity, "I_DO_NOT_EXIST"));
        assertNull(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionEntityAndUserId(null, USER_ID));
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionEntityAndUserId()
    {
        // Create and persist the relative database entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceEntity.getCode(), BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity1 =
            businessObjectDefinitionDescriptionSuggestionStatusDaoTestHelper
                .createBusinessObjectDefinitionDescriptionSuggestionStatusEntity(BDEF_DESCRIPTION_SUGGESTION_STATUS);

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity2 =
            businessObjectDefinitionDescriptionSuggestionStatusDaoTestHelper
                .createBusinessObjectDefinitionDescriptionSuggestionStatusEntity(BDEF_DESCRIPTION_SUGGESTION_STATUS_2);

        // Create new business object definition description suggestion entities and persist the new entities.
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(USER_ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity1);
        businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(businessObjectDefinitionDescriptionSuggestionEntity);

        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity2 =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity2.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity2.setDescriptionSuggestion(DESCRIPTION_SUGGESTION_2);
        businessObjectDefinitionDescriptionSuggestionEntity2.setUserId(USER_ID_2);
        businessObjectDefinitionDescriptionSuggestionEntity2.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity2);
        businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(businessObjectDefinitionDescriptionSuggestionEntity2);

        // Create business object definition description suggestion keys.
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey2 =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID_2);

        // Create a list of business object definition description suggestion keys.
        List<BusinessObjectDefinitionDescriptionSuggestionKey> businessObjectDefinitionDescriptionSuggestionKeys =
            Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionKey, businessObjectDefinitionDescriptionSuggestionKey2);

        // Get business object definition description suggestion keys and validate the result.
        List<BusinessObjectDefinitionDescriptionSuggestionKey> results = businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionEntity(businessObjectDefinitionEntity);

        // Validate results.
        for (int i = 0; i < results.size(); i++)
        {
            assertEquals(businessObjectDefinitionDescriptionSuggestionKeys.get(i).getNamespace(), results.get(i).getNamespace());
            assertEquals(businessObjectDefinitionDescriptionSuggestionKeys.get(i).getBusinessObjectDefinitionName(),
                results.get(i).getBusinessObjectDefinitionName());
            assertEquals(businessObjectDefinitionDescriptionSuggestionKeys.get(i).getUserId(), results.get(i).getUserId());
        }

        // Try to retrieve business object definition description suggestion keys using invalid input parameters.
        assertEquals(
            businessObjectDefinitionDescriptionSuggestionDao.getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionEntity(null).size(), 0);
    }
}