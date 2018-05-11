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

import org.junit.Test;

import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

public class BusinessObjectDefinitionDescriptionSuggestionDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionByKey()
    {
        // Create and persist the relative database entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_CODE);
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceEntity.getCode(), BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create a new business object definition description suggestion entity and persist the new entity.
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(USER_ID);
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
}