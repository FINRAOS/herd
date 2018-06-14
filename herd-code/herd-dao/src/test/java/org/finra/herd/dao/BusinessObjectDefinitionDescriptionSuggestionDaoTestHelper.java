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

import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;

@Component
public class BusinessObjectDefinitionDescriptionSuggestionDaoTestHelper
{
    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionDao businessObjectDefinitionDescriptionSuggestionDao;

    /**
     * Creates and persists a new business object definition description suggestion entity.
     *
     * @param businessObjectDefinitionEntity the business object definition entity associated with this business object definition description suggestion
     * @param descriptionSuggestion the business object definition description suggestion
     * @param userId the user id associated with this business object definition description suggestion
     * @param businessObjectDefinitionDescriptionSuggestionStatusEntity the status of the business object definition description suggestion
     *
     * @return the newly created business object definition description suggestion entity
     */
    public BusinessObjectDefinitionDescriptionSuggestionEntity createBusinessObjectDefinitionDescriptionSuggestionEntity(
        final BusinessObjectDefinitionEntity businessObjectDefinitionEntity, final String descriptionSuggestion, final String userId,
        final BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity)
    {
        // Create a new business object definition description suggestion entity and persist the new entity.
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(descriptionSuggestion);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(userId);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);
        businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(businessObjectDefinitionDescriptionSuggestionEntity);

        return businessObjectDefinitionDescriptionSuggestionEntity;
    }
}
