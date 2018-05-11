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
package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;

/**
 * Helper for business object definition description suggestion related operations which require DAO.
 */
@Component
public class BusinessObjectDefinitionDescriptionSuggestionDaoHelper
{
    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionDao businessObjectDefinitionDescriptionSuggestionDao;

    /**
     * Gets a business object definition description suggestion entity on the key and makes sure that it exists.
     *
     * @param businessObjectDefinitionEntity the business object definition entity associated with the business object definition description suggestion
     * @param userId the userId associated with the business object definition description suggestion
     *
     * @return the business object definition description suggestion entity
     */
    public BusinessObjectDefinitionDescriptionSuggestionEntity getBusinessObjectDefinitionDescriptionSuggestionEntity(
        final BusinessObjectDefinitionEntity businessObjectDefinitionEntity, final String userId)
    {
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            businessObjectDefinitionDescriptionSuggestionDao
                .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionEntityAndUserId(businessObjectDefinitionEntity, userId);

        if (businessObjectDefinitionDescriptionSuggestionEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Business object definition description suggestion with the parameters " +
                    " {namespace=\"%s\", businessObjectDefinitionName=\"%s\", userId=\"%s\"} does not exist.",
                businessObjectDefinitionEntity.getNamespace().getCode(), businessObjectDefinitionEntity.getName(), userId));
        }

        return businessObjectDefinitionDescriptionSuggestionEntity;
    }
}
