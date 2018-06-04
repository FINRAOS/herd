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

import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionStatusDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity;

/**
 * Helper for business object definition description suggestion status operations which require DAO.
 */
@Component
public class BusinessObjectDefinitionDescriptionSuggestionStatusDaoHelper
{
    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionStatusDao businessObjectDefinitionDescriptionSuggestionStatusDao;

    /**
     * Gets a business object definition description suggestion status entity by its code and ensure it exists.
     *
     * @param code the business object definition description suggestion status code (case insensitive)
     *
     * @return the business object definition description suggestion status entity
     * @throws ObjectNotFoundException if the business object definition description suggestion status entity doesn't exist
     */
    public BusinessObjectDefinitionDescriptionSuggestionStatusEntity getBusinessObjectDefinitionDescriptionSuggestionStatusEntity(String code)
        throws ObjectNotFoundException
    {
        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            businessObjectDefinitionDescriptionSuggestionStatusDao.getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(code);

        if (businessObjectDefinitionDescriptionSuggestionStatusEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Business object definition description suggestion status with code \"%s\" doesn't exist.", code));
        }

        return businessObjectDefinitionDescriptionSuggestionStatusEntity;
    }
}
