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

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionDao;
import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionStatusDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;

/**
 * Helper for business object definition description suggestion related operations which require DAO.
 */
@Component
public class BusinessObjectDefinitionDescriptionSuggestionDaoHelper
{
    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionDao businessObjectDefinitionDescriptionSuggestionDao;

    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionStatusDao businessObjectDefinitionDescriptionSuggestionStatusDao;

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
                .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionAndUserId(businessObjectDefinitionEntity, userId);

        if (businessObjectDefinitionDescriptionSuggestionEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Business object definition description suggestion with the parameters " +
                    " {namespace=\"%s\", businessObjectDefinitionName=\"%s\", userId=\"%s\"} does not exist.",
                businessObjectDefinitionEntity.getNamespace().getCode(), businessObjectDefinitionEntity.getName(), userId));
        }

        return businessObjectDefinitionDescriptionSuggestionEntity;
    }


    /**
     * Gets a collection of business object definition description suggestions by business object definition and status.
     *
     * @param businessObjectDefinitionKey the business object definition key associated with the description suggestions
     * @param status the status of the business object definition description suggestions
     *
     * @return the business object definition description suggestions for the specified business object definition and status
     */
    public List<BusinessObjectDefinitionDescriptionSuggestionEntity> getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(
        final BusinessObjectDefinitionKey businessObjectDefinitionKey, final String status)
    {
        // Retrieve the business object definition and ensure it exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);

        // If the business object definition entity does not exist return an empty list.
        if (businessObjectDefinitionEntity == null)
        {
            return Lists.newArrayList();
        }

        // First get the status entity.
        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity = null;
        if (StringUtils.isNotBlank(status))
        {
            // Attempt to find the status entity associated with the status string.
            businessObjectDefinitionDescriptionSuggestionStatusEntity =
                businessObjectDefinitionDescriptionSuggestionStatusDao.getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(status);

            // If the status was not found in the status table return an empty list.
            if (businessObjectDefinitionDescriptionSuggestionStatusEntity == null)
            {
                return Lists.newArrayList();
            }
        }

        // The list of business object definition description suggestions.
        List<BusinessObjectDefinitionDescriptionSuggestionEntity> businessObjectDefinitionDescriptionSuggestionEntities =
            businessObjectDefinitionDescriptionSuggestionDao
                .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionEntity,
                    businessObjectDefinitionDescriptionSuggestionStatusEntity);

        // If business object definition description suggestion entities do not exist return an empty list.
        if (businessObjectDefinitionDescriptionSuggestionEntities == null)
        {
            return Lists.newArrayList();
        }

        return businessObjectDefinitionDescriptionSuggestionEntities;
    }
}
