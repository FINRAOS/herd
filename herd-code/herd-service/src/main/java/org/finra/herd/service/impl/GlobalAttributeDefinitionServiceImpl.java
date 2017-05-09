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
package org.finra.herd.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.GlobalAttributeDefinitionDao;
import org.finra.herd.dao.GlobalAttributeDefinitionLevelDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.GlobalAttributeDefinition;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionCreateRequest;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKeys;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionEntity;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionLevelEntity;
import org.finra.herd.service.GlobalAttributeDefinitionService;
import org.finra.herd.service.helper.AttributeValueListDaoHelper;
import org.finra.herd.service.helper.AttributeValueListHelper;
import org.finra.herd.service.helper.GlobalAttributeDefinitionDaoHelper;
import org.finra.herd.service.helper.GlobalAttributeDefinitionHelper;

/**
 * The global attribute definition service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class GlobalAttributeDefinitionServiceImpl implements GlobalAttributeDefinitionService
{
    @Autowired
    private GlobalAttributeDefinitionDao globalAttributeDefinitionDao;

    @Autowired
    private GlobalAttributeDefinitionDaoHelper globalAttributeDefinitionDaoHelper;

    @Autowired
    private GlobalAttributeDefinitionHelper globalAttributeDefinitionHelper;

    @Autowired
    private GlobalAttributeDefinitionLevelDao globalAttributeDefinitionLevelDao;

    @Autowired
    private AttributeValueListHelper attributeValueListHelper;

    @Autowired
    private AttributeValueListDaoHelper attributeValueListDaoHelper;

    @Override
    public GlobalAttributeDefinition createGlobalAttributeDefinition(GlobalAttributeDefinitionCreateRequest request)
    {
        // Validate and trim the request parameters.
        validateGlobalAttributeDefinitionCreateRequest(request);

        // Validate the global Attribute Definition entity does not already exist in the database.
        globalAttributeDefinitionDaoHelper.validateGlobalAttributeDefinitionNoExists(request.getGlobalAttributeDefinitionKey());

        //Get the existing global Attribute Definition level entity
        GlobalAttributeDefinitionLevelEntity globalAttributeDefinitionLevelEntity =
            globalAttributeDefinitionLevelDao.getGlobalAttributeDefinitionLevel(request.getGlobalAttributeDefinitionKey().getGlobalAttributeDefinitionLevel());

        AttributeValueListEntity attributeValueListEntity = null;
        //Get the attribute value list if the attribute value key exists
        if (request.getAttributeValueListKey() != null)
        {
            //Get the existing attribute list and ensure it exists
            attributeValueListEntity = attributeValueListDaoHelper.getAttributeValueListEntity(request.getAttributeValueListKey());
        }

        // Create and persist a new global Attribute Definition entity from the request information.
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            createGlobalAttributeDefinitionEntity(request.getGlobalAttributeDefinitionKey(), globalAttributeDefinitionLevelEntity, attributeValueListEntity);

        // Create and return the global Attribute Definition object from the persisted entity.
        return createGlobalAttributeDefinitionFromEntity(globalAttributeDefinitionEntity);
    }

    @Override
    public GlobalAttributeDefinition deleteGlobalAttributeDefinition(GlobalAttributeDefinitionKey globalAttributeDefinitionKey)
    {
        // Perform validation and trim.
        globalAttributeDefinitionHelper.validateGlobalAttributeDefinitionKey(globalAttributeDefinitionKey);

        // Retrieve and ensure that a global Attribute Definition already exists with the specified key.
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDaoHelper.getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey);

        // Delete the global Attribute Definition.
        globalAttributeDefinitionDao.delete(globalAttributeDefinitionEntity);

        // Create and return the global Attribute Definition object from the deleted entity.
        return createGlobalAttributeDefinitionFromEntity(globalAttributeDefinitionEntity);
    }

    @Override
    public GlobalAttributeDefinitionKeys getGlobalAttributeDefinitionKeys()
    {
        return new GlobalAttributeDefinitionKeys(globalAttributeDefinitionDao.getAllGlobalAttributeDefinitionKeys());
    }

    @Override
    public GlobalAttributeDefinition getGlobalAttributeDefinition(GlobalAttributeDefinitionKey globalAttributeDefinitionKey)
    {
        // Perform validation and trim.
        globalAttributeDefinitionHelper.validateGlobalAttributeDefinitionKey(globalAttributeDefinitionKey);

        // Retrieve and ensure that a global Attribute Definition already exists with the specified key.
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDaoHelper.getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey);
        
        // Create and return the global Attribute Definition object from the deleted entity.
        return createGlobalAttributeDefinitionFromEntity(globalAttributeDefinitionEntity);
    }

    /**
     * Validates the global Attribute Definition create request. This method also trims the request parameters. Currently only format level is supported
     *
     * @param request the global Attribute Definition create request
     *
     * @throws IllegalArgumentException throws exception if any other level other than format is specified
     */
    private void validateGlobalAttributeDefinitionCreateRequest(GlobalAttributeDefinitionCreateRequest request)
    {
        Assert.notNull(request, "A global attribute definition create request must be specified.");
        globalAttributeDefinitionHelper.validateGlobalAttributeDefinitionKey(request.getGlobalAttributeDefinitionKey());
        if (!GlobalAttributeDefinitionLevelEntity.GlobalAttributeDefinitionLevels.BUS_OBJCT_FRMT.name()
            .equalsIgnoreCase(request.getGlobalAttributeDefinitionKey().getGlobalAttributeDefinitionLevel()))
        {
            throw new IllegalArgumentException(String.format("Global attribute definition with level \"%s\" is not supported.",
                request.getGlobalAttributeDefinitionKey().getGlobalAttributeDefinitionLevel()));
        }
        if (request.getAttributeValueListKey() != null)
        {
            attributeValueListHelper.validateAttributeValueListKey(request.getAttributeValueListKey());
        }
    }

    private GlobalAttributeDefinition createGlobalAttributeDefinitionFromEntity(GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity)
    {
        GlobalAttributeDefinition globalAttributeDefinition = new GlobalAttributeDefinition();
        globalAttributeDefinition.setId(globalAttributeDefinitionEntity.getId());

        GlobalAttributeDefinitionKey globalAttributeDefinitionKey = new GlobalAttributeDefinitionKey();
        globalAttributeDefinitionKey
            .setGlobalAttributeDefinitionLevel(globalAttributeDefinitionEntity.getGlobalAttributeDefinitionLevel().getGlobalAttributeDefinitionLevel());
        globalAttributeDefinitionKey.setGlobalAttributeDefinitionName(globalAttributeDefinitionEntity.getGlobalAttributeDefinitionName());
        globalAttributeDefinition.setGlobalAttributeDefinitionKey(globalAttributeDefinitionKey);
        AttributeValueListEntity attributeValueListEntity = globalAttributeDefinitionEntity.getAttributeValueListEntity();
        if (attributeValueListEntity != null)
        {
            globalAttributeDefinition.setAttributeValueList(attributeValueListDaoHelper.createAttributeValueListFromEntity(attributeValueListEntity));
        }

        return globalAttributeDefinition;
    }

    /**
     * Creates and persists a new global Attribute Definition entity.
     *
     * @param globalAttributeDefinitionKey the global Attribute Definition key
     * @param globalAttributeDefinitionLevelEntity the global attribute definition level entity
     * @param attributeValueListEntity the attribute list entity (optional)
     *
     * @return the newly created global Attribute Definition entity
     */
    private GlobalAttributeDefinitionEntity createGlobalAttributeDefinitionEntity(GlobalAttributeDefinitionKey globalAttributeDefinitionKey,
        GlobalAttributeDefinitionLevelEntity globalAttributeDefinitionLevelEntity, AttributeValueListEntity attributeValueListEntity)
    {
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity = new GlobalAttributeDefinitionEntity();
        globalAttributeDefinitionEntity.setGlobalAttributeDefinitionLevel(globalAttributeDefinitionLevelEntity);
        globalAttributeDefinitionEntity.setGlobalAttributeDefinitionName(globalAttributeDefinitionKey.getGlobalAttributeDefinitionName());
        globalAttributeDefinitionEntity.setAttributeValueListEntity(attributeValueListEntity);
        return globalAttributeDefinitionDao.saveAndRefresh(globalAttributeDefinitionEntity);
    }
}
