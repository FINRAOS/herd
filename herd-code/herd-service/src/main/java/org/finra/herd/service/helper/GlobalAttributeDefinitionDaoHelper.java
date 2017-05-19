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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.GlobalAttributeDefinitionDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.jpa.AllowedAttributeValueEntity;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionEntity;

@Component
public class GlobalAttributeDefinitionDaoHelper
{
    @Autowired
    private GlobalAttributeDefinitionDao globalAttributeDefinitionDao;

    /**
     * Gets a global Attribute Definition entity and ensure it exists.
     *
     * @param globalAttributeDefinitionKey the global Attribute Definition key (case insensitive)
     *
     * @return the global Attribute Definition entity
     * @throws ObjectNotFoundException if the global Attribute Definition entity doesn't exist
     */
    public GlobalAttributeDefinitionEntity getGlobalAttributeDefinitionEntity(GlobalAttributeDefinitionKey globalAttributeDefinitionKey)
        throws ObjectNotFoundException
    {
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey);

        if (globalAttributeDefinitionEntity == null)
        {
            throw new ObjectNotFoundException(String
                .format("Global attribute definition with level \"%s\" doesn't exist for global attribute definition name \"%s\".",
                    globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel(), globalAttributeDefinitionKey.getGlobalAttributeDefinitionName()));
        }

        return globalAttributeDefinitionEntity;
    }

    /**
     * Gets a global Attribute Definition entity and checks if it already exists.
     *
     * @param globalAttributeDefinitionKey the global Attribute Definition key (case insensitive)
     *
     * @throws AlreadyExistsException if the global Attribute Definition entity already exist
     */
    public void validateGlobalAttributeDefinitionNoExists(GlobalAttributeDefinitionKey globalAttributeDefinitionKey) throws AlreadyExistsException
    {
        // Validate that the global attribute definition entity does not already exist.
        if (globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey) != null)
        {
            throw new AlreadyExistsException(String.format("Unable to create global attribute definition with global attribute definition level \"%s\" and " +
                    "global attribute definition name \"%s\" because it already exists.",
                globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel(), globalAttributeDefinitionKey.getGlobalAttributeDefinitionName()));
        }
    }

    /**
     * Gets allowed attribute values for the global attribute definition
     *
     * @param globalAttributeDefinitionKey the global attribute definition key
     * 
     * @return list of allowed attribute values, if the global attribute definition does not have attribute list returns null
     */
    public List<String> getAllowedAttributeValues(GlobalAttributeDefinitionKey globalAttributeDefinitionKey)
    {
        List<String> allowedAttributeValues = null;
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey);

        if (globalAttributeDefinitionEntity.getAttributeValueList() != null)
        {
            allowedAttributeValues = new ArrayList<>();
            Collection<AllowedAttributeValueEntity> list = globalAttributeDefinitionEntity.getAttributeValueList().getAllowedAttributeValues();
            for (AllowedAttributeValueEntity allowedAttributeValueEntity : list)
            {
                allowedAttributeValues.add(allowedAttributeValueEntity.getAllowedAttributeValue());
            }
        }
        
        return allowedAttributeValues;
    }
}
