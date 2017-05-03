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

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionEntity;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionLevelEntity;

@Component
public class GlobalAttributeDefinitionDaoTestHelper
{
    @Autowired
    private GlobalAttributeDefinitionDao globalAttributeDefinitionDao;

    @Autowired
    private GlobalAttributeDefinitionLevelDaoTestHelper globalAttributeDefinitionLevelDaoTestHelper;

    @Autowired
    private GlobalAttributeDefinitionLevelDao globalAttributeDefinitionLevelDao;

    /**
     * Creates and persists a new Global Attribute Definition entity.
     *
     * @param globalAttributeDefinitionLevel the level
     * @param globalAttributeDefinitionName the name
     *
     * @return the newly created Global Attribute Definition entity.
     */
    public GlobalAttributeDefinitionEntity createGlobalAttributeDefinitionEntity(String globalAttributeDefinitionLevel, String globalAttributeDefinitionName)
    {
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity = new GlobalAttributeDefinitionEntity();
        GlobalAttributeDefinitionLevelEntity globalAttributeDefinitionLevelEntity =
            globalAttributeDefinitionLevelDao.getGlobalAttributeDefinitionLevel(globalAttributeDefinitionLevel);
        if (globalAttributeDefinitionLevelEntity == null)
        {
            globalAttributeDefinitionLevelEntity =
                globalAttributeDefinitionLevelDaoTestHelper.createGlobalAttributeDefinitionLevelEntity(globalAttributeDefinitionLevel);
        }
        globalAttributeDefinitionEntity.setGlobalAttributeDefinitionLevel(globalAttributeDefinitionLevelEntity);
        globalAttributeDefinitionEntity.setGlobalAttributeDefinitionName(globalAttributeDefinitionName);
        return globalAttributeDefinitionDao.saveAndRefresh(globalAttributeDefinitionEntity);
    }

    /**
     * Returns a list of global attribute definition keys.
     *
     * @return the list of test global attribute definition keys
     */
    public List<GlobalAttributeDefinitionKey> getTestGlobalAttributeDefinitionKeys()
    {
        return Arrays
            .asList(new GlobalAttributeDefinitionKey(AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_NAME),
                new GlobalAttributeDefinitionKey(AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_NAME_2));
    }
}
