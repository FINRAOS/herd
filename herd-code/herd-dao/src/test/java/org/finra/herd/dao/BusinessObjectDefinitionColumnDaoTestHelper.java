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

import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionColumnEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;

@Component
public class BusinessObjectDefinitionColumnDaoTestHelper
{
    @Autowired
    private BusinessObjectDefinitionColumnDao businessObjectDefinitionColumnDao;

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDaoTestHelper businessObjectDefinitionDaoTestHelper;

    /**
     * Creates and persists a new business object definition column entity.
     *
     * @param businessObjectDefinitionColumnKey the business object definition column key
     * @param businessObjectDefinitionColumnDescription the description of the business object definition column
     *
     * @return the newly created business object definition column entity
     */
    public BusinessObjectDefinitionColumnEntity createBusinessObjectDefinitionColumnEntity(BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey,
        String businessObjectDefinitionColumnDescription)
    {
        // Create a business object definition column.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(businessObjectDefinitionColumnKey.getNamespace(),
            businessObjectDefinitionColumnKey.getBusinessObjectDefinitionName());

        // Create a business object definition entity if needed.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);
        if (businessObjectDefinitionEntity == null)
        {
            businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(businessObjectDefinitionKey, AbstractDaoTest.DATA_PROVIDER_NAME, AbstractDaoTest.DESCRIPTION);
        }

        return createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionEntity,
            businessObjectDefinitionColumnKey.getBusinessObjectDefinitionColumnName(), businessObjectDefinitionColumnDescription);
    }

    /**
     * Creates and persists a new business object definition column entity.
     *
     * @param businessObjectDefinitionColumnKey the business object definition column key
     * @param businessObjectDefinitionColumnDescription the description of the business object definition column
     * @param businessObjectDefinitionColumnSchemaColumnName the business object definition column schema column name
     *
     * @return the newly created business object definition column entity
     */
    public BusinessObjectDefinitionColumnEntity createBusinessObjectDefinitionColumnEntity(BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey,
        String businessObjectDefinitionColumnDescription, String businessObjectDefinitionColumnSchemaColumnName)
    {
        // Create a business object definition column.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(businessObjectDefinitionColumnKey.getNamespace(),
            businessObjectDefinitionColumnKey.getBusinessObjectDefinitionName());

        // Create a business object definition entity if needed.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);
        if (businessObjectDefinitionEntity == null)
        {
            businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(businessObjectDefinitionKey, AbstractDaoTest.DATA_PROVIDER_NAME, AbstractDaoTest.DESCRIPTION);
        }

        return createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionEntity,
            businessObjectDefinitionColumnKey.getBusinessObjectDefinitionColumnName(), businessObjectDefinitionColumnDescription,
            businessObjectDefinitionColumnSchemaColumnName);
    }

    /**
     * Creates and persists a new business object definition column entity.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param businessObjectDefinitionColumnName the name of the business object definition column
     * @param businessObjectDefinitionColumnDescription the description of the business object definition column
     *
     * @return the newly created business object definition column entity
     */
    public BusinessObjectDefinitionColumnEntity createBusinessObjectDefinitionColumnEntity(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        String businessObjectDefinitionColumnName, String businessObjectDefinitionColumnDescription)
    {
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity = new BusinessObjectDefinitionColumnEntity();

        businessObjectDefinitionColumnEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionColumnEntity.setName(businessObjectDefinitionColumnName);
        businessObjectDefinitionColumnEntity.setDescription(businessObjectDefinitionColumnDescription);

        return businessObjectDefinitionColumnDao.saveAndRefresh(businessObjectDefinitionColumnEntity);
    }

    /**
     * Creates and persists a new business object definition column entity.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param businessObjectDefinitionColumnName the name of the business object definition column
     * @param businessObjectDefinitionColumnDescription the description of the business object definition column
     * @param businessObjectDefinitionColumnSchemaColumnName the business object definition column schema column name
     *
     * @return the newly created business object definition column entity
     */
    public BusinessObjectDefinitionColumnEntity createBusinessObjectDefinitionColumnEntity(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        String businessObjectDefinitionColumnName, String businessObjectDefinitionColumnDescription, String businessObjectDefinitionColumnSchemaColumnName)
    {
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity = new BusinessObjectDefinitionColumnEntity();

        businessObjectDefinitionColumnEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionColumnEntity.setName(businessObjectDefinitionColumnName);
        businessObjectDefinitionColumnEntity.setDescription(businessObjectDefinitionColumnDescription);
        businessObjectDefinitionColumnEntity.setSchemaColumnName(businessObjectDefinitionColumnSchemaColumnName);

        return businessObjectDefinitionColumnDao.saveAndRefresh(businessObjectDefinitionColumnEntity);
    }
}
