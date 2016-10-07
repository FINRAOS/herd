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

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

@Component
public class BusinessObjectDefinitionDaoTestHelper
{
    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private DataProviderDao dataProviderDao;

    @Autowired
    private DataProviderDaoTestHelper dataProviderDaoTestHelper;

    @Autowired
    private NamespaceDao namespaceDao;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    /**
     * Creates and persists a new business object definition.
     *
     * @return the newly created business object definition.
     */
    public BusinessObjectDefinitionEntity createBusinessObjectDefinition()
    {
        String businessObjectDefinitionName = "BusObjDefTest" + AbstractDaoTest.getRandomSuffix();
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceDaoTestHelper.createNamespaceEntity());
        businessObjectDefinitionEntity.setDataProvider(dataProviderDaoTestHelper.createDataProviderEntity());
        businessObjectDefinitionEntity.setName(businessObjectDefinitionName);
        businessObjectDefinitionEntity.setDescription("test");
        return businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);
    }

    /**
     * Creates and persists a new business object definition entity.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param dataProviderName the name of the data provider
     * @param businessObjectDefinitionDescription the description of the business object definition
     *
     * @return the newly created business object definition entity
     */
    public BusinessObjectDefinitionEntity createBusinessObjectDefinitionEntity(BusinessObjectDefinitionKey businessObjectDefinitionKey, String dataProviderName,
        String businessObjectDefinitionDescription)
    {
        return createBusinessObjectDefinitionEntity(businessObjectDefinitionKey.getNamespace(), businessObjectDefinitionKey.getBusinessObjectDefinitionName(),
            dataProviderName, businessObjectDefinitionDescription, null);
    }

    /**
     * Creates and persists a new business object definition.
     *
     * @return the newly created business object definition.
     */
    public BusinessObjectDefinitionEntity createBusinessObjectDefinitionEntity(String namespaceCode, String businessObjectDefinitionName,
        String dataProviderName, String businessObjectDefinitionDescription)
    {
        return createBusinessObjectDefinitionEntity(namespaceCode, businessObjectDefinitionName, dataProviderName, businessObjectDefinitionDescription, null);
    }

    /**
     * Creates and persists a new business object definition.
     *
     * @return the newly created business object definition.
     */
    public BusinessObjectDefinitionEntity createBusinessObjectDefinitionEntity(String namespaceCode, String businessObjectDefinitionName,
        String dataProviderName, String businessObjectDefinitionDescription, String displayName, List<Attribute> attributes)
    {
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(namespaceCode, businessObjectDefinitionName, dataProviderName, businessObjectDefinitionDescription,
                attributes);
        businessObjectDefinitionEntity.setDisplayName(displayName);
        return businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);
    }

    /**
     * Creates and persists a new business object definition.
     *
     * @return the newly created business object definition.
     */
    public BusinessObjectDefinitionEntity createBusinessObjectDefinitionEntity(String namespaceCode, String businessObjectDefinitionName,
        String dataProviderName, String businessObjectDefinitionDescription, List<Attribute> attributes)
    {
        // Create a namespace entity if needed.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(namespaceCode);
        if (namespaceEntity == null)
        {
            namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(namespaceCode);
        }

        // Create a data provider entity if needed.
        DataProviderEntity dataProviderEntity = dataProviderDao.getDataProviderByName(dataProviderName);
        if (dataProviderEntity == null)
        {
            dataProviderEntity = dataProviderDaoTestHelper.createDataProviderEntity(dataProviderName);
        }

        return createBusinessObjectDefinitionEntity(namespaceEntity, businessObjectDefinitionName, dataProviderEntity, businessObjectDefinitionDescription,
            attributes);
    }

    /**
     * Creates and persists a new business object definition.
     *
     * @return the newly created business object definition.
     */
    public BusinessObjectDefinitionEntity createBusinessObjectDefinitionEntity(NamespaceEntity namespaceEntity, String businessObjectDefinitionName,
        DataProviderEntity dataProviderEntity, String businessObjectDefinitionDescription, List<Attribute> attributes)
    {
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setDataProvider(dataProviderEntity);
        businessObjectDefinitionEntity.setName(businessObjectDefinitionName);
        businessObjectDefinitionEntity.setDescription(businessObjectDefinitionDescription);

        // Create the attributes if they are specified.
        if (!CollectionUtils.isEmpty(attributes))
        {
            List<BusinessObjectDefinitionAttributeEntity> attributeEntities = new ArrayList<>();
            businessObjectDefinitionEntity.setAttributes(attributeEntities);
            for (Attribute attribute : attributes)
            {
                BusinessObjectDefinitionAttributeEntity attributeEntity = new BusinessObjectDefinitionAttributeEntity();
                attributeEntities.add(attributeEntity);
                attributeEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
                attributeEntity.setName(attribute.getName());
                attributeEntity.setValue(attribute.getValue());
            }
        }

        return businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);
    }

    /**
     * Returns a list of test business object definition keys expected to be returned by getBusinessObjectDefinitions() method.
     *
     * @return the list of expected business object definition keys
     */
    public List<BusinessObjectDefinitionKey> getExpectedBusinessObjectDefinitionKeys()
    {
        List<BusinessObjectDefinitionKey> keys = new ArrayList<>();

        keys.add(new BusinessObjectDefinitionKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME));
        keys.add(new BusinessObjectDefinitionKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME_2));

        return keys;
    }

    /**
     * Returns a list of test business object definition keys.
     *
     * @return the list of test business object definition keys
     */
    public List<BusinessObjectDefinitionKey> getTestBusinessObjectDefinitionKeys()
    {
        List<BusinessObjectDefinitionKey> keys = new ArrayList<>();

        keys.add(new BusinessObjectDefinitionKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME_2));
        keys.add(new BusinessObjectDefinitionKey(AbstractDaoTest.NAMESPACE_2, AbstractDaoTest.BDEF_NAME_2));
        keys.add(new BusinessObjectDefinitionKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME));
        keys.add(new BusinessObjectDefinitionKey(AbstractDaoTest.NAMESPACE_2, AbstractDaoTest.BDEF_NAME));

        return keys;
    }
}
