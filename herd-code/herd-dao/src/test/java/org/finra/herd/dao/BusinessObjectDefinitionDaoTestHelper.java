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
import org.finra.herd.model.api.xml.SampleDataFile;
import org.finra.herd.model.jpa.BusinessObjectDefinitionAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionSampleDataFileEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.StorageEntity;

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

    @Autowired
    private StorageDao storageDao;

    @Autowired
    private StorageDaoTestHelper storageDaoTestHelper;

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
            dataProviderName, businessObjectDefinitionDescription);
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
     * Creates and persists a new business object definition entity.
     *
     * @return the newly created business object definition entity
     */
    public BusinessObjectDefinitionEntity createBusinessObjectDefinitionEntity(String namespaceCode, String businessObjectDefinitionName,
        String dataProviderName, String businessObjectDefinitionDescription, List<Attribute> attributes)
    {
        return createBusinessObjectDefinitionEntity(namespaceCode, businessObjectDefinitionName, dataProviderName, businessObjectDefinitionDescription, null,
            attributes);
    }

    /**
     * Creates and persists a new business object definition entity.
     *
     * @return the newly created business object definition entity
     */
    public BusinessObjectDefinitionEntity createBusinessObjectDefinitionEntity(String namespaceCode, String businessObjectDefinitionName,
        String dataProviderName, String businessObjectDefinitionDescription, String displayName, List<Attribute> attributes)
    {
        return createBusinessObjectDefinitionEntity(namespaceCode, businessObjectDefinitionName, dataProviderName, businessObjectDefinitionDescription,
            displayName, attributes, null);
    }

    /**
     * Creates and persists a new business object definition entity.
     *
     * @return the newly created business object definition entity
     */
    public BusinessObjectDefinitionEntity createBusinessObjectDefinitionEntity(String namespaceCode, String businessObjectDefinitionName,
        String dataProviderName, String businessObjectDefinitionDescription, String displayName, List<Attribute> attributes,
        List<SampleDataFile> sampleDataFiles)
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
            displayName, attributes, sampleDataFiles);
    }

    /**
     * Creates and persists a new business object definition entity.
     *
     * @return the newly created business object definition entity
     */
    public BusinessObjectDefinitionEntity createBusinessObjectDefinitionEntity(NamespaceEntity namespaceEntity, String businessObjectDefinitionName,
        DataProviderEntity dataProviderEntity, String businessObjectDefinitionDescription, String displayName, List<Attribute> attributes,
        List<SampleDataFile> sampleDataFiles)
    {
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setDataProvider(dataProviderEntity);
        businessObjectDefinitionEntity.setName(businessObjectDefinitionName);
        businessObjectDefinitionEntity.setDescription(businessObjectDefinitionDescription);
        businessObjectDefinitionEntity.setDisplayName(displayName);

        // Create business object definition attribute entities if they are specified.
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

        // Create business object definition sample data file entities if they are specified.
        if (!CollectionUtils.isEmpty(sampleDataFiles))
        {
            // Create a storage entity if needed.
            StorageEntity storageEntity = storageDao.getStorageByName(AbstractDaoTest.STORAGE_NAME);
            if (storageEntity == null)
            {
                storageEntity = storageDaoTestHelper.createStorageEntity(AbstractDaoTest.STORAGE_NAME);
            }

            // Create sample data file entities.
            List<BusinessObjectDefinitionSampleDataFileEntity> sampleDataFileEntities = new ArrayList<>();
            businessObjectDefinitionEntity.setSampleDataFiles(sampleDataFileEntities);
            for (SampleDataFile sampleDataFile : sampleDataFiles)
            {
                BusinessObjectDefinitionSampleDataFileEntity sampleDataFileEntity = new BusinessObjectDefinitionSampleDataFileEntity();
                sampleDataFileEntities.add(sampleDataFileEntity);
                sampleDataFileEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
                sampleDataFileEntity.setDirectoryPath(sampleDataFile.getDirectoryPath());
                sampleDataFileEntity.setFileName(sampleDataFile.getFileName());
                sampleDataFileEntity.setFileSizeBytes(AbstractDaoTest.FILE_SIZE_1_KB);
                sampleDataFileEntity.setStorage(storageEntity);
            }
        }

        return businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);
    }

    /**
     * Returns a list of test business object definition keys expected to be returned by getBusinessObjectDefinitionKeys() method.
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
