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
package org.finra.herd.service;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.BusinessObjectDefinitionDaoTestHelper;
import org.finra.herd.dao.FileTypeDaoTestHelper;
import org.finra.herd.dao.StorageDaoTestHelper;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.impl.BusinessObjectDataServiceImpl;

public class RelationalTableRegistrationServiceTest extends AbstractServiceTest
{
    @Autowired
    private BusinessObjectDefinitionDaoTestHelper businessObjectDefinitionDaoTestHelper;

    @Autowired
    private StorageDaoTestHelper storageDaoTestHelper;

    @Autowired
    private RelationalTableRegistrationService relationalTableRegistrationService;

    @Autowired
    private FileTypeDaoTestHelper fileTypeDaoTestHelper;

    @Autowired
    private BusinessObjectFormatService businessObjectFormatService;

    @Before
    public void setupData()
    {
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
            businessObjectDefinitionServiceTestHelper.getNewAttributes());
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.RELATIONAL);
        fileTypeDaoTestHelper.createFileTypeEntity(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
    }

    @Test
    public void testCreateRelationalTableRegistration()
    {
        String businessObjectFormatAttributeName =
            configurationHelper.getProperty(ConfigurationValue.RELATIONAL_TABLE_BUSINESS_OBJECT_FORMAT_ATTRIBUTE_NAME, String.class);

        RelationalTableRegistrationCreateRequest createRequest = new RelationalTableRegistrationCreateRequest();
        createRequest.setNamespace(BDEF_NAMESPACE);
        createRequest.setDataProviderName(DATA_PROVIDER_NAME);
        createRequest.setBusinessObjectDefinitionName(BDEF_NAME);
        createRequest.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        createRequest.setRelationalTableName(RELATIONAL_TABLE_NAME);
        createRequest.setStorageName(STORAGE_NAME);

        BusinessObjectData businessObjectData = relationalTableRegistrationService.createRelationalTableRegistration(createRequest);
        BusinessObjectData expectedBusinessObjectData = new BusinessObjectData();
        expectedBusinessObjectData.setId(businessObjectData.getId());
        expectedBusinessObjectData.setNamespace(BDEF_NAMESPACE);
        expectedBusinessObjectData.setBusinessObjectDefinitionName(BDEF_NAME);
        expectedBusinessObjectData.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        expectedBusinessObjectData.setVersion(0);
        expectedBusinessObjectData.setStatus("VALID");
        expectedBusinessObjectData.setLatestVersion(true);
        expectedBusinessObjectData.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        expectedBusinessObjectData.setPartitionValue(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE);
        expectedBusinessObjectData.setPartitionKey(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY);

        StorageUnit expectedStorageUnit = new StorageUnit();
        Storage expectedStorage = new Storage();
        expectedStorage.setName(STORAGE_NAME);
        expectedStorage.setStoragePlatformName(StoragePlatformEntity.RELATIONAL);
        expectedStorageUnit.setStorage(expectedStorage);
        expectedStorageUnit.setStorageUnitStatus(StorageUnitStatusEntity.ENABLED);

        expectedBusinessObjectData.setStorageUnits(Arrays.asList(expectedStorageUnit));
        expectedBusinessObjectData.setSubPartitionValues(new ArrayList<>());
        expectedBusinessObjectData.setAttributes(new ArrayList<>());
        expectedBusinessObjectData.setBusinessObjectDataParents(new ArrayList<>());
        expectedBusinessObjectData.setBusinessObjectDataChildren(new ArrayList<>());

        assertEquals(businessObjectData, expectedBusinessObjectData);

        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey();
        businessObjectFormatKey.setNamespace(BDEF_NAMESPACE);
        businessObjectFormatKey.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectFormatKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectFormatKey.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        businessObjectFormatKey.setBusinessObjectFormatVersion(0);

        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.getBusinessObjectFormat(businessObjectFormatKey);
        BusinessObjectFormat expectedBusinessObjectFormat = new BusinessObjectFormat();
        expectedBusinessObjectFormat.setId(businessObjectFormat.getId());
        expectedBusinessObjectFormat.setNamespace(BDEF_NAMESPACE);
        expectedBusinessObjectFormat.setBusinessObjectDefinitionName(BDEF_NAME);
        expectedBusinessObjectFormat.setPartitionKey(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY);
        expectedBusinessObjectFormat.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        expectedBusinessObjectFormat.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        expectedBusinessObjectFormat.setBusinessObjectFormatVersion(0);
        expectedBusinessObjectFormat.setLatestVersion(true);
        expectedBusinessObjectFormat.setBusinessObjectFormatParents(new ArrayList<>());
        expectedBusinessObjectFormat.setBusinessObjectFormatChildren(new ArrayList<>());
        expectedBusinessObjectFormat.setAttributeDefinitions(new ArrayList<>());
        expectedBusinessObjectFormat.setAttributes(Arrays.asList(new Attribute(businessObjectFormatAttributeName, RELATIONAL_TABLE_NAME)));

        assertEquals(businessObjectFormat, expectedBusinessObjectFormat);
    }

    @Test
    public void testCreateRelationalTableRegistrationMissingRequiredParameters()
    {
        RelationalTableRegistrationCreateRequest createRequest = new RelationalTableRegistrationCreateRequest();
        createRequest.setNamespace(BDEF_NAMESPACE);
        createRequest.setDataProviderName(DATA_PROVIDER_NAME);
        createRequest.setBusinessObjectDefinitionName(BDEF_NAME);
        createRequest.setRelationalTableName(RELATIONAL_TABLE_NAME);
        createRequest.setStorageName(STORAGE_NAME);
        createRequest.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);

        try
        {
            createRequest.setNamespace(null);
            relationalTableRegistrationService.createRelationalTableRegistration(createRequest);
            fail();
        }
        catch (IllegalArgumentException ex)
        {
            Assert.assertEquals("A namespace must be specified.", ex.getMessage());
        }

        createRequest.setNamespace(BDEF_NAMESPACE);
        try
        {
            createRequest.setBusinessObjectDefinitionName("");
            relationalTableRegistrationService.createRelationalTableRegistration(createRequest);
            fail();
        }
        catch (IllegalArgumentException ex)
        {
            Assert.assertEquals("A business object definition name must be specified.", ex.getMessage());
        }

        createRequest.setBusinessObjectDefinitionName(BDEF_NAME);
        try
        {
            createRequest.setBusinessObjectFormatUsage("  ");
            relationalTableRegistrationService.createRelationalTableRegistration(createRequest);
            fail();
        }
        catch (IllegalArgumentException ex)
        {
            Assert.assertEquals("A business object format usage must be specified.", ex.getMessage());
        }
    }

    @Test
    public void testCreateRelationalTableRegistrationRequiredEntityNotFound()
    {
        RelationalTableRegistrationCreateRequest createRequest = new RelationalTableRegistrationCreateRequest();
        createRequest.setNamespace(BDEF_NAMESPACE);
        createRequest.setDataProviderName(DATA_PROVIDER_NAME);
        createRequest.setBusinessObjectDefinitionName(BDEF_NAME);
        createRequest.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        createRequest.setRelationalTableName(RELATIONAL_TABLE_NAME);
        createRequest.setStorageName(STORAGE_NAME);

        createRequest.setNamespace(BDEF_NAMESPACE_2);
        try
        {
            relationalTableRegistrationService.createRelationalTableRegistration(createRequest);
            fail();
        }
        catch (ObjectNotFoundException ex)
        {
            Assert.assertEquals(String.format("Namespace \"%s\" doesn't exist.", BDEF_NAMESPACE_2), ex.getMessage());
        }

        createRequest.setNamespace(BDEF_NAMESPACE);
        createRequest.setDataProviderName(DATA_PROVIDER_NAME_2);
        try
        {
            relationalTableRegistrationService.createRelationalTableRegistration(createRequest);
            fail();
        }
        catch (ObjectNotFoundException ex)
        {
            Assert.assertEquals(String.format("Data provider with name \"%s\" doesn't exist.", DATA_PROVIDER_NAME_2), ex.getMessage());
        }

        createRequest.setDataProviderName(DATA_PROVIDER_NAME);
        createRequest.setStorageName(STORAGE_NAME_2);
        try
        {
            relationalTableRegistrationService.createRelationalTableRegistration(createRequest);
            fail();
        }
        catch (ObjectNotFoundException ex)
        {
            Assert.assertEquals(String.format("Storage with name \"%s\" doesn't exist.", STORAGE_NAME_2), ex.getMessage());
        }
    }

    @Test
    public void testCreateRelationalTableRegistrationBusinessObjectDefinitionExists()
    {
        RelationalTableRegistrationCreateRequest createRequest = new RelationalTableRegistrationCreateRequest();
        createRequest.setNamespace(BDEF_NAMESPACE);
        createRequest.setDataProviderName(DATA_PROVIDER_NAME);
        createRequest.setBusinessObjectDefinitionName(BDEF_NAME_2);
        createRequest.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        createRequest.setRelationalTableName(RELATIONAL_TABLE_NAME);
        createRequest.setStorageName(STORAGE_NAME);

        try
        {
            relationalTableRegistrationService.createRelationalTableRegistration(createRequest);
            fail();
        }
        catch (AlreadyExistsException ex)
        {
            Assert.assertEquals(String.format("Unable to create business object definition with name \"%s\" because it already exists for namespace \"%s\".", BDEF_NAME_2, BDEF_NAMESPACE), ex.getMessage());
        }
    }

    @Test
    public void testCreateRelationalTableRegistrationWrongStorageName()
    {
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_2, StoragePlatformEntity.S3);

        RelationalTableRegistrationCreateRequest createRequest = new RelationalTableRegistrationCreateRequest();
        createRequest.setNamespace(BDEF_NAMESPACE);
        createRequest.setDataProviderName(DATA_PROVIDER_NAME);
        createRequest.setBusinessObjectDefinitionName(BDEF_NAME);
        createRequest.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        createRequest.setRelationalTableName(RELATIONAL_TABLE_NAME);
        createRequest.setStorageName(STORAGE_NAME_2);

        try
        {
            relationalTableRegistrationService.createRelationalTableRegistration(createRequest);
            fail();
        }
        catch (IllegalArgumentException ex)
        {
            Assert.assertEquals(String.format("Only %s storage platform is supported.", StoragePlatformEntity.RELATIONAL), ex.getMessage());
        }
    }
}
