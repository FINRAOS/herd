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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageFileDaoHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;
import org.finra.herd.service.impl.BusinessObjectDataStorageFileServiceImpl;

/**
 * This class tests various functionality within the business object data storage file service.
 */
public class BusinessObjectDataStorageFileServiceMockTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Mock
    private S3Service s3Service;

    @Mock
    private StorageFileDaoHelper storageFileDaoHelper;

    @Mock
    private StorageFileHelper storageFileHelper;

    @Mock
    private StorageHelper storageHelper;

    @Mock
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @InjectMocks
    private BusinessObjectDataStorageFileServiceImpl businessObjectDataStorageFileService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    private static final BusinessObjectDataKey BUSINESS_OBJECT_DATA_KEY =
        new BusinessObjectDataKey("UT_BusinessObjectDataKeyNamespace_1_" + RANDOM_SUFFIX, null, null, null, 0, null, null, 0);

    private static final String FILE_PATH_1 = "file1";

    private static final String FILE_PATH_2 = "file2";

    private static final String PARTITION_VALUE_2 = "pv2_" + Math.random();

    private static final String PARTITION_VALUE_3 = "pv3_" + Math.random();

    private static final String PARTITION_VALUE_4 = "pv4_" + Math.random();

    private static final String PARTITION_VALUE_5 = "pv5_" + Math.random();

    private static final List<StorageFile> STORAGE_FILES = Lists.newArrayList(new StorageFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000));

    private static final List<StorageFile> STORAGE_FILES_WITH_NULL_ROW_COUNT = Lists.newArrayList(new StorageFile(FILE_PATH_2, FILE_SIZE_1_KB, null));

    private static final List<StorageFile> STORAGE_FILES_WITH_PATH =
        Lists.newArrayList(new StorageFile("some/path/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000));

    private static final List<String> SUB_PARTITION_VALUES = Arrays.asList(PARTITION_VALUE_2, PARTITION_VALUE_3, PARTITION_VALUE_4, PARTITION_VALUE_5);

    private static final String testS3KeyPrefix =
        getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
            PARTITION_VALUE, null, null, DATA_VERSION);

    private static final List<StorageFile> TEST_S3_STORAGE_FILES =
        Lists.newArrayList(new StorageFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000));

    @Test
    public void testCreateBusinessObjectDataStorageFiles()
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUES,
                DATA_VERSION);

        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);
        businessObjectDataStatusEntity.setPreRegistrationStatus(true);

        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(FORMAT_FILE_TYPE_CODE);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(BDEF_NAME);

        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setUsage(FORMAT_USAGE_CODE);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(FORMAT_VERSION);

        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);
        businessObjectDataEntity.setVersion(DATA_VERSION);

        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ENABLED);

        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        storageUnitEntity.setStorage(storageEntity);

        // Setup the mock calls
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(businessObjectDataHelper.getSubPartitionValues(businessObjectDataEntity)).thenReturn(SUB_PARTITION_VALUES);

        // Prepare the request object.
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, STORAGE_FILES, NO_DISCOVER_STORAGE_FILES);

        // Call the method under test.
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);

        // Verify the mock calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(businessObjectDataHelper).getSubPartitionValues(businessObjectDataEntity);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(BUSINESS_OBJECT_DATA_KEY);
        verify(storageFileHelper).getFilePathsFromStorageFiles(STORAGE_FILES);
        verify(storageFileDaoHelper).createStorageFileEntitiesFromStorageFiles(storageUnitEntity, STORAGE_FILES);
        verify(storageFileHelper).validateCreateRequestStorageFiles(STORAGE_FILES);
        verify(storageFileHelper).getStorageFileEntitiesMap(storageUnitEntity.getStorageFiles());
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), storageEntity,
                false, true);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesUpperCaseParameters()
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION);

        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);
        businessObjectDataStatusEntity.setPreRegistrationStatus(true);

        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(FORMAT_FILE_TYPE_CODE);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(BDEF_NAME);

        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setUsage(FORMAT_USAGE_CODE);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(FORMAT_VERSION);

        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);
        businessObjectDataEntity.setVersion(DATA_VERSION);

        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ENABLED);

        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        storageUnitEntity.setStorage(storageEntity);

        // Setup the mock calls
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME.toUpperCase(), businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(businessObjectDataHelper.getSubPartitionValues(businessObjectDataEntity)).thenReturn(SUB_PARTITION_VALUES);

        // Create business object data storage files using upper case input parameters (except for case-sensitive partition values and storage file paths).
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME.toUpperCase(),
                STORAGE_FILES, NO_DISCOVER_STORAGE_FILES);

        // Call method under test.
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);

        // Verify the mock calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME.toUpperCase(), businessObjectDataEntity);
        verify(businessObjectDataHelper).getSubPartitionValues(businessObjectDataEntity);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(BUSINESS_OBJECT_DATA_KEY);
        verify(storageFileHelper).getFilePathsFromStorageFiles(STORAGE_FILES);
        verify(storageFileDaoHelper).createStorageFileEntitiesFromStorageFiles(storageUnitEntity, STORAGE_FILES);
        verify(storageFileHelper).validateCreateRequestStorageFiles(STORAGE_FILES);
        verify(storageFileHelper).getStorageFileEntitiesMap(storageUnitEntity.getStorageFiles());
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), storageEntity,
                false, true);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesLowerCaseParameters()
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION);

        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);
        businessObjectDataStatusEntity.setPreRegistrationStatus(true);

        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(FORMAT_FILE_TYPE_CODE);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(BDEF_NAME);

        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setUsage(FORMAT_USAGE_CODE);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(FORMAT_VERSION);

        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);
        businessObjectDataEntity.setVersion(DATA_VERSION);

        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ENABLED);

        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        storageUnitEntity.setStorage(storageEntity);

        // Setup the mock calls
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME.toLowerCase(), businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(businessObjectDataHelper.getSubPartitionValues(businessObjectDataEntity)).thenReturn(SUB_PARTITION_VALUES);

        // Create business object data storage files using lower case input parameters (except for case-sensitive partition values and storage file paths).
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME.toLowerCase(),
                STORAGE_FILES, NO_DISCOVER_STORAGE_FILES);

        // Call method under test.
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);

        // Verify the mock calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME.toLowerCase(), businessObjectDataEntity);
        verify(businessObjectDataHelper).getSubPartitionValues(businessObjectDataEntity);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(BUSINESS_OBJECT_DATA_KEY);
        verify(storageFileHelper).getFilePathsFromStorageFiles(STORAGE_FILES);
        verify(storageFileDaoHelper).createStorageFileEntitiesFromStorageFiles(storageUnitEntity, STORAGE_FILES);
        verify(storageFileHelper).validateCreateRequestStorageFiles(STORAGE_FILES);
        verify(storageFileHelper).getStorageFileEntitiesMap(storageUnitEntity.getStorageFiles());
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), storageEntity,
                false, true);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesTrimParameters()
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUES,
                DATA_VERSION);

        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);
        businessObjectDataStatusEntity.setPreRegistrationStatus(true);

        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(FORMAT_FILE_TYPE_CODE);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(BDEF_NAME);

        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setUsage(FORMAT_USAGE_CODE);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(FORMAT_VERSION);

        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);
        businessObjectDataEntity.setVersion(DATA_VERSION);

        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ENABLED);

        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        storageUnitEntity.setStorage(storageEntity);

        // Setup the mock calls
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(businessObjectDataHelper.getSubPartitionValues(businessObjectDataEntity)).thenReturn(SUB_PARTITION_VALUES);

        // Create business object data storage files by passing all input parameters with leading and trailing empty spaces.
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUB_PARTITION_VALUES), DATA_VERSION,
                addWhitespace(STORAGE_NAME), STORAGE_FILES, NO_DISCOVER_STORAGE_FILES);

        // Call method under test.
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, STORAGE_FILES, response);

        // Verify the mock calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(businessObjectDataHelper).getSubPartitionValues(businessObjectDataEntity);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(BUSINESS_OBJECT_DATA_KEY);
        verify(storageFileHelper).getFilePathsFromStorageFiles(STORAGE_FILES);
        verify(storageFileDaoHelper).createStorageFileEntitiesFromStorageFiles(storageUnitEntity, STORAGE_FILES);
        verify(storageFileHelper).validateCreateRequestStorageFiles(STORAGE_FILES);
        verify(storageFileHelper).getStorageFileEntitiesMap(storageUnitEntity.getStorageFiles());
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), storageEntity,
                false, true);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesMissingOptionalParameters()
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION);

        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);
        businessObjectDataStatusEntity.setPreRegistrationStatus(true);

        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(FORMAT_FILE_TYPE_CODE);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(BDEF_NAME);

        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setUsage(FORMAT_USAGE_CODE);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(FORMAT_VERSION);

        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);
        businessObjectDataEntity.setVersion(DATA_VERSION);

        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ENABLED);

        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        storageUnitEntity.setStorage(storageEntity);

        // Setup the mock calls
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(businessObjectDataHelper.getSubPartitionValues(businessObjectDataEntity)).thenReturn(NO_SUBPARTITION_VALUES);

        // Create business object data storage files without passing any of the optional parameters.
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, STORAGE_NAME, STORAGE_FILES_WITH_NULL_ROW_COUNT, NO_DISCOVER_STORAGE_FILES);

        // Call method under test.
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME, STORAGE_FILES_WITH_NULL_ROW_COUNT, response);

        // Verify the mock calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(businessObjectDataHelper).getSubPartitionValues(businessObjectDataEntity);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(BUSINESS_OBJECT_DATA_KEY);
        verify(storageFileHelper).getFilePathsFromStorageFiles(STORAGE_FILES_WITH_NULL_ROW_COUNT);
        verify(storageFileDaoHelper).createStorageFileEntitiesFromStorageFiles(storageUnitEntity, STORAGE_FILES_WITH_NULL_ROW_COUNT);
        verify(storageFileHelper).validateCreateRequestStorageFiles(STORAGE_FILES_WITH_NULL_ROW_COUNT);
        verify(storageFileHelper).getStorageFileEntitiesMap(storageUnitEntity.getStorageFiles());
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), storageEntity,
                false, true);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesWithStorageDirectory()
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION);

        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);
        businessObjectDataStatusEntity.setPreRegistrationStatus(true);

        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(FORMAT_FILE_TYPE_CODE);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(BDEF_NAME);

        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setUsage(FORMAT_USAGE_CODE);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(FORMAT_VERSION);

        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);
        businessObjectDataEntity.setVersion(DATA_VERSION);

        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ENABLED);

        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        storageUnitEntity.setStorage(storageEntity);

        // Setup the mock calls
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(businessObjectDataHelper.getSubPartitionValues(businessObjectDataEntity)).thenReturn(NO_SUBPARTITION_VALUES);

        // Add a storage file to a storage unit with a storage directory path.
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, STORAGE_NAME, STORAGE_FILES_WITH_PATH, NO_DISCOVER_STORAGE_FILES);

        // Call method under test.
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME, STORAGE_FILES_WITH_PATH, response);

        // Verify the mock calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(businessObjectDataHelper).getSubPartitionValues(businessObjectDataEntity);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(BUSINESS_OBJECT_DATA_KEY);
        verify(storageFileHelper).getFilePathsFromStorageFiles(STORAGE_FILES_WITH_PATH);
        verify(storageFileDaoHelper).createStorageFileEntitiesFromStorageFiles(storageUnitEntity, STORAGE_FILES_WITH_PATH);
        verify(storageFileHelper).validateCreateRequestStorageFiles(STORAGE_FILES_WITH_PATH);
        verify(storageFileHelper).getStorageFileEntitiesMap(storageUnitEntity.getStorageFiles());
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), storageEntity,
                false, true);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesPreviouslyRegisteredS3FileSizeMismatchIgnoredDueToDisabledFileSizeValidation()
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION);

        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);
        businessObjectDataStatusEntity.setPreRegistrationStatus(true);

        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(FORMAT_FILE_TYPE_CODE);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(BDEF_NAME);

        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setUsage(FORMAT_USAGE_CODE);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(FORMAT_VERSION);

        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);
        businessObjectDataEntity.setVersion(DATA_VERSION);

        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ENABLED);

        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        storageUnitEntity.setStorage(storageEntity);

        // Setup the mock calls
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(businessObjectDataHelper.getSubPartitionValues(businessObjectDataEntity)).thenReturn(NO_SUBPARTITION_VALUES);

        // Add a second storage file to this business object data.
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, STORAGE_NAME, TEST_S3_STORAGE_FILES, NO_DISCOVER_STORAGE_FILES);

        // Call method under test.
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME, TEST_S3_STORAGE_FILES, response);

        // Verify the mock calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(businessObjectDataHelper).getSubPartitionValues(businessObjectDataEntity);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(BUSINESS_OBJECT_DATA_KEY);
        verify(storageFileHelper).getFilePathsFromStorageFiles(TEST_S3_STORAGE_FILES);
        verify(storageFileDaoHelper).createStorageFileEntitiesFromStorageFiles(storageUnitEntity, TEST_S3_STORAGE_FILES);
        verify(storageFileHelper).validateCreateRequestStorageFiles(TEST_S3_STORAGE_FILES);
        verify(storageFileHelper).getStorageFileEntitiesMap(storageUnitEntity.getStorageFiles());
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true);
        verify(storageHelper, times(3))
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), storageEntity,
                false, true);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesS3Managed()
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUES,
                DATA_VERSION);

        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);
        businessObjectDataStatusEntity.setPreRegistrationStatus(true);

        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(FORMAT_FILE_TYPE_CODE);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(BDEF_NAME);

        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setUsage(FORMAT_USAGE_CODE);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(FORMAT_VERSION);

        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);
        businessObjectDataEntity.setVersion(DATA_VERSION);

        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ENABLED);

        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(StorageEntity.MANAGED_STORAGE);

        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);

        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);

        List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<>();
        s3ObjectSummaries.add(new S3ObjectSummary());

        Map<String, StorageFile> actualS3Keys = new HashMap<>();
        actualS3Keys.put(testS3KeyPrefix + "/" + FILE_PATH_2, new StorageFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000));

        // Setup the mock calls
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY);
        when(storageUnitDaoHelper.getStorageUnitEntity(StorageEntity.MANAGED_STORAGE, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX)).thenReturn(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        when(storageHelper.getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX, storageEntity, false, true)).thenReturn(true);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE))
            .thenReturn(S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        when(storageHelper.getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE, storageEntity, false, true)).thenReturn(true);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE)).thenReturn(S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE);
        when(storageHelper.getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE, storageEntity, false, true)).thenReturn(true);
        when(storageHelper.getS3BucketAccessParams(storageUnitEntity.getStorage())).thenReturn(s3FileTransferRequestParamsDto);
        when(s3Service.listDirectory(s3FileTransferRequestParamsDto, true)).thenReturn(s3ObjectSummaries);
        when(storageFileHelper.getStorageFilesMapFromS3ObjectSummaries(s3ObjectSummaries)).thenReturn(actualS3Keys);
        when(businessObjectDataHelper.getSubPartitionValues(businessObjectDataEntity)).thenReturn(SUB_PARTITION_VALUES);

        // Create the request object
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUB_PARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, TEST_S3_STORAGE_FILES, NO_DISCOVER_STORAGE_FILES);

        // Call method under test.
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, TEST_S3_STORAGE_FILES, response);

        // Verify the mock calls.
        verify(storageFileHelper).validateCreateRequestStorageFiles(TEST_S3_STORAGE_FILES);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(BUSINESS_OBJECT_DATA_KEY);
        verify(storageUnitDaoHelper).getStorageUnitEntity(StorageEntity.MANAGED_STORAGE, businessObjectDataEntity);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        verify(storageHelper).getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX, storageEntity, false, true);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        verify(storageHelper).getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE, storageEntity, false, true);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE);
        verify(storageHelper).getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE, storageEntity, false, true);
        verify(s3KeyPrefixHelper).buildS3KeyPrefix(storageEntity, businessObjectFormatEntity, BUSINESS_OBJECT_DATA_KEY);
        verify(storageFileHelper).getStorageFileEntitiesMap(storageUnitEntity.getStorageFiles());
        verify(storageFileHelper).getFilePathsFromStorageFiles(TEST_S3_STORAGE_FILES);
        verify(storageHelper).getS3BucketAccessParams(storageEntity);
        verify(s3Service).listDirectory(s3FileTransferRequestParamsDto, true);
        verify(storageFileHelper).getStorageFilesMapFromS3ObjectSummaries(s3ObjectSummaries);
        verify(storageFileHelper).validateStorageFile(TEST_S3_STORAGE_FILES.get(0), S3_BUCKET_NAME, actualS3Keys, true);
        verify(storageFileDaoHelper).createStorageFileEntitiesFromStorageFiles(storageUnitEntity, TEST_S3_STORAGE_FILES);
        verify(businessObjectDataHelper).getSubPartitionValues(businessObjectDataEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesAutoDiscovery()
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES,
                DATA_VERSION);

        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);
        businessObjectDataStatusEntity.setPreRegistrationStatus(true);

        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(FORMAT_FILE_TYPE_CODE);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(BDEF_NAME);

        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setUsage(FORMAT_USAGE_CODE);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(FORMAT_VERSION);

        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);
        businessObjectDataEntity.setVersion(DATA_VERSION);

        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ENABLED);

        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(StorageEntity.MANAGED_STORAGE);

        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setDirectoryPath(testS3KeyPrefix);

        BusinessObjectFormat businessObjectFormat = new BusinessObjectFormat();
        businessObjectFormat.setId(getRandomLong());

        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);

        List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<>();
        s3ObjectSummaries.add(new S3ObjectSummary());

        Map<String, StorageFile> actualS3Keys = new HashMap<>();
        actualS3Keys.put(testS3KeyPrefix + "/" + FILE_PATH_2, new StorageFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000));

        // Setup the mock calls
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY);
        when(storageUnitDaoHelper.getStorageUnitEntity(StorageEntity.MANAGED_STORAGE, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX)).thenReturn(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        when(storageHelper.getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX, storageEntity, false, true)).thenReturn(true);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE))
            .thenReturn(S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        when(storageHelper.getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE, storageEntity, false, true)).thenReturn(true);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE)).thenReturn(S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE);
        when(storageHelper.getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE, storageEntity, false, true)).thenReturn(true);
        when(businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity)).thenReturn(businessObjectFormat);
        when(storageHelper.getS3BucketAccessParams(storageEntity)).thenReturn(s3FileTransferRequestParamsDto);
        when(s3Service.listDirectory(s3FileTransferRequestParamsDto, true)).thenReturn(s3ObjectSummaries);
        when(storageFileHelper.getStorageFilesMapFromS3ObjectSummaries(s3ObjectSummaries)).thenReturn(actualS3Keys);
        when(businessObjectDataHelper.getSubPartitionValues(businessObjectDataEntity)).thenReturn(NO_SUBPARTITION_VALUES);

        // Discover storage files in S3 managed storage.
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, NO_STORAGE_FILES, DISCOVER_STORAGE_FILES));

        // Validate the returned object.
        assertEquals(
            new BusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                Lists.newArrayList(new StorageFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))), response);

        // Verify the mock calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(BUSINESS_OBJECT_DATA_KEY);
        verify(storageUnitDaoHelper).getStorageUnitEntity(StorageEntity.MANAGED_STORAGE, businessObjectDataEntity);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        verify(storageHelper).getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX, storageEntity, false, true);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        verify(storageHelper).getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE, storageEntity, false, true);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE);
        verify(storageHelper).getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE, storageEntity, false, true);
        verify(businessObjectFormatHelper).createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
        verify(storageUnitDaoHelper)
            .checkBusinessObjectDataForExplicitlyRegisteredSubPartitionsInStorage(storageEntity, businessObjectFormatEntity, businessObjectFormat,
                BUSINESS_OBJECT_DATA_KEY, testS3KeyPrefix);
        verify(storageFileHelper).getStorageFileEntitiesMap(storageUnitEntity.getStorageFiles());
        verify(storageHelper).getS3BucketAccessParams(storageEntity);
        verify(s3Service).listDirectory(s3FileTransferRequestParamsDto, true);
        verify(storageFileHelper).getStorageFilesMapFromS3ObjectSummaries(s3ObjectSummaries);
        verify(storageFileDaoHelper).createStorageFileEntitiesFromStorageFiles(storageUnitEntity, TEST_S3_STORAGE_FILES);
        verify(businessObjectDataHelper).getSubPartitionValues(businessObjectDataEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesAutoDiscoveryNoStorageUnitDirectoryPath()
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);
        businessObjectDataStatusEntity.setPreRegistrationStatus(true);

        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(FORMAT_FILE_TYPE_CODE);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(BDEF_NAME);

        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setUsage(FORMAT_USAGE_CODE);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(FORMAT_VERSION);

        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);
        businessObjectDataEntity.setVersion(DATA_VERSION);

        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ENABLED);

        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(StorageEntity.MANAGED_STORAGE);

        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setDirectoryPath(BLANK_TEXT);

        // Setup the mock calls
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY);
        when(storageUnitDaoHelper.getStorageUnitEntity(StorageEntity.MANAGED_STORAGE, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX)).thenReturn(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        when(storageHelper.getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX, storageEntity, false, true)).thenReturn(true);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE))
            .thenReturn(S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        when(storageHelper.getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE, storageEntity, false, true)).thenReturn(true);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE)).thenReturn(S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE);
        when(storageHelper.getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE, storageEntity, false, true)).thenReturn(true);

        // Try to discover storage files in S3 storage when storage unit has a blank directory path.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, NO_STORAGE_FILES, DISCOVER_STORAGE_FILES));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object data has no storage directory path which is required for auto-discovery of storage files.", e.getMessage());
        }

        // Verify the mock calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(BUSINESS_OBJECT_DATA_KEY);
        verify(storageUnitDaoHelper).getStorageUnitEntity(StorageEntity.MANAGED_STORAGE, businessObjectDataEntity);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        verify(storageHelper).getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX, storageEntity, false, true);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        verify(storageHelper).getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE, storageEntity, false, true);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE);
        verify(storageHelper).getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE, storageEntity, false, true);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataDaoHelper, businessObjectDataHelper, businessObjectFormatHelper, configurationHelper, s3KeyPrefixHelper,
            s3Service, storageFileDaoHelper, storageFileHelper, storageHelper, storageUnitDaoHelper);
    }
}
