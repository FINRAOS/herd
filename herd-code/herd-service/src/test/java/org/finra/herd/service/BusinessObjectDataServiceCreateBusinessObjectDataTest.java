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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.persistence.PersistenceException;

import org.apache.commons.io.FileUtils;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.Command;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitCreateRequest;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * This class tests the createBusinessObjectData functionality within the business object data REST controller.
 */
public class BusinessObjectDataServiceCreateBusinessObjectDataTest extends AbstractServiceTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessObjectDataServiceCreateBusinessObjectDataTest.class);

    private static final String testS3KeyPrefix =
        getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
            PARTITION_VALUE, null, null, INITIAL_DATA_VERSION);

    private Path localTempPath;

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        // Create a local temp directory.
        localTempPath = Files.createTempDirectory(null);
    }

    /**
     * Cleans up the local temp directory and S3 test path that we are using.
     */
    @After
    public void cleanEnv() throws IOException
    {
        try
        {
            // Clean up the local directory.
            FileUtils.deleteDirectory(localTempPath.toFile());

            // Clean up the destination S3 folder.
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3KeyPrefix(testS3KeyPrefix);
            s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
        }
        catch (Exception ex)
        {
            // If an exception is thrown by one of the @Test methods, some cleanup operations could also fail. This is why we are just logging a warning here.
            LOGGER.warn("Unable to cleanup environment.", ex);
        }
    }

    @Test
    public void testCreateBusinessObjectData()
    {
        // Create an initial version of the business object data.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);

        // Verify the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectData(businessObjectDataCreateRequest, INITIAL_DATA_VERSION, true, resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataMissingRequiredParameters()
    {
        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME);

        BusinessObjectDataCreateRequest request;
        List<StorageFile> storageFiles;

        // Try to create a business object data instance when business object definition name is not specified.
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to create a business object data instance when business object format usage is not specified.
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to create a business object data instance when business object format file type is not specified.
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, INITIAL_FORMAT_VERSION, PARTITION_KEY, PARTITION_VALUE,
                BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to create a business object data instance when business object format version is not specified.
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_KEY, PARTITION_VALUE,
                BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to create a business object data instance when business object format partition key is not specified.
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, BLANK_TEXT,
                PARTITION_VALUE, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when business object format partition key is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition key must be specified.", e.getMessage());
        }

        // Try to create a business object data instance when partition value is not specified.
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                BLANK_TEXT, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to create a business object data instance when request contains no storage units element.
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));
        request.setStorageUnits(null);
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when when request contains no storage units element.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one storage unit must be specified.", e.getMessage());
        }

        // Try to create a business object data instance when no storage units are specified.
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));
        request.setStorageUnits(new ArrayList<StorageUnitCreateRequest>());
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when when no storage units are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one storage unit must be specified.", e.getMessage());
        }

        // Try to create a business object data instance when request contains an empty storage unit element.
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));
        List<StorageUnitCreateRequest> storageUnits = new ArrayList<>();
        storageUnits.add(null);
        request.setStorageUnits(storageUnits);
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when when no storage units are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage unit can't be null.", e.getMessage());
        }

        // Try to create a business object data instance when storage name is not specified for a storage unit.
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));
        request.getStorageUnits().get(0).setStorageName(BLANK_TEXT);
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when storage name is not specified for a storage unit.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage name is required for each storage unit.", e.getMessage());
        }

        // Try to create a business object data instance when both storage directory and storage files are not specified.
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, null, null);
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when both storage directory and storage files are not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage directory or at least one storage file must be specified for each storage unit.", e.getMessage());
        }

        // Try to create a business object data instance when storage directory element is present, but the actual directory path value is not specified.
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, BLANK_TEXT,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when storage directory element is present, but the actual directory path value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage directory path must be specified.", e.getMessage());
        }

        // Try to create a business object data instance when storage file element is present, but the actual file path value is not specified.
        storageFiles = businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, Arrays.asList(LOCAL_FILE));
        storageFiles.get(0).setFilePath(BLANK_TEXT);
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix, storageFiles);
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when storage file element is present, but the actual file path value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A file path must be specified.", e.getMessage());
        }

        // Try to create a business object data instance when storage file size is not specified.
        storageFiles = businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, Arrays.asList(LOCAL_FILE));
        storageFiles.get(0).setFileSizeBytes(null);
        request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix, storageFiles);
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when storage file size is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A file size must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataMissingOptionalParameters()
    {
        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME);

        // Create an initial version of business object data without specifying any of the optional parameters except for namespace.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, STORAGE_NAME, testS3KeyPrefix, null);
        assertNull(request.getSubPartitionValues());
        request.setStatus(BLANK_TEXT);
        assertNull(request.getAttributes());
        assertNull(request.getStorageUnits().get(0).getStorageFiles());
        request.setCreateNewVersion(null);
        assertNull(request.getBusinessObjectDataParents());
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.createBusinessObjectData(request);

        // Verify the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectData(request, INITIAL_DATA_VERSION, true, resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataNoStorageDirectory()
    {
        // Create an initial version of the business object data without specifying a storage directory.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();
        request.getStorageUnits().get(0).setStorageDirectory(null);
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.createBusinessObjectData(request);

        // Verify the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectData(request, INITIAL_DATA_VERSION, true, resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataNoStorageFiles()
    {
        // Create the initial version of the business object data without specifying storage files.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();
        request.getStorageUnits().get(0).setStorageFiles(null);
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.createBusinessObjectData(request);

        // Verify the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectData(request, INITIAL_DATA_VERSION, true, resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataNoAttributes()
    {
        // Create business object data with no attribute definitions and no attributes which is valid.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest(false);
        businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
    }

    @Test
    public void testCreateBusinessObjectDataTrimParameters()
    {
        // Create relative database entities.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME);

        // Build a business object data create request with some of the request parameters having leading and trailing whitespace characters.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE), addWhitespace(FORMAT_FILE_TYPE_CODE),
                INITIAL_FORMAT_VERSION, addWhitespace(PARTITION_KEY), addWhitespace(PARTITION_VALUE), addWhitespace(BDATA_STATUS), addWhitespace(STORAGE_NAME),
                addWhitespace(testS3KeyPrefix), businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));
        for (StorageFile storageFile : request.getStorageUnits().get(0).getStorageFiles())
        {
            storageFile.setFilePath(addWhitespace(storageFile.getFilePath()));
        }
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.createBusinessObjectData(request);

        // Verify the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectFormatEntity, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS,
                STORAGE_NAME, testS3KeyPrefix, businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES),
                CollectionUtils.isEmpty(request.getAttributes()) ? new ArrayList<Attribute>() : request.getAttributes(), resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataInvalidParameters()
    {
        // Try to create a business object data when namespace contains a forward slash character.
        try
        {
            businessObjectDataService.createBusinessObjectData(
                new BusinessObjectDataCreateRequest(addSlash(BDEF_NAMESPACE), BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_KEY, PARTITION_VALUE, SUBPARTITION_VALUES, BusinessObjectDataStatusEntity.VALID, Arrays.asList(
                    new StorageUnitCreateRequest(STORAGE_NAME, new StorageDirectory(STORAGE_DIRECTORY_PATH), NO_STORAGE_FILES, NO_DISCOVER_STORAGE_FILES)),
                    NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION));
            fail("Should throw an IllegalArgumentException when namespace contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object data when business object definition name contains a forward slash character.
        try
        {
            businessObjectDataService.createBusinessObjectData(
                new BusinessObjectDataCreateRequest(BDEF_NAMESPACE, addSlash(BDEF_NAME), FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_KEY, PARTITION_VALUE, SUBPARTITION_VALUES, BusinessObjectDataStatusEntity.VALID, Arrays.asList(
                    new StorageUnitCreateRequest(STORAGE_NAME, new StorageDirectory(STORAGE_DIRECTORY_PATH), NO_STORAGE_FILES, NO_DISCOVER_STORAGE_FILES)),
                    NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION));
            fail("Should throw an IllegalArgumentException when business object definition name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition name can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object data when business object format usage contains a forward slash character.
        try
        {
            businessObjectDataService.createBusinessObjectData(
                new BusinessObjectDataCreateRequest(BDEF_NAMESPACE, BDEF_NAME, addSlash(FORMAT_USAGE_CODE), FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_KEY, PARTITION_VALUE, SUBPARTITION_VALUES, BusinessObjectDataStatusEntity.VALID, Arrays.asList(
                    new StorageUnitCreateRequest(STORAGE_NAME, new StorageDirectory(STORAGE_DIRECTORY_PATH), NO_STORAGE_FILES, NO_DISCOVER_STORAGE_FILES)),
                    NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION));
            fail("Should throw an IllegalArgumentException when business object format usage contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object format usage can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object data when business object format file type contains a forward slash character.
        try
        {
            businessObjectDataService.createBusinessObjectData(
                new BusinessObjectDataCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, addSlash(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION,
                    PARTITION_KEY, PARTITION_VALUE, SUBPARTITION_VALUES, BusinessObjectDataStatusEntity.VALID, Arrays.asList(
                    new StorageUnitCreateRequest(STORAGE_NAME, new StorageDirectory(STORAGE_DIRECTORY_PATH), NO_STORAGE_FILES, NO_DISCOVER_STORAGE_FILES)),
                    NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION));
            fail("Should throw an IllegalArgumentException when business object format file type contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object format file type can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object data when partition key contains a forward slash character.
        try
        {
            businessObjectDataService.createBusinessObjectData(
                new BusinessObjectDataCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    addSlash(PARTITION_KEY), PARTITION_VALUE, SUBPARTITION_VALUES, BusinessObjectDataStatusEntity.VALID, Arrays.asList(
                    new StorageUnitCreateRequest(STORAGE_NAME, new StorageDirectory(STORAGE_DIRECTORY_PATH), NO_STORAGE_FILES, NO_DISCOVER_STORAGE_FILES)),
                    NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION));
            fail("Should throw an IllegalArgumentException when partition key contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Partition key can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object data when partition value contains a forward slash character.
        try
        {
            businessObjectDataService.createBusinessObjectData(
                new BusinessObjectDataCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                    addSlash(PARTITION_VALUE), SUBPARTITION_VALUES, BusinessObjectDataStatusEntity.VALID, Arrays.asList(
                    new StorageUnitCreateRequest(STORAGE_NAME, new StorageDirectory(STORAGE_DIRECTORY_PATH), NO_STORAGE_FILES, NO_DISCOVER_STORAGE_FILES)),
                    NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION));
            fail("Should throw an IllegalArgumentException when partition value contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Partition value can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object data when a sub-partition value contains a forward slash character.
        try
        {
            businessObjectDataService.createBusinessObjectData(
                new BusinessObjectDataCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                    PARTITION_VALUE, Arrays.asList(addSlash(PARTITION_VALUE_2)), BusinessObjectDataStatusEntity.VALID, Arrays.asList(
                    new StorageUnitCreateRequest(STORAGE_NAME, new StorageDirectory(STORAGE_DIRECTORY_PATH), NO_STORAGE_FILES, NO_DISCOVER_STORAGE_FILES)),
                    NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION));
            fail("Should throw an IllegalArgumentException when sub-partition value contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Subpartition value can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object data when an attribute name contains a forward slash character.
        try
        {
            businessObjectDataService.createBusinessObjectData(
                new BusinessObjectDataCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                    PARTITION_VALUE, SUBPARTITION_VALUES, BusinessObjectDataStatusEntity.VALID, Arrays.asList(
                    new StorageUnitCreateRequest(STORAGE_NAME, new StorageDirectory(STORAGE_DIRECTORY_PATH), NO_STORAGE_FILES, NO_DISCOVER_STORAGE_FILES)),
                    Arrays.asList(new Attribute(addSlash(ATTRIBUTE_NAME_1_MIXED_CASE), ATTRIBUTE_VALUE_1)), NO_BUSINESS_OBJECT_DATA_PARENTS,
                    NO_CREATE_NEW_VERSION));
            fail("Should throw an IllegalArgumentException when attribute name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Attribute name can not contain a forward slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectBusinessObjectStatusCodeNoExists()
    {
        // Create database entities required for testing.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME);

        // Build a business object data create request with a non-existing business object data status code.
        String invalidStatusCode = "I_DO_NOT_EXIST";
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, invalidStatusCode, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, Arrays.asList(LOCAL_FILE)));

        try
        {
            // Try to create a business object data instance using non-existing business object data status code.
            businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
            fail("Should throw an ObjectNotFoundException when business object data status code does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data status \"%s\" doesn't exist.", invalidStatusCode), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataBusinessObjectFormatNoExists()
    {
        // Create database entities required for testing.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME);

        // Build a business object data create request with a non-existing business object format.
        String invalidBusinessObjectFormatUsage = "I_DO_NOT_EXIST";
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, invalidBusinessObjectFormatUsage, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, Arrays.asList(LOCAL_FILE)));

        try
        {
            // Try to create a business object data instance using non-existing business object format.
            businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
            fail("Should throw an ObjectNotFoundException when a non-existing business object format is specified.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, invalidBusinessObjectFormatUsage, FORMAT_FILE_TYPE_CODE,
                    FORMAT_VERSION), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataInvalidPartitionKey()
    {
        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Try to create a business object data instance when an invalid partition key value is specified.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                "INVALID_PARTITION_KEY", PARTITION_VALUE, BDATA_STATUS, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, Arrays.asList(LOCAL_FILE)));
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when an invalid partition key value is specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                    .format("Partition key \"%s\" doesn't match configured business object format partition key \"%s\".", request.getPartitionKey(), PARTITION_KEY),
                e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageNotFound()
    {
        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Build a business object data create request with a non-existing storage name.
        String invalidStorageName = "I_DO_NOT_EXIST";
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, invalidStorageName, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, Arrays.asList(LOCAL_FILE)));

        try
        {
            // Try to create a business object data instance using a non-existing storage.
            businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
            fail("Should throw an ObjectNotFoundException when specified storage name does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", invalidStorageName), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageHasValidateFileSizeEnabledWithoutValidateFileExistence()
    {
        // Create an S3 storage with file existence validation enabled without file size validation.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME_2),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), Boolean.toString(true))));

        // Create a business object format.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

        // Create a business object data status.
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Try to create a business object data instance using storage with file existence validation enabled without file size validation.
        try
        {
            businessObjectDataService.createBusinessObjectData(businessObjectDataServiceTestHelper
                .createBusinessObjectDataCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                    PARTITION_KEY, PARTITION_VALUE, BDATA_STATUS, STORAGE_NAME, testS3KeyPrefix,
                    businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, Arrays.asList(LOCAL_FILE))));
            fail("Should throw an IllegalArgumentException when using storage with file existence validation enabled without file size validation.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage \"%s\" has file size validation enabled without file existence validation.", STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataInvalidStorageFile()
    {
        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME);

        // Build a business object data create request with a storage file path not matching the storage directory path.
        String wrongS3KeyPrefix = "WRONG_S3_KEY_PREFIX";
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(wrongS3KeyPrefix, Arrays.asList(LOCAL_FILE)));

        try
        {
            // Try to create a business object data instance.
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when a storage storage file path does not match the storage directory path.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Storage file path \"%s/%s\" does not match the storage directory path \"%s\".", wrongS3KeyPrefix, LOCAL_FILE, testS3KeyPrefix),
                e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataValidateFileExistenceNoValidatePrefix() throws Exception
    {
        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.TRUE.toString()));
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, attributes);

        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Build a list of storage files.
        List<StorageFile> storageFiles = businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES);

        // Create and upload to S3 managed storage a set of test files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(S3_BUCKET_NAME, testS3KeyPrefix, localTempPath, LOCAL_FILES, new ArrayList<String>());

        // Build a new business object data create request.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, STORAGE_NAME, testS3KeyPrefix, storageFiles);

        // Try to create a business object data instance. It should go through without any errors.
        businessObjectDataService.createBusinessObjectData(request);
    }

    @Test
    public void testCreateBusinessObjectDataValidateFileExistenceNoValidatePrefixDirectoryInvalid() throws Exception
    {
        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.TRUE.toString()));
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, attributes);

        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Build a list of storage files.
        List<StorageFile> storageFiles = businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES);

        // Create and upload to S3 managed storage a set of test files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(S3_BUCKET_NAME, testS3KeyPrefix, localTempPath, LOCAL_FILES, new ArrayList<String>());

        // Build a new business object data create request.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, STORAGE_NAME, "INVALID_DIRECTORY_PATH", storageFiles);

        try
        {
            // Try to create a business object data instance.
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when a directory path doesn't match the S3 uploaded files.");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage().contains(String.format("does not match the storage directory path \"%s\"", "INVALID_DIRECTORY_PATH")));
        }
    }

    @Test
    public void testCreateBusinessObjectDataInvalidValidationBooleanValue()
    {
        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), INVALID_BOOLEAN_VALUE));
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, attributes);

        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Build a list of storage files.
        List<StorageFile> storageFiles = businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES);

        // Build a new business object data create request.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, STORAGE_NAME, testS3KeyPrefix, storageFiles);

        try
        {
            // Try to create a business object data instance.
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalStateException when a validation boolean value is invalid.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage has an invalid boolean value: " + "\"%s\".",
                ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE.getDefaultValue(), STORAGE_NAME, INVALID_BOOLEAN_VALUE), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataValidateFlagIgnoredForNonS3Storage() throws Exception
    {
        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.TRUE.toString()));
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, "NON_S3_STORAGE", attributes);

        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Build a list of storage files.
        List<StorageFile> storageFiles = businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES);

        // Build a new business object data create request.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, STORAGE_NAME, testS3KeyPrefix, storageFiles);

        // Try to create a business object data instance. This should succeed since the bucket is not of type "S3", even though there are no files in S3.
        businessObjectDataService.createBusinessObjectData(request);
    }

    @Test
    public void testCreateBusinessObjectDataInvalidStorageFileRowCount()
    {
        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME);

        // Try to create a business object data instance when storage file row count has a negative value.
        List<StorageFile> storageFiles = businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, Arrays.asList(LOCAL_FILE));
        storageFiles.get(0).setRowCount(-1L);
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix, storageFiles);
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when storage file size is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("File \"%s/%s\" has a row count which is < 0.", testS3KeyPrefix, LOCAL_FILE), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataInitialDataVersionExists()
    {
        // Create relative database entities including the initial version of the business object data.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, true, BDATA_STATUS);
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME);

        // Build a list of storage files.
        List<StorageFile> storageFiles = businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES);

        // Build a new business object data create request.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, STORAGE_NAME, testS3KeyPrefix, storageFiles);

        // Try to create the second version of the business object data (version 1) with createNewVersion flag set to null and to false.
        for (Boolean createNewVersionFlag : new Boolean[] {null, Boolean.FALSE})
        {
            request.setCreateNewVersion(createNewVersionFlag);
            try
            {
                businessObjectDataService.createBusinessObjectData(request);
                fail(String.format("Should throw an IllegalArgumentException when the initial data version exists and createNewVersion flag is set to \"%s\".",
                    createNewVersionFlag));
            }
            catch (AlreadyExistsException e)
            {
                assertEquals("Unable to create business object data because it already exists.", e.getMessage());
            }
        }

        // Try to create the second version of the business object data with createNewVersion flag set to true.
        request.setCreateNewVersion(true);
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.createBusinessObjectData(request);

        // Validate the results for the second version if the business object data.
        businessObjectDataServiceTestHelper.validateBusinessObjectData(request, SECOND_DATA_VERSION, true, resultBusinessObjectData);

        // Confirm that the initial version of the business object data now does not have the latestFlag set.
        BusinessObjectDataEntity initialVersionBusinessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION));
        assertEquals(false, initialVersionBusinessObjectDataEntity.getLatestVersion());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateBusinessObjectDataMissingRequiredAttribute()
    {
        // This will create a business object data create request with attributes. It will be associated with a format that has attribute definitions
        // that require the attribute to be specified.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest(true);

        // Null out the attributes in the create request, even though the format requires them.
        businessObjectDataCreateRequest.setAttributes(null);

        // Create the business object data which should fail since required attributes are missing.
        businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
    }

    @Test
    public void testCreateBusinessObjectDataDuplicateAttributeNames() throws Exception
    {
        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME);

        // Try to create a business object data instance when duplicate attribute names are specified.
        // Ensure different cases are still considered a duplicate.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));
        List<Attribute> attributes = new ArrayList<>();
        request.setAttributes(attributes);
        attributes.add(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1));
        try
        {
            // Try to create a business object data instance.
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when duplicate attribute names are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate attribute name found: %s", ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase()), e.getMessage());
        }
    }

    @Test(expected = PersistenceException.class)
    public void testCreateBusinessObjectDataAttributeValueTooLarge() throws Exception
    {
        final BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();

        // Create and add a duplicate attribute which is not allowed.
        Attribute newAttribute = new Attribute();
        newAttribute.setName("Valid Name");
        newAttribute.setValue(new String(new char[4001]).replace('\0', 'A')); // Test value greater than 4000 byte limit.
        businessObjectDataCreateRequest.getAttributes().add(newAttribute);

        executeWithoutLogging(SqlExceptionHelper.class, new Command()
        {
            @Override
            public void execute()
            {
                // Create the business object data which is invalid.
                businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
            }
        });
    }

    @Test
    public void testCreateBusinessObjectDataS3ManagedBucket() throws Exception
    {
        // Create relative database entities.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, false, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create and upload to S3 managed storage a set of test files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, LOCAL_FILES);

        // Build a new business object data create request.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, StorageEntity.MANAGED_STORAGE, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));

        // Create the business object data.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.createBusinessObjectData(request);

        // Verify the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectFormatEntity, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS,
                StorageEntity.MANAGED_STORAGE, testS3KeyPrefix, businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES),
                CollectionUtils.isEmpty(request.getAttributes()) ? new ArrayList<Attribute>() : request.getAttributes(), resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataS3ManagedBucketExtraFilesInS3() throws Exception
    {
        // Create relative database entities.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, false, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create and upload to S3 managed storage a set of test files including extra
        // files not to be listed in the create business object data create request.
        List<String> localFiles = new ArrayList<>();
        localFiles.addAll(LOCAL_FILES);
        localFiles.add(FILE_NAME);
        localFiles.add(FILE_NAME_2);
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, localFiles);

        // Build a new business object data create request.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, StorageEntity.MANAGED_STORAGE, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));

        // Create the business object data.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.createBusinessObjectData(request);

        // Verify the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectFormatEntity, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS,
                StorageEntity.MANAGED_STORAGE, testS3KeyPrefix, businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES),
                CollectionUtils.isEmpty(request.getAttributes()) ? new ArrayList<>() : request.getAttributes(), resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataS3ManagedBucketInvalidStorageDirectory()
    {
        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Build a business object data create request with directory path not matching the expected S3 key prefix.
        String invalidS3KeyPrefix = "INVALID_S3_KEY_PREFIX";
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, StorageEntity.MANAGED_STORAGE, invalidS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));

        try
        {
            // Try to create a business object data instance.
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when a storage directory path in S3 managed storage does not match the expected S3 key prefix.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Specified directory path \"%s\" does not match the expected S3 key prefix \"%s\".", invalidS3KeyPrefix, testS3KeyPrefix),
                e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataS3ManagedBucketInvalidStorageFile()
    {
        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Build a business object data create request with a storage file path not matching the expected S3 key prefix.
        String invalidS3KeyPrefix = "INVALID_S3_KEY_PREFIX";
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, StorageEntity.MANAGED_STORAGE, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(invalidS3KeyPrefix, Arrays.asList(LOCAL_FILE)));

        try
        {
            // Try to create a business object data instance.
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an IllegalArgumentException when a storage storage file path in S3 managed storage does not match the expected S3 key prefix.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Specified storage file path \"%s/%s\" does not match the expected S3 key prefix \"%s\".", invalidS3KeyPrefix, LOCAL_FILE,
                    testS3KeyPrefix), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataS3ManagedBucketDirectoryPathAlreadyRegistered()
    {
        // Create relative database entities including a storage unit for the business object data with PARTITION_VALUE_2 partition value,
        // but with a directory path that would actually match with a test business object data with PARTITION_VALUE partition value.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE_2,
                INITIAL_DATA_VERSION, true, BDATA_STATUS);
        storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDao.getStorageByName(StorageEntity.MANAGED_STORAGE), businessObjectDataEntity, StorageUnitStatusEntity.ENABLED,
                testS3KeyPrefix);

        // Build a new business object data create request containing the already registered storage directory path.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, StorageEntity.MANAGED_STORAGE, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, Arrays.asList(LOCAL_FILE)));

        try
        {
            // Try to create a business object data instance.
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an AlreadyExistsException when directory path in S3 managed " +
                "storage matches the location of an already registered business object data.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Storage directory \"%s\" in \"%s\" storage is already registered by the business object data {%s}.", testS3KeyPrefix,
                StorageEntity.MANAGED_STORAGE, businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                        PARTITION_VALUE_2, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataS3ManagedBucketFileAlreadyRegistered()
    {
        // Create relative database entities including a storage file entity registered by a test business object data with PARTITION_VALUE_2 partition value.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE_2,
                INITIAL_DATA_VERSION, true, BDATA_STATUS);
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDao.getStorageByName(StorageEntity.MANAGED_STORAGE), businessObjectDataEntity, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper
            .createStorageFileEntity(storageUnitEntity, String.format("%s/%s", testS3KeyPrefix, LOCAL_FILE), FILE_SIZE_1_KB, ROW_COUNT_1000);

        // Build a new business object data create request containing the already registered storage file.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, StorageEntity.MANAGED_STORAGE, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, Arrays.asList(LOCAL_FILE)));

        try
        {
            // Try to create a business object data instance.
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an AlreadyExistsException when a storage file in S3 managed storage is already registered by another business object data.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String
                .format("Found 1 storage file(s) matching \"%s\" S3 key prefix in \"%s\" storage that is registered with another business object data.",
                    testS3KeyPrefix, StorageEntity.MANAGED_STORAGE), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataS3ManagedBucketS3FileNotFound()
    {
        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, false, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Build a new business object data create request with a storage file which was not uploaded to S3 managed storage.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, StorageEntity.MANAGED_STORAGE, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, Arrays.asList(LOCAL_FILE)));

        try
        {
            // Try to create a business object data instance.
            businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
            fail("Should throw an ObjectNotFoundException when a storage file does not exist in S3 managed storage.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("File not found at s3://%s/%s/%s location.", storageDaoTestHelper.getS3ManagedBucketName(), testS3KeyPrefix, LOCAL_FILE),
                e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataS3ManagedBucketS3FileSizeMismatch() throws Exception
    {
        // Create relative database entities.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, false, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create and upload to S3 managed storage a set of test files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(LOCAL_FILE));

        // Build a new business object data create request.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, StorageEntity.MANAGED_STORAGE, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, Arrays.asList(LOCAL_FILE)));

        // Change the storage file size to 0 bytes in the request.
        request.getStorageUnits().get(0).getStorageFiles().get(0).setFileSizeBytes(FILE_SIZE_0_BYTE);

        // Try to create a business object data instance when storage file size does not match to file size reported by S3.
        try
        {
            businessObjectDataService.createBusinessObjectData(request);
            fail("Should throw an ObjectNotFoundException when a storage file size does not match file size reported by S3.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Specified file size of %d bytes for \"%s/%s\" storage file does not match file size of %d bytes reported by S3.", FILE_SIZE_0_BYTE,
                    testS3KeyPrefix, LOCAL_FILE, FILE_SIZE_1_KB), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataS3ManagedBucketWithZeroByteDirectoryMarkers() throws Exception
    {
        // Create relative database entities.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, false, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Place test files and 0 byte S3 directory markers in the S3 managed storage.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, LOCAL_FILES, S3_DIRECTORY_MARKERS);

        // Build a new business object data create request.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, StorageEntity.MANAGED_STORAGE, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));

        // Create the business object data.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.createBusinessObjectData(request);

        // Verify the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectFormatEntity, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS,
                StorageEntity.MANAGED_STORAGE, testS3KeyPrefix, businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES),
                CollectionUtils.isEmpty(request.getAttributes()) ? new ArrayList<Attribute>() : request.getAttributes(), resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataDuplicateParents()
    {
        // This will create a business object data create request with parents.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest(true);

        // Add a duplicate parent.
        List<BusinessObjectDataKey> businessObjectDataKeys = businessObjectDataCreateRequest.getBusinessObjectDataParents();
        businessObjectDataKeys.add(businessObjectDataKeys.get(0));

        // Try to create a business object data with duplicate parents.
        try
        {
            businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
            fail("Should throw an IllegalArgumentException when business object data create request contains duplicate parents.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object data keys can not contain duplicates.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataParentMissingBusinessObjectFormatVersion()
    {
        // This will create a business object data create request with parents.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest(true);

        // Remove the business object format version required field.
        List<BusinessObjectDataKey> businessObjectDataKeys = businessObjectDataCreateRequest.getBusinessObjectDataParents();
        businessObjectDataKeys.get(0).setBusinessObjectFormatVersion(null);

        // Try to create a business object data which should fail since one of the required attributes is missing.
        try
        {
            businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
            fail("Should throw an IllegalArgumentException when business object format version is not specified for a business object data parent.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataParentMissingPartitionValue()
    {
        // This will create a business object data create request with parents.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest(true);

        // Remove the partition value required field.
        List<BusinessObjectDataKey> businessObjectDataKeys = businessObjectDataCreateRequest.getBusinessObjectDataParents();
        businessObjectDataKeys.get(0).setPartitionValue(null);

        // Try to create a business object data which should fail since one of the required attributes is missing.
        try
        {
            businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
            fail("Should throw an IllegalArgumentException when partition value is not specified for a business object data parent.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataParentMissingBusinessObjectDataVersion()
    {
        // This will create a business object data create request with parents.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest(true);

        // Remove the business object data version required field.
        List<BusinessObjectDataKey> businessObjectDataKeys = businessObjectDataCreateRequest.getBusinessObjectDataParents();
        businessObjectDataKeys.get(0).setBusinessObjectDataVersion(null);

        // Try to create a business object data which should fail since one of the required attributes is missing.
        try
        {
            businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
            fail("Should throw an IllegalArgumentException when business object data version is not specified for a business object data parent.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data version must be specified.", e.getMessage());
        }
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testCreateBusinessObjectDataParentNoExists()
    {
        // This will create a business object data create request with parents.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest(true);

        // Set the partition value to a business object data that doesn't exist.
        List<BusinessObjectDataKey> businessObjectDataKeys = businessObjectDataCreateRequest.getBusinessObjectDataParents();
        businessObjectDataKeys.get(0).setPartitionValue("Invalid_Partition_Value");

        // Create the business object data which should fail since required attributes are missing.
        businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
    }

    /**
     * This test case validates that business object data registration succeeds even when specified business object data parents form a circular dependency. We
     * have B registered B as a child of C and C registered as a child of B (B->C and C->B), and now we try to register A as a child of C.  That business object
     * data registration is expected not to fail due to a pre-existing circular dependency between B and C.  Circular dependency for the already registered
     * business object data instances is an edge case and should not happen for normal operations and can only occur if the database was modified directly.
     */
    @Test
    public void testCreateBusinessObjectDataIgnoringCircularDependency()
    {
        // Create two business object data keys.
        BusinessObjectDataKey alphaBusinessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);
        BusinessObjectDataKey betaBusinessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create the relative business object data entities.
        BusinessObjectDataEntity alphaBusinessObjectDataEntity =
            businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(alphaBusinessObjectDataKey, true, BDATA_STATUS);
        BusinessObjectDataEntity betaBusinessObjectDataEntity =
            businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(betaBusinessObjectDataKey, true, BDATA_STATUS);

        // Associate with each other the two business object data entities created above, so we get a circular dependency.
        // Make "alpha" a parent of "beta".
        alphaBusinessObjectDataEntity.getBusinessObjectDataChildren().add(betaBusinessObjectDataEntity);
        betaBusinessObjectDataEntity.getBusinessObjectDataParents().add(alphaBusinessObjectDataEntity);
        // Make "beta" a parent of "alpha".
        betaBusinessObjectDataEntity.getBusinessObjectDataChildren().add(alphaBusinessObjectDataEntity);
        alphaBusinessObjectDataEntity.getBusinessObjectDataParents().add(betaBusinessObjectDataEntity);

        // Create other database entities required for testing.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME);

        // Create a business object data create request with one of the entities created above listed as a parent.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE_2, BDATA_STATUS, STORAGE_NAME, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, Arrays.asList(LOCAL_FILE)));
        List<BusinessObjectDataKey> parents = new ArrayList<>();
        request.setBusinessObjectDataParents(parents);
        parents.add(betaBusinessObjectDataKey);

        // Create a business object data when a parent is part of a circular dependency.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.createBusinessObjectData(request);

        // Validate the results.
        assertNotNull(resultBusinessObjectData);
        assertNotNull(resultBusinessObjectData.getBusinessObjectDataParents());
        assertEquals(1, resultBusinessObjectData.getBusinessObjectDataParents().size());
        assertTrue(resultBusinessObjectData.getBusinessObjectDataParents().contains(betaBusinessObjectDataKey));
    }

    @Test
    public void testCreateBusinessObjectDataDiscoverStorageFiles() throws Exception
    {
        // Create a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create a business object data status entity.
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create and upload to S3 managed storage a set of test files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, LOCAL_FILES);

        // Build a new business object data create request with enabled discovery of storage files.
        BusinessObjectDataCreateRequest request =
            new BusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, BDATA_STATUS, Arrays.asList(
                new StorageUnitCreateRequest(StorageEntity.MANAGED_STORAGE, new StorageDirectory(testS3KeyPrefix), NO_STORAGE_FILES, DISCOVER_STORAGE_FILES)),
                NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION);

        // Create the business object data.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.createBusinessObjectData(request);

        // Verify the results.
        assertEquals(
            new BusinessObjectData(resultBusinessObjectData.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_KEY, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, Arrays.asList(
                new StorageUnit(new Storage(StorageEntity.MANAGED_STORAGE, StoragePlatformEntity.S3, Arrays.asList(
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                        storageDaoTestHelper.getS3ManagedBucketName()),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                        S3_KEY_PREFIX_VELOCITY_TEMPLATE),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.TRUE.toString()),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), Boolean.TRUE.toString()),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.TRUE.toString()))),
                    new StorageDirectory(testS3KeyPrefix), businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES, false),
                    StorageUnitStatusEntity.ENABLED, NO_STORAGE_UNIT_STATUS_HISTORY, NO_STORAGE_POLICY_TRANSITION_FAILED_ATTEMPTS, NO_RESTORE_EXPIRATION_ON)),
                NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_BUSINESS_OBJECT_DATA_CHILDREN, NO_BUSINESS_OBJECT_DATA_STATUS_HISTORY,
                NO_RETENTION_EXPIRATION_DATE), resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataDiscoverStorageFilesNoStorageDirectory()
    {
        // Try to create an initial version of the business object data when discovery of storage files is enabled and storage directory is not specified.
        try
        {
            businessObjectDataService.createBusinessObjectData(
                new BusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                    PARTITION_VALUE, NO_SUBPARTITION_VALUES, BusinessObjectDataStatusEntity.VALID,
                    Arrays.asList(new StorageUnitCreateRequest(STORAGE_NAME, NO_STORAGE_DIRECTORY, NO_STORAGE_FILES, DISCOVER_STORAGE_FILES)), NO_ATTRIBUTES,
                    NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION));
            fail("Should throw an IllegalArgumentException when discovery of storage files is enabled and storage directory is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage directory must be specified when discovery of storage files is enabled.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataDiscoverStorageFilesStorageFilesSpecified()
    {
        // Try to create an initial version of the business object data when discovery of storage files is enabled and storage files are specified.
        try
        {
            businessObjectDataService.createBusinessObjectData(
                new BusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                    PARTITION_VALUE, NO_SUBPARTITION_VALUES, BusinessObjectDataStatusEntity.VALID, Arrays.asList(
                    new StorageUnitCreateRequest(STORAGE_NAME, new StorageDirectory(STORAGE_DIRECTORY_PATH),
                        Arrays.asList(new StorageFile(LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000)), DISCOVER_STORAGE_FILES)), NO_ATTRIBUTES,
                    NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION));
            fail("Should throw an IllegalArgumentException when discovery of storage files is enabled and storage files are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Storage files cannot be specified when discovery of storage files is enabled.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataDiscoverStorageFilesInvalidStoragePlatform()
    {
        // Create a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create a business object data status entity.
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create a non-S3 storage entity with a  bucket name attribute.
        storageDaoTestHelper
            .createStorageEntity(STORAGE_NAME, STORAGE_PLATFORM_CODE, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                S3_BUCKET_NAME);

        // Try to create an initial version of the business object data when storage platform is not supported for discovery of storage files.
        try
        {
            businessObjectDataService.createBusinessObjectData(
                new BusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                    PARTITION_VALUE, NO_SUBPARTITION_VALUES, BDATA_STATUS,
                    Arrays.asList(new StorageUnitCreateRequest(STORAGE_NAME, new StorageDirectory(testS3KeyPrefix), NO_STORAGE_FILES, DISCOVER_STORAGE_FILES)),
                    NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION));
            fail("Should throw an IllegalArgumentException when storage platform is not supported for discovery of storage files.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Cannot discover storage files at \"%s\" storage platform.", STORAGE_PLATFORM_CODE), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataDiscoverStorageFilesNoS3FilesExist()
    {
        // Create a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create a business object data status entity.
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Try to create an initial version of the business object data when there are no files in S3 to discover.
        try
        {
            businessObjectDataService.createBusinessObjectData(
                new BusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                    PARTITION_VALUE, NO_SUBPARTITION_VALUES, BDATA_STATUS, Arrays.asList(
                    new StorageUnitCreateRequest(StorageEntity.MANAGED_STORAGE, new StorageDirectory(testS3KeyPrefix), NO_STORAGE_FILES,
                        DISCOVER_STORAGE_FILES)), NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION));
            fail("Should throw an ObjectNotFoundException when there are no files in S3 to discover.");
        }
        catch (ObjectNotFoundException e)
        {
            assertTrue(e.getMessage().startsWith(String.format("Found no files at \"s3://%s", storageDaoTestHelper.getS3ManagedBucketName())));
        }
    }

    @Test
    public void testCreateBusinessObjectDataDiscoverStorageFilesStorageDirectoryEndsWithSlash() throws Exception
    {
        // Create a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create a business object data status entity.
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create and upload to S3 managed storage a set of test files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, LOCAL_FILES);

        // Create an S3 storage entity with a bucket name attribute with a value matching to the test S3 managed storage (required for unit test clean up).
        String testBucketName = storageDaoTestHelper.getS3ManagedBucketName();
        storageDaoTestHelper
            .createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                testBucketName);

        // Build a new business object data create request with enabled discovery of storage files and with storage directory ending with a slash.
        String testStorageDirectoryPath = testS3KeyPrefix + "/";
        BusinessObjectDataCreateRequest request =
            new BusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, BDATA_STATUS, Arrays
                .asList(new StorageUnitCreateRequest(STORAGE_NAME, new StorageDirectory(testStorageDirectoryPath), NO_STORAGE_FILES, DISCOVER_STORAGE_FILES)),
                NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION);

        // Create the business object data.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.createBusinessObjectData(request);

        // Verify the results.
        assertEquals(
            new BusinessObjectData(resultBusinessObjectData.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_KEY, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, Arrays.asList(
                new StorageUnit(new Storage(STORAGE_NAME, StoragePlatformEntity.S3,
                    Arrays.asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), testBucketName))),
                    new StorageDirectory(testStorageDirectoryPath),
                    businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES, false), StorageUnitStatusEntity.ENABLED,
                    NO_STORAGE_UNIT_STATUS_HISTORY, NO_STORAGE_POLICY_TRANSITION_FAILED_ATTEMPTS, NO_RESTORE_EXPIRATION_ON)), NO_ATTRIBUTES,
                NO_BUSINESS_OBJECT_DATA_PARENTS, NO_BUSINESS_OBJECT_DATA_CHILDREN, NO_BUSINESS_OBJECT_DATA_STATUS_HISTORY, NO_RETENTION_EXPIRATION_DATE),
            resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataPreRegistrationAssertDirectoryPathNotRequiredWhenStatusIsPreRegistrationAndDirectoryIsSetInResponse()
    {
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME);
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                "foo"));

        // Create an initial version of the business object data.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();
        businessObjectDataCreateRequest.setSubPartitionValues(null);
        businessObjectDataCreateRequest.setStatus("UPLOADING");
        businessObjectDataCreateRequest.setStorageUnits(Arrays.asList(new StorageUnitCreateRequest(STORAGE_NAME, null, null, null)));
        BusinessObjectData businessObjectData = businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
        assertEquals("foo", businessObjectData.getStorageUnits().get(0).getStorageDirectory().getDirectoryPath());
    }

    @Test
    public void testCreateBusinessObjectDataPreRegistrationAssertCannotCreateNewVersionWhenLatestIsPreRegistrationStatus()
    {
        // Create an initial version of the business object data.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();
        businessObjectDataCreateRequest.setStatus("UPLOADING");
        businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);

        businessObjectDataCreateRequest.setCreateNewVersion(true);
        businessObjectDataCreateRequest.setStatus("VALID");

        try
        {
            businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AlreadyExistsException.class, e.getClass());
            assertEquals("Unable to create business object data because it already exists.", e.getMessage());
        }
    }
}
