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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

public class BusinessObjectDataStorageFileServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "businessObjectDataStorageFileServiceImpl")
    private BusinessObjectDataStorageFileService businessObjectDataStorageFileServiceImpl;

    private static final String FILE_PATH_1 = "file1";

    private static final String FILE_PATH_2 = "file2";

    private static final String PARTITION_KEY_2 = "pk2_" + Math.random();

    private static final String PARTITION_VALUE_2 = "pv2_" + Math.random();

    private static final String PARTITION_VALUE_3 = "pv3_" + Math.random();

    private static final String PARTITION_VALUE_4 = "pv4_" + Math.random();

    private static final String PARTITION_VALUE_5 = "pv5_" + Math.random();

    private static final List<String> SUB_PARTITION_VALUES = Lists.newArrayList(PARTITION_VALUE_2, PARTITION_VALUE_3, PARTITION_VALUE_4, PARTITION_VALUE_5);

    private static final String testS3KeyPrefix =
        getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
            PARTITION_VALUE, null, null, DATA_VERSION);

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
        // Clean up the local directory.
        FileUtils.deleteDirectory(localTempPath.toFile());

        // Clean up the destination S3 folder.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(testS3KeyPrefix);
        s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesMissingRequiredParameters()
    {
        // Try to add storage files when business object definition name is not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME,
                    Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to add storage files when business object format usage is not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to add storage files when business object format file type is not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE,
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to add storage files when business object format version is not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE,
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to add storage files when partition value is not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT,
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to add storage files when subpartition value is not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Lists.newArrayList(BLANK_TEXT), DATA_VERSION, STORAGE_NAME, Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to add storage files when business object data version is not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUB_PARTITION_VALUES, null, STORAGE_NAME, Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when business object data version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data version must be specified.", e.getMessage());
        }

        // Try to add storage files when storage name is not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUB_PARTITION_VALUES, DATA_VERSION, BLANK_TEXT, Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when storage name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage name must be specified.", e.getMessage());
        }

        // Try to add storage files when storage files are not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, null, NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when storage files are not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one storage file must be specified when discovery of storage files is not enabled.", e.getMessage());
        }

        // Try to add storage files when storage file path is not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Lists.newArrayList(createFile(BLANK_TEXT, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when storage file path is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A file path must be specified.", e.getMessage());
        }

        // Try to add storage files when storage file size is not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Lists.newArrayList(createFile(FILE_PATH_2, null, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when storage file size is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A file size must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesInvalidParameters()
    {
        // Try to add business object data storage files specifying too many sub-partition values.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Lists.newArrayList(PARTITION_VALUE_2, PARTITION_VALUE_3, PARTITION_VALUE_4, PARTITION_VALUE_5, PARTITION_KEY_2), DATA_VERSION, STORAGE_NAME,
                    Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when too many sub-partition values are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Exceeded maximum number of allowed subpartitions: %d.", BusinessObjectDataEntity.MAX_SUBPARTITIONS), e.getMessage());
        }

        // Try to add business object data storage files using an invalid row count number.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, -1L)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when storage file has an invalid row count number.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("File \"%s\" has a row count which is < 0.", FILE_PATH_2), e.getMessage());
        }

        // Try to add duplicate business object data storage files.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME,
                    Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000), createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when trying to add duplicate storage files.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate storage file found: %s", FILE_PATH_2), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesWithStorageDirectoryIncorrectStorageDirectory()
    {
        createData("some/path", false);

        // Try to add a storage file when the storage file path doesn't match the storage directory path.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, STORAGE_NAME, Lists.newArrayList(createFile("incorrect/path/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when a storage file path does not match the storage directory path.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Specified storage file path \"incorrect/path/%s\" does not match the storage unit directory path \"some/path/\".", FILE_PATH_2),
                e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesBusinessObjectDataNoExists()
    {
        // Try to add storage files to a non-existing business object data.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, STORAGE_NAME, Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an ObjectNotFoundException when using non-existing business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesBusinessObjectDataHasNoPreRegistrationStatus()
    {
        // Create and persist a business object data status entity that is not flagged as "pre-registration" status.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS, DESCRIPTION, NO_BDATA_STATUS_PRE_REGISTRATION_FLAG_SET);

        // Create and persist a business object data entity.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION, true,
                businessObjectDataStatusEntity.getCode());

        // Try to add storage files to a business object data which is not in a pre-registered state.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, STORAGE_NAME, Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when business object data status is not flagged as a pre-registration status.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format(
                "Business object data status must be one of the pre-registration statuses. " + "Business object data status {%s}, business object data {%s}",
                businessObjectDataStatusEntity.getCode(), businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        NO_PARTITION_VALUES, DATA_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesStorageNoExists()
    {
        createData(null, false);

        // Try to add storage files to a non-existing storage.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, "I_DO_NOT_EXIST", Lists.newArrayList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an ObjectNotFoundException when using non-existing storage.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Could not find storage unit in \"I_DO_NOT_EXIST\" storage for the business object data {%s}.",
                businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        null, DATA_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesStorageHasValidateFileSizeEnabledWithoutValidateFileExistence()
    {
        // Create an S3 storage with file size validation enabled without file existence validation.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME_2),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), Boolean.toString(true))));

        // Create a storage unit.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Try to add storage files to an S3 storage with file existence validation enabled without file size validation.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME,
                    Lists.newArrayList(createFile(FILE_PATH_1, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when adding files to storage with file existence validation enabled without file size validation.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage \"%s\" has file size validation enabled without file existence validation.", STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesStorageFilePreviouslyRegistered()
    {
        createData(null, false);

        // Try to add an already registered storage file.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, STORAGE_NAME, Lists.newArrayList(createFile(FILE_PATH_1, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an AlreadyExistsException when request contains storage file what is already registered.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("S3 file \"%s\" in \"%s\" storage is already registered by the business object data {%s}.", FILE_PATH_1, STORAGE_NAME,
                businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        NO_SUBPARTITION_VALUES, DATA_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesS3ManagedInvalidS3FilePath()
    {
        createData(null, true);

        // Try to add a storage file to S3 managed storage when the storage file path doesn't match the expected S3 key prefix.
        String invalidS3KeyPrefix = "INVALID_S3_KEY_PREFIX";
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Lists.newArrayList(createFile(invalidS3KeyPrefix + "/" + FILE_PATH_1, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when a storage file path in S3 managed storage does not match the expected S3 key prefix.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Specified storage file path \"%s/%s\" does not match the expected S3 key prefix \"%s\".", invalidS3KeyPrefix, FILE_PATH_1,
                    testS3KeyPrefix + "/"), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesS3ManagedStorageFilePreviouslyRegistered()
    {
        // Create a test file path.
        String testFilePath = testS3KeyPrefix + "/" + FILE_PATH_1;

        // Create test data.
        createData(null, true, Lists.newArrayList(testFilePath));

        // Try to add an already registered storage file.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE, Lists.newArrayList(createFile(testFilePath, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an AlreadyExistsException when request contains storage file what is already registered.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String
                .format("S3 file \"%s\" in \"%s\" storage is already registered by the business object data {%s}.", testFilePath, StorageEntity.MANAGED_STORAGE,
                    businessObjectDataServiceTestHelper
                        .getExpectedBusinessObjectDataKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                            PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesS3ManagedPreviouslyRegisteredS3FileNotFound() throws Exception
    {
        // Create test data.
        createData(null, true, Lists.newArrayList(testS3KeyPrefix + "/" + FILE_PATH_1));

        // Create and upload to S3 managed storage a test file that is not the already registered storage file.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Lists.newArrayList(FILE_PATH_2));

        // Try to add a storage file to the business object data when an already registered storage file is not found in S3.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Lists.newArrayList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an ObjectNotFoundException when an already registered storage file not found in S3.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Previously registered storage file not found at s3://%s/%s/%s location.", storageDaoTestHelper.getS3ManagedBucketName(),
                testS3KeyPrefix, FILE_PATH_1), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesS3ManagedPreviouslyRegisteredS3FileSizeIsNull() throws Exception
    {
        // Create and persist a storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(StorageEntity.MANAGED_STORAGE, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING,
                StorageUnitStatusEntity.ENABLED, testS3KeyPrefix);

        // Create and persist a storage file with a null file size.
        StorageFileEntity storageFileEntity =
            storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, testS3KeyPrefix + "/" + FILE_PATH_1, NO_FILE_SIZE, NO_ROW_COUNT);
        storageUnitEntity.getStorageFiles().add(storageFileEntity);

        // Create and upload to S3 managed storage two test files with 1 KB file size.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Lists.newArrayList(FILE_PATH_1, FILE_PATH_2));

        // Try to add storage file to the business object data when an already registered storage file has a null file size.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Lists.newArrayList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when an already registered storage file has a null file size");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Previously registered storage file \"%s/%s\" has no file size specified.", testS3KeyPrefix, FILE_PATH_1),
                e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesS3ManagedPreviouslyRegisteredS3FileSizeMismatch() throws Exception
    {
        // Create and persist a storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(StorageEntity.MANAGED_STORAGE, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING,
                StorageUnitStatusEntity.ENABLED, testS3KeyPrefix);

        // Create and persist a storage file with file size that would not match S3 reported size.
        StorageFileEntity storageFileEntity =
            storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, testS3KeyPrefix + "/" + FILE_PATH_1, FILE_SIZE_2_KB, ROW_COUNT_1000);
        storageUnitEntity.getStorageFiles().add(storageFileEntity);

        // Create and upload to S3 managed storage two test files with 1 KB file size.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Lists.newArrayList(FILE_PATH_1, FILE_PATH_2));

        // Try to add storage file to the business object data when file size reported by S3 does not match for an already registered storage file.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Lists.newArrayList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when an already registered storage file size does not match file size reported by S3.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Previously registered storage file \"%s/%s\" has file size of %d bytes that does not match file size of %d bytes reported by S3.",
                    testS3KeyPrefix, FILE_PATH_1, FILE_SIZE_2_KB, FILE_SIZE_1_KB), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesS3ManagedS3FileNotFound() throws Exception
    {
        createData(null, true, Lists.newArrayList(testS3KeyPrefix + "/" + FILE_PATH_1));
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Lists.newArrayList(FILE_PATH_1));

        // Try to add a storage file that does not exist in S3 managed storage.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Lists.newArrayList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an ObjectNotFoundException when a storage file does not exist in S3 managed storage.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("File not found at s3://%s/%s/%s location.", storageDaoTestHelper.getS3ManagedBucketName(), testS3KeyPrefix, FILE_PATH_2),
                e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesS3ManagedS3FileSizeMismatch() throws Exception
    {
        createData(null, true, Lists.newArrayList(testS3KeyPrefix + "/" + FILE_PATH_1));
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Lists.newArrayList(FILE_PATH_1, FILE_PATH_2));

        // Try to add a storage file with file size that does not match to file size reported by S3.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Lists.newArrayList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_0_BYTE, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when a storage file size does not match file size reported by S3.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Specified file size of %d bytes for \"%s/%s\" storage file does not match file size of %d bytes reported by S3.", FILE_SIZE_0_BYTE,
                    testS3KeyPrefix, FILE_PATH_2, FILE_SIZE_1_KB), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesAutoDiscoveryStorageFilesSpecified()
    {
        // Try to create storage files when discovery of storage files is enabled and storage files are specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Lists.newArrayList(createFile(FILE_PATH_1, FILE_SIZE_0_BYTE, ROW_COUNT_1000)), DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when discovery of storage files is enabled and storage files are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Storage files cannot be specified when discovery of storage files is enabled.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesAutoDiscoveryStorageUnitHasNoStorageDirectoryPath() throws Exception
    {
        createData(null, true, Lists.newArrayList(testS3KeyPrefix + "/" + FILE_PATH_1));
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Lists.newArrayList(FILE_PATH_1, FILE_PATH_2));

        // Try to discover storage files when storage unit has no storage directory path.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, NO_STORAGE_FILES, DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when storage unit has no storage directory path.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object data has no storage directory path which is required for auto-discovery of storage files.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesAutoDiscoveryExplicitlyRegisteredSubPartitionExists()
    {
        // Get a list of two partition columns that is larger than number of partitions supported by business object data registration.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        assertTrue(CollectionUtils.size(partitionColumns) > BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1);

        // Get a list of regular columns.
        List<SchemaColumn> regularColumns = schemaColumnDaoTestHelper.getTestSchemaColumns();

        // Create a business object format with schema that has one more partition column than supported by business object data registration.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, partitionColumns.get(0).getName(), NO_PARTITION_KEY_GROUP,
                NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, SCHEMA_NULL_VALUE_BACKSLASH_N, regularColumns,
                partitionColumns);

        // Create business object data registered using four partition values (primary and three sub-partition values).
        List<String> subPartitionValues = Lists.newArrayList(SUB_PARTITION_VALUE_1, SUB_PARTITION_VALUE_2, SUB_PARTITION_VALUE_3);
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create storage unit and storage file entity for the business object data.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDao.getStorageByName(StorageEntity.MANAGED_STORAGE), businessObjectDataEntity, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Create business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                DATA_VERSION);

        // Create business object data registered using three partition values (primary and two sub-partition values) in UPLOADING state.
        List<String> higherLevelSubPartitionValues = Lists.newArrayList(SUB_PARTITION_VALUE_1, SUB_PARTITION_VALUE_2);
        BusinessObjectDataEntity higherLevelBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                higherLevelSubPartitionValues, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING);

        // Get expected S3 key prefix for the higher level business object data.
        String higherLevelS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                partitionColumns.get(0).getName(), PARTITION_VALUE, Lists.newArrayList(partitionColumns.subList(1, 3)).toArray(new SchemaColumn[0]),
                higherLevelSubPartitionValues.toArray(new String[0]), DATA_VERSION);

        // Create storage unit for the higher level business object data.
        storageUnitDaoTestHelper.createStorageUnitEntity(storageDao.getStorageByName(StorageEntity.MANAGED_STORAGE), higherLevelBusinessObjectDataEntity,
            StorageUnitStatusEntity.ENABLED, higherLevelS3KeyPrefix);

        // Try to discover storage files for the business object data with an expected S3 key prefix
        // that overlaps with the business object data already registered in the same storage.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    higherLevelSubPartitionValues, DATA_VERSION, StorageEntity.MANAGED_STORAGE, NO_STORAGE_FILES, DISCOVER_STORAGE_FILES));
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format(
                "Found another business object data matching \"%s\" S3 key prefix that is also registered in \"%s\" storage. Business object data: {%s}",
                higherLevelS3KeyPrefix, StorageEntity.MANAGED_STORAGE,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesAutoDiscoveryPreviouslyRegisteredS3FileNotFound() throws Exception
    {
        // Create test data.
        createData(testS3KeyPrefix, true, Lists.newArrayList(testS3KeyPrefix + "/" + FILE_PATH_1));

        // Create and upload to S3 managed storage a test file that is not the already registered storage file.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Lists.newArrayList(FILE_PATH_2));

        // Try to discover storage files when an already registered storage file is not found in S3.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, NO_STORAGE_FILES, DISCOVER_STORAGE_FILES));
            fail("Should throw an ObjectNotFoundException when an already registered storage file not found in S3.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Previously registered storage file not found at s3://%s/%s/%s location.", storageDaoTestHelper.getS3ManagedBucketName(),
                testS3KeyPrefix, FILE_PATH_1), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesAutoDiscoveryPreviouslyRegisteredS3FileSizeMismatch() throws Exception
    {
        // Create and persist a storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(StorageEntity.MANAGED_STORAGE, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING,
                StorageUnitStatusEntity.ENABLED, testS3KeyPrefix);

        // Create and persist a storage file with file size that would not match S3 reported size.
        StorageFileEntity storageFileEntity =
            storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, testS3KeyPrefix + "/" + FILE_PATH_1, FILE_SIZE_2_KB, NO_ROW_COUNT);
        storageUnitEntity.getStorageFiles().add(storageFileEntity);

        // Create and upload to S3 managed storage two test files with 1 KB file size.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Lists.newArrayList(FILE_PATH_1, FILE_PATH_2));

        // Try to discover storage files when file size reported by S3 does not match for an already registered storage file.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, NO_STORAGE_FILES, DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when an already registered storage file size does not match file size reported by S3.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Previously registered storage file \"%s/%s\" has file size of %d bytes that does not match file size of %d bytes reported by S3.",
                    testS3KeyPrefix, FILE_PATH_1, FILE_SIZE_2_KB, FILE_SIZE_1_KB), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesAutoDiscoveryNoUnregisteredS3FilesExist() throws Exception
    {
        // Create test data.
        createData(testS3KeyPrefix, true, Lists.newArrayList(testS3KeyPrefix + "/" + FILE_PATH_1));

        // Create and upload to S3 managed storage only the already registered file.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Lists.newArrayList(FILE_PATH_1));

        // Try to discover storage files when no unregistered storage files are present in S3.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, NO_STORAGE_FILES, DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when no unregistered storage files are present in S3.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("No unregistered storage files were discovered at s3://%s/%s/ location.", storageDaoTestHelper.getS3ManagedBucketName(),
                testS3KeyPrefix), e.getMessage());
        }
    }

    /**
     * This method is to get the coverage for the business object data storage file service method that starts the new transaction.
     */
    @Test
    public void testBusinessObjectDataStorageFileServiceMethodsNewTx()
    {
        BusinessObjectDataStorageFilesCreateRequest request = new BusinessObjectDataStorageFilesCreateRequest();
        try
        {
            businessObjectDataStorageFileServiceImpl.createBusinessObjectDataStorageFiles(request);
            fail("Should throw a IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }

    private StorageFile createFile(String filePath, Long size, Long rowCount)
    {
        StorageFile f = new StorageFile();
        f.setFilePath(filePath);
        f.setFileSizeBytes(size);
        f.setRowCount(rowCount);
        return f;
    }

    private void createData(String storageUnitDirectory, boolean s3Managed)
    {
        createData(storageUnitDirectory, s3Managed, Lists.newArrayList(FILE_PATH_1));
    }

    private void createData(String storageUnitDirectory, boolean s3Managed, Collection<String> files)
    {
        // Create and persist a "pre-registration" business object data status entity.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS, DESCRIPTION, BDATA_STATUS_PRE_REGISTRATION_FLAG_SET);

        // Create and persist a business object data entity.
        BusinessObjectDataEntity bod = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION, true,
                businessObjectDataStatusEntity.getCode());

        StorageEntity s;
        if (s3Managed)
        {
            s = storageDao.getStorageByName(StorageEntity.MANAGED_STORAGE);
        }
        else
        {
            s = super.storageDaoTestHelper.createStorageEntity(STORAGE_NAME, STORAGE_PLATFORM_CODE);
        }

        StorageUnitEntity su = super.storageUnitDaoTestHelper.createStorageUnitEntity(s, bod, StorageUnitStatusEntity.ENABLED, storageUnitDirectory);
        for (String file : files)
        {
            StorageFileEntity f = super.storageFileDaoTestHelper.createStorageFileEntity(su, file, FILE_SIZE_1_KB, ROW_COUNT_1000);
            su.getStorageFiles().add(f);
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesBadStorageUnitStatus()
    {
        // Create an S3 storage with file size validation enabled without file existence validation.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME_2),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), Boolean.toString(true))));

        // Create a storage unit.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING, StorageUnitStatusEntity.ARCHIVED,
                NO_STORAGE_DIRECTORY_PATH);

        // Try to add storage files to an S3 storage unit with non-ENABLED status.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME,
                    Lists.newArrayList(createFile(FILE_PATH_1, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when adding files to storage unit with status that is not ENABLED.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Storage unit must be in the ENABLED status. Storage unit status {%s}, business object data {%s}", StorageUnitStatusEntity.ARCHIVED,
                    businessObjectDataServiceTestHelper
                        .getExpectedBusinessObjectDataKeyAsString(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                            PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION)), e.getMessage());
        }
    }
}
