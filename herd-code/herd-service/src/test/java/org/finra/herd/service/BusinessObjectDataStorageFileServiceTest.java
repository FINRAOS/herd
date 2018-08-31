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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
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

    private static final String FILE_PATH_3 = "file3";

    private static final String PARTITION_KEY_2 = "pk2_" + Math.random();

    private static final String PARTITION_KEY_3 = "pk3_" + Math.random();

    private static final String PARTITION_KEY_4 = "pk4_" + Math.random();

    private static final String PARTITION_KEY_5 = "pk5_" + Math.random();

    private static final String PARTITION_VALUE_2 = "pv2_" + Math.random();

    private static final String PARTITION_VALUE_3 = "pv3_" + Math.random();

    private static final String PARTITION_VALUE_4 = "pv4_" + Math.random();

    private static final String PARTITION_VALUE_5 = "pv5_" + Math.random();

    private static final List<String> SUB_PARTITION_VALUES = Arrays.asList(PARTITION_VALUE_2, PARTITION_VALUE_3, PARTITION_VALUE_4, PARTITION_VALUE_5);

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
    public void testCreateBusinessObjectDataStorageFiles()
    {
        createDataWithSubPartitions();

        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                NO_DISCOVER_STORAGE_FILES);

        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesMissingRequiredParameters()
    {
        // Try to add storage files when business object definition name is not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
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
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
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
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
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
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
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
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
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
                    Arrays.asList(BLANK_TEXT), DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
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
                    SUB_PARTITION_VALUES, null, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
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
                    SUB_PARTITION_VALUES, DATA_VERSION, BLANK_TEXT, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
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
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(BLANK_TEXT, FILE_SIZE_1_KB, ROW_COUNT_1000)),
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
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, null, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when storage file size is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A file size must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesMissingOptionalParameters()
    {
        // Create and persist a business object definition.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        createData(null, false);

        // Create business object data storage files without passing any of the optional parameters.
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, null)), NO_DISCOVER_STORAGE_FILES);

        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesTrimParameters()
    {
        createDataWithSubPartitions();

        // Create business object data storage files by passing all input parameters with leading and trailing empty spaces.
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUB_PARTITION_VALUES), DATA_VERSION,
                addWhitespace(STORAGE_NAME), Arrays.asList(createFile(addWhitespace(FILE_PATH_2), FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES);

        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesUpperCaseParameters()
    {
        createDataWithSubPartitions();

        // Create business object data storage files using upper case input parameters (except for case-sensitive partition values and storage file paths).
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME.toUpperCase(),
                Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES);

        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesLowerCaseParameters()
    {
        createDataWithSubPartitions();

        // Create business object data storage files using lower case input parameters (except for case-sensitive partition values and storage file paths).
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME.toLowerCase(),
                Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES);

        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesInvalidParameters()
    {
        // Try to add business object data storage files specifying too many sub-partition values.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(PARTITION_VALUE_2, PARTITION_VALUE_3, PARTITION_VALUE_4, PARTITION_VALUE_5, PARTITION_KEY_2), DATA_VERSION, STORAGE_NAME,
                    Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
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
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, -1l)), NO_DISCOVER_STORAGE_FILES));
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
                    Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000), createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when trying to add duplicate storage files.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate storage file found: %s", FILE_PATH_2), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesWithStorageDirectory()
    {
        // Add a storage file to a storage unit with a storage directory path.
        createData("some/path", false);
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile("some/path/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                NO_DISCOVER_STORAGE_FILES);
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);
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
                    null, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile("incorrect/path/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when a storage file path does not match the storage directory path.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage file path \"incorrect/path/%s\" does not match the storage directory path \"some/path/\".", FILE_PATH_2),
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
                    null, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
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
                    null, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
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
                    null, DATA_VERSION, "I_DO_NOT_EXIST", Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
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
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_1, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when adding files to storage with file existence validation enabled without file size validation.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage \"%s\" has file size validation enabled without file existence validation.", STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesPreviouslyRegisteredS3FileSizeMismatchIgnoredDueToDisabledFileSizeValidation() throws Exception
    {
        // Create an S3 storage with file existence validation enabled, but without path prefix validation and file size validation.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays.asList(
            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageDaoTestHelper.getS3ManagedBucketName()),
            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), S3_KEY_PREFIX_VELOCITY_TEMPLATE),
            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.toString(false)),
            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.toString(true)),
            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), Boolean.toString(false))));

        // Create and persist a storage unit entity without a storage directory path.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a storage file with file size that would not match S3 reported size.
        StorageFileEntity storageFileEntity =
            storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, testS3KeyPrefix + "/" + FILE_PATH_1, FILE_SIZE_2_KB, ROW_COUNT_1000);
        storageUnitEntity.getStorageFiles().add(storageFileEntity);

        // Create and upload to S3 managed storage two test files with 1 KB file size.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_1, FILE_PATH_2));

        // Add a second storage file to this business object data.
        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                NO_DISCOVER_STORAGE_FILES);
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);
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
                    null, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_1, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
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
    public void testCreateBusinessObjectDataStorageFilesS3Managed() throws Exception
    {
        createData(null, true, Arrays.asList(testS3KeyPrefix + "/" + FILE_PATH_1));
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_1, FILE_PATH_2));

        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                Arrays.asList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES);
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, request.getStorageFiles(), response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesS3ManagedStorageUnitHasStorageDirectoryPathSet() throws Exception
    {
        // Prepare test data with a storage unit registered with a storage directory path.
        createData(testS3KeyPrefix, true, Arrays.asList(testS3KeyPrefix + "/" + FILE_PATH_1));
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_1, FILE_PATH_2));

        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                Arrays.asList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES);
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, request.getStorageFiles(), response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesS3ManagedExtraFilesInS3() throws Exception
    {
        // Create test data.
        createData(null, true, Arrays.asList(testS3KeyPrefix + "/" + FILE_PATH_1));

        // Create and upload to S3 managed storage a set of test files including an extra
        // file not to be listed in the create business object data create request.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_1, FILE_PATH_2, FILE_PATH_3));

        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                Arrays.asList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES);
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, request.getStorageFiles(), response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesS3ManagedInvalidS3FilePath() throws Exception
    {
        createData(null, true);

        // Try to add a storage file to S3 managed storage when the storage file path doesn't match the expected S3 key prefix.
        String invalidS3KeyPrefix = "INVALID_S3_KEY_PREFIX";
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Arrays.asList(createFile(invalidS3KeyPrefix + "/" + FILE_PATH_1, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
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
        createData(null, true, Arrays.asList(testFilePath));

        // Try to add an already registered storage file.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE, Arrays.asList(createFile(testFilePath, FILE_SIZE_1_KB, ROW_COUNT_1000)),
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
        createData(null, true, Arrays.asList(testS3KeyPrefix + "/" + FILE_PATH_1));

        // Create and upload to S3 managed storage a test file that is not the already registered storage file.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_2));

        // Try to add a storage file to the business object data when an already registered storage file is not found in S3.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Arrays.asList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
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
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_1, FILE_PATH_2));

        // Try to add storage file to the business object data when an already registered storage file has a null file size.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Arrays.asList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when an already registered storage file has a null file size");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Previously registered storage file \"%s/%s\" has no file size specified.", testS3KeyPrefix, FILE_PATH_1, FILE_SIZE_2_KB,
                FILE_SIZE_1_KB), e.getMessage());
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
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_1, FILE_PATH_2));

        // Try to add storage file to the business object data when file size reported by S3 does not match for an already registered storage file.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Arrays.asList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
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
        createData(null, true, Arrays.asList(testS3KeyPrefix + "/" + FILE_PATH_1));
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_1));

        // Try to add a storage file that does not exist in S3 managed storage.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Arrays.asList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
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
        createData(null, true, Arrays.asList(testS3KeyPrefix + "/" + FILE_PATH_1));
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_1, FILE_PATH_2));

        // Try to add a storage file with file size that does not match to file size reported by S3.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Arrays.asList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_0_BYTE, ROW_COUNT_1000)), NO_DISCOVER_STORAGE_FILES));
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
    public void testCreateBusinessObjectDataStorageFilesAutoDiscovery() throws Exception
    {
        createData(testS3KeyPrefix, true, Arrays.asList(testS3KeyPrefix + "/" + FILE_PATH_1));
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_1, FILE_PATH_2));

        // Discover storage files in S3 managed storage.
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, NO_STORAGE_FILES, DISCOVER_STORAGE_FILES));

        // Validate the returned object.
        assertEquals(
            new BusinessObjectDataStorageFilesCreateResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                Arrays.asList(new StorageFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, NO_ROW_COUNT))), response);
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
                    Arrays.asList(createFile(FILE_PATH_1, FILE_SIZE_0_BYTE, ROW_COUNT_1000)), DISCOVER_STORAGE_FILES));
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
        createData(null, true, Arrays.asList(testS3KeyPrefix + "/" + FILE_PATH_1));
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_1, FILE_PATH_2));

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
    public void testCreateBusinessObjectDataStorageFilesAutoDiscoveryStorageDirectoryPathMatchesAnotherBdataStorageFiles() throws Exception
    {
        // Create test data.
        createData(testS3KeyPrefix, true, Arrays.asList(testS3KeyPrefix + "/" + FILE_PATH_1));

        // Create and persist a storage unit entity for another business object data that would also have a storage file starting with the test S3 key prefix.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(StorageEntity.MANAGED_STORAGE, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE_2, NO_SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING,
                StorageUnitStatusEntity.ENABLED, testS3KeyPrefix);

        // Create and persist a storage file for the second business object data that is also starting with the test S3 key prefix.
        StorageFileEntity storageFileEntity =
            storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, NO_ROW_COUNT);
        storageUnitEntity.getStorageFiles().add(storageFileEntity);

        // Try to discover storage files when another business object data have a storage file starting with the test S3 key prefix.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, NO_STORAGE_FILES, DISCOVER_STORAGE_FILES));
            fail("Should throw an IllegalArgumentException when another business object data have a storage file starting with the test S3 key prefix.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Number of storage files (1) already registered for the business object data in \"%s\" storage is not equal " +
                    "to the number of registered storage files (2) matching \"%s/\" S3 key prefix in the same storage.", StorageEntity.MANAGED_STORAGE,
                testS3KeyPrefix), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesAutoDiscoveryPreviouslyRegisteredS3FileNotFound() throws Exception
    {
        // Create test data.
        createData(testS3KeyPrefix, true, Arrays.asList(testS3KeyPrefix + "/" + FILE_PATH_1));

        // Create and upload to S3 managed storage a test file that is not the already registered storage file.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_2));

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
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_1, FILE_PATH_2));

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
        createData(testS3KeyPrefix, true, Arrays.asList(testS3KeyPrefix + "/" + FILE_PATH_1));

        // Create and upload to S3 managed storage only the already registered file.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, Arrays.asList(FILE_PATH_1));

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
    public void testBusinessObjectDataStorageFileServiceMethodsNewTx() throws Exception
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
        createData(storageUnitDirectory, s3Managed, Arrays.asList(FILE_PATH_1));
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

    private void createDataWithSubPartitions()
    {
        NamespaceEntity namespaceEntity = super.namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        DataProviderEntity dataProviderEntity = super.dataProviderDaoTestHelper.createDataProviderEntity(DATA_PROVIDER_NAME);
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = super.businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceEntity, BDEF_NAME, dataProviderEntity, NO_BDEF_DESCRIPTION, NO_BDEF_DISPLAY_NAME, NO_ATTRIBUTES,
                NO_SAMPLE_DATA_FILES);
        FileTypeEntity fileTypeEntity = super.fileTypeDaoTestHelper.createFileTypeEntity(FORMAT_FILE_TYPE_CODE, FORMAT_DESCRIPTION);
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY);
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY_2);
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY_3);
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY_4);
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY_5);
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        BusinessObjectFormatEntity businessObjectFormatEntity = super.businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity, FORMAT_VERSION, null, null, true,
                PARTITION_KEY, null, NO_ATTRIBUTES, null, null, null, schemaColumns, null);
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS, DESCRIPTION, BDATA_STATUS_PRE_REGISTRATION_FLAG_SET);
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, true,
                businessObjectDataStatusEntity.getCode());

        StorageEntity storageEntity = super.storageDaoTestHelper.createStorageEntity(STORAGE_NAME);
        StorageUnitEntity storageUnitEntity = super.storageUnitDaoTestHelper
            .createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        super.storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, FILE_PATH_1, FILE_SIZE_1_KB, null);
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
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_1, FILE_SIZE_1_KB, ROW_COUNT_1000)),
                    NO_DISCOVER_STORAGE_FILES));
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
