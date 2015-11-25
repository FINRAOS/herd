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
package org.finra.dm.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.dm.model.AlreadyExistsException;
import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.dm.model.api.xml.SchemaColumn;
import org.finra.dm.model.api.xml.StorageFile;
import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.DataProviderEntity;
import org.finra.dm.model.jpa.FileTypeEntity;
import org.finra.dm.model.jpa.NamespaceEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.jpa.StorageFileEntity;
import org.finra.dm.model.jpa.StorageUnitEntity;

public class BusinessObjectDataStorageFileServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "businessObjectDataStorageFileServiceImpl")
    private BusinessObjectDataStorageFileService businessObjectDataStorageFileServiceImpl;

    private static final String FILE_PATH_1 = "file1";
    private static final String FILE_PATH_2 = "file2";

    private static final String PARTITION_KEY_2 = "pk2_" + Math.random();
    private static final String PARTITION_KEY_3 = "pk3_" + Math.random();
    private static final String PARTITION_KEY_4 = "pk4_" + Math.random();
    private static final String PARTITION_KEY_5 = "pk5_" + Math.random();

    private static final String PARTITION_VALUE_2 = "pv2_" + Math.random();
    private static final String PARTITION_VALUE_3 = "pv3_" + Math.random();
    private static final String PARTITION_VALUE_4 = "pv4_" + Math.random();
    private static final String PARTITION_VALUE_5 = "pv5_" + Math.random();

    private static final List<String> SUB_PARTITION_VALUES = Arrays.asList(PARTITION_VALUE_2, PARTITION_VALUE_3, PARTITION_VALUE_4, PARTITION_VALUE_5);

    private String testS3KeyPrefix =
        getExpectedS3KeyPrefix(NAMESPACE_CD, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
            PARTITION_VALUE, null, null, DATA_VERSION);

    /**
     * Initialize the environment. This method is run once before any of the test methods in the class.
     */
    @BeforeClass
    public static void initEnv() throws IOException
    {
        localTempPath = Paths.get(System.getProperty("java.io.tmpdir"), "dm-bod-service-create-test-local-folder");
    }

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        // Create local temp directory.
        localTempPath.toFile().mkdir();
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
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(testS3KeyPrefix);
        s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFiles()
    {
        createDataWithSubPartitions();

        BusinessObjectDataStorageFilesCreateRequest request =
            createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)));

        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesMissingRequiredParameters()
    {
        // Try to add storage files when business object definition name is not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))));
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
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))));
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
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE,
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))));
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
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE,
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))));
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
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT,
                    SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))));
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
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, Arrays.asList(BLANK_TEXT), DATA_VERSION, STORAGE_NAME,
                    Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))));
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
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUB_PARTITION_VALUES, null, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))));
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
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, BLANK_TEXT, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))));
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
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, null));
            fail("Should throw an IllegalArgumentException when storage files are not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one storage file must be specified.", e.getMessage());
        }

        // Try to add storage files when storage file path is not specified.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(BLANK_TEXT, FILE_SIZE_1_KB, ROW_COUNT_1000))));
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
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, null, ROW_COUNT_1000))));
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
        // Create and persist a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        createData(null, false);

        // Create business object data storage files without passing any of the optional parameters.
        BusinessObjectDataStorageFilesCreateRequest request =
            createBusinessObjectDataStorageFilesCreateRequest(BLANK_TEXT, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, null)));

        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            NO_SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesTrimParameters()
    {
        createDataWithSubPartitions();

        // Create business object data storage files by passing all input parameters with leading and trailing empty spaces.
        BusinessObjectDataStorageFilesCreateRequest request =
            createBusinessObjectDataStorageFilesCreateRequest(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUB_PARTITION_VALUES), DATA_VERSION,
                addWhitespace(STORAGE_NAME), Arrays.asList(createFile(addWhitespace(FILE_PATH_2), FILE_SIZE_1_KB, ROW_COUNT_1000)));

        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)), response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesUpperCaseParameters()
    {
        createDataWithSubPartitions();

        // Create business object data storage files using upper case input parameters (except for case-sensitive partition values and storage file paths).
        BusinessObjectDataStorageFilesCreateRequest request =
            createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME.toUpperCase(),
                Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)));

        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesLowerCaseParameters()
    {
        createDataWithSubPartitions();

        // Create business object data storage files using lower case input parameters (except for case-sensitive partition values and storage file paths).
        BusinessObjectDataStorageFilesCreateRequest request =
            createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME.toLowerCase(),
                Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)));

        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesInvalidParameters()
    {
        createDataWithSubPartitions();

        // Try to add business object data storage files using an invalid row count number.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, -1l))));
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
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME,
                    Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000), createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))));
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
            createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile("some/path/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)));
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            NO_SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesWithStorageDirectoryIncorrectStorageDirectory()
    {
        createData("some/path", false);

        // Try to add a storage file when the storage file path doesn't match the storage directory path.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, null, DATA_VERSION, STORAGE_NAME,
                    Arrays.asList(createFile("incorrect/path/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))));
            fail("Should throw an IllegalArgumentException when a storage file path does not match the storage directory path.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage file path \"incorrect/path/%s\" does not match the storage directory path \"some/path\".", FILE_PATH_2),
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
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, null, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))));
            fail("Should throw an ObjectNotFoundException when using non-existing business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
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
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, null, DATA_VERSION, "I_DO_NOT_EXIST", Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))));
            fail("Should throw an ObjectNotFoundException when using non-existing storage.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage unit not found for business data {%s} and storage name \"I_DO_NOT_EXIST\".",
                getExpectedBusinessObjectDataKeyAsString(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesStorageFileAlreadyExists()
    {
        createData(null, false);

        // Try to add an already registered storage file.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, null, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_1, FILE_SIZE_1_KB, ROW_COUNT_1000))));
            fail("Should throw an AlreadyExistsException when request contains storage file what is already registered.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("S3 file \"%s\" in \"%s\" storage is already registered by the business object data {%s}.", FILE_PATH_1, STORAGE_NAME,
                getExpectedBusinessObjectDataKeyAsString(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, DATA_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataStorageFilesS3Managed() throws Exception
    {
        createData(null, true);
        prepareTestS3Files(testS3KeyPrefix, Arrays.asList(FILE_PATH_1));

        BusinessObjectDataStorageFilesCreateRequest request =
            createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                Arrays.asList(createFile(testS3KeyPrefix + "/" + FILE_PATH_1, FILE_SIZE_1_KB, ROW_COUNT_1000)));
        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            NO_SUBPARTITION_VALUES, DATA_VERSION, StorageEntity.MANAGED_STORAGE, request.getStorageFiles(), response);
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
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Arrays.asList(createFile(invalidS3KeyPrefix + "/" + FILE_PATH_1, FILE_SIZE_1_KB, ROW_COUNT_1000))));
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
    public void testCreateBusinessObjectDataStorageFilesS3ManagedS3FileNotFound() throws Exception
    {
        createData(null, true);
        prepareTestS3Files(testS3KeyPrefix, Arrays.asList(FILE_PATH_1));

        // Try to add a storage file that does not exist in S3 managed storage.
        try
        {
            businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(
                createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, null, DATA_VERSION, StorageEntity.MANAGED_STORAGE,
                    Arrays.asList(createFile(testS3KeyPrefix + "/" + FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000))));
            fail("Should throw an ObjectNotFoundException when a storage file does not exist in S3 managed storage.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("File not found at s3://%s/%s/%s location.", getS3ManagedBucketName(), testS3KeyPrefix, FILE_PATH_2), e.getMessage());
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
            assertEquals("A business object definition name must be specified.", e.getMessage());
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
        BusinessObjectDataEntity bod = super
            .createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION,
                true, BDATA_STATUS);

        StorageEntity s;
        if (s3Managed)
        {
            s = dmDao.getStorageByName(StorageEntity.MANAGED_STORAGE);
        }
        else
        {
            s = super.createStorageEntity(STORAGE_NAME, STORAGE_PLATFORM_CODE);
        }

        StorageUnitEntity su = super.createStorageUnitEntity(s, bod, storageUnitDirectory);
        for (String file : files)
        {
            StorageFileEntity f = super.createStorageFileEntity(su, file, FILE_SIZE_1_KB, ROW_COUNT_1000);
            su.getStorageFiles().add(f);
        }
    }

    private void createDataWithSubPartitions()
    {
        NamespaceEntity namespaceEntity = super.createNamespaceEntity(NAMESPACE_CD);
        DataProviderEntity dataProviderEntity = super.createDataProviderEntity(DATA_PROVIDER_NAME);
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            super.createBusinessObjectDefinitionEntity(namespaceEntity, BOD_NAME, dataProviderEntity, null, null, true);
        FileTypeEntity fileTypeEntity = super.createFileTypeEntity(FORMAT_FILE_TYPE_CODE, FORMAT_DESCRIPTION);
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
        BusinessObjectFormatEntity businessObjectFormatEntity = super
            .createBusinessObjectFormatEntity(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity, FORMAT_VERSION, null, true, PARTITION_KEY,
                null, NO_ATTRIBUTES, null, null, null, schemaColumns, null);
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        StorageEntity storageEntity = super.createStorageEntity(STORAGE_NAME);
        StorageUnitEntity storageUnitEntity = super.createStorageUnitEntity(storageEntity, businessObjectDataEntity);

        super.createStorageFileEntity(storageUnitEntity, FILE_PATH_1, FILE_SIZE_1_KB, null);
    }
}
