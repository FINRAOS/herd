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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.AbstractServiceTest;

public class StorageFileHelperTest extends AbstractServiceTest
{
    protected static final Path LOCAL_TEMP_PATH = Paths.get(System.getProperty("java.io.tmpdir"), "herd-helper-test", RANDOM_SUFFIX);

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv()
    {
        // Create a local temporary directory.
        LOCAL_TEMP_PATH.toFile().mkdirs();
    }

    /**
     * Cleans up the test environment.
     */
    @After
    public void cleanEnv() throws IOException
    {
        // Clean up the local directory.
        FileUtils.deleteDirectory(LOCAL_TEMP_PATH.toFile());
    }

    @Test
    public void testValidateCopiedS3Files() throws IOException
    {
        // Create two lists of expected and actual storage files.
        // Please note we use different row count values to confirm that row count match is not validated.
        List<StorageFile> testExpectedFiles = new ArrayList<>();
        List<S3ObjectSummary> testActualFiles = new ArrayList<>();
        for (String file : LOCAL_FILES)
        {
            String filePath = String.format(String.format("%s/%s", TEST_S3_KEY_PREFIX, file));
            testExpectedFiles.add(new StorageFile(filePath, FILE_SIZE, ROW_COUNT));
            testActualFiles.add(createS3ObjectSummary(filePath, FILE_SIZE));
        }

        // Validate the files.
        storageFileHelper.validateCopiedS3Files(testExpectedFiles, testActualFiles, STORAGE_NAME,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION));
    }

    @Test
    public void testValidateCopiedS3FilesActualFileNoExists() throws IOException
    {
        // Create two lists of expected and actual storage files, with one expected file not being added to the list of actual files.
        List<StorageFile> testExpectedFiles = Arrays.asList(new StorageFile(TARGET_S3_KEY, FILE_SIZE, ROW_COUNT_1000));
        List<S3ObjectSummary> testActualFiles = new ArrayList<>();

        // Try to validate S3 files when expected S3 file does not exist.
        try
        {
            storageFileHelper.validateCopiedS3Files(testExpectedFiles, testActualFiles, STORAGE_NAME,
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an ObjectNotFoundException when the copied S3 file does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Copied file \"%s\" does not exist in \"%s\" storage.", TARGET_S3_KEY, STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testValidateCopiedS3FilesActualFileSizeMismatch() throws IOException
    {
        // Create two lists of expected and actual storage files, with expected file size not matching actual file size.
        List<StorageFile> testExpectedFiles = Arrays.asList(new StorageFile(TARGET_S3_KEY, FILE_SIZE, NO_ROW_COUNT));
        List<S3ObjectSummary> testActualFiles = Arrays.asList(createS3ObjectSummary(TARGET_S3_KEY, FILE_SIZE_2));

        // Try to validate S3 files when expected file size does not match actual file size.
        try
        {
            storageFileHelper.validateCopiedS3Files(testExpectedFiles, testActualFiles, STORAGE_NAME,
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an IllegalStateException when expected file size does not match actual file size.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String
                .format("Specified file size of %d bytes for copied \"%s\" S3 file in \"%s\" storage does not match file size of %d bytes reported by S3.",
                    FILE_SIZE, TARGET_S3_KEY, STORAGE_NAME, FILE_SIZE_2), e.getMessage());
        }
    }

    @Test
    public void testValidateCopiedS3FilesUnexpectedS3FileFound() throws IOException
    {
        // Create two lists of expected and actual storage files, with an actual file not being added to the list of expected files.
        List<StorageFile> testExpectedFiles = new ArrayList<>();
        List<S3ObjectSummary> testActualFiles = Arrays.asList(createS3ObjectSummary(TARGET_S3_KEY, FILE_SIZE_1_KB));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Try to validate S3 files when unexpected S3 file exists.
        try
        {
            storageFileHelper.validateCopiedS3Files(testExpectedFiles, testActualFiles, STORAGE_NAME, businessObjectDataKey);
            fail("Should throw an IllegalStateException when S3 contains unexpected S3 file.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String
                .format("Found unexpected S3 file \"%s\" in \"%s\" storage while validating copied S3 files. Business object data {%s}", TARGET_S3_KEY,
                    STORAGE_NAME, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testValidateSourceS3Files() throws IOException
    {
        // Create two lists of expected and actual storage files.
        // Please note we use different row count values to confirm that row count match is not validated.
        List<StorageFile> testExpectedFiles = new ArrayList<>();
        List<S3ObjectSummary> testActualFiles = new ArrayList<>();
        for (String file : LOCAL_FILES)
        {
            String filePath = String.format(String.format("%s/%s", TEST_S3_KEY_PREFIX, file));
            testExpectedFiles.add(new StorageFile(filePath, FILE_SIZE, ROW_COUNT));
            testActualFiles.add(createS3ObjectSummary(filePath, FILE_SIZE));
        }

        // Validate the files.
        storageFileHelper.validateSourceS3Files(testExpectedFiles, testActualFiles, STORAGE_NAME,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION));
    }

    @Test
    public void testValidateSourceS3FilesActualFileNoExists() throws IOException
    {
        // Create two lists of expected and actual storage files, with one expected file not being added to the list of actual files.
        List<StorageFile> testExpectedFiles = Arrays.asList(new StorageFile(TARGET_S3_KEY, FILE_SIZE, ROW_COUNT_1000));
        List<S3ObjectSummary> testActualFiles = new ArrayList<>();

        // Try to validate S3 files when expected S3 file does not exist.
        try
        {
            storageFileHelper.validateSourceS3Files(testExpectedFiles, testActualFiles, STORAGE_NAME,
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an ObjectNotFoundException when the registered S3 file does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Registered file \"%s\" does not exist in \"%s\" storage.", TARGET_S3_KEY, STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testValidateSourceS3FilesActualFileSizeMismatch() throws IOException
    {
        // Create two lists of expected and actual storage files, with expected file size not matching actual file size.
        List<StorageFile> testExpectedFiles = Arrays.asList(new StorageFile(TARGET_S3_KEY, FILE_SIZE, NO_ROW_COUNT));
        List<S3ObjectSummary> testActualFiles = Arrays.asList(createS3ObjectSummary(TARGET_S3_KEY, FILE_SIZE_2));

        // Try to validate S3 files when expected expected file size does not match actual file size.
        try
        {
            storageFileHelper.validateSourceS3Files(testExpectedFiles, testActualFiles, STORAGE_NAME,
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an IllegalStateException when expected file size does not match actual file size.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String
                .format("Specified file size of %d bytes for registered \"%s\" S3 file in \"%s\" storage does not match file size of %d bytes reported by S3.",
                    FILE_SIZE, TARGET_S3_KEY, STORAGE_NAME, FILE_SIZE_2), e.getMessage());
        }
    }

    @Test
    public void testValidateSourceS3FilesUnexpectedS3FileFound() throws IOException
    {
        // Create two lists of expected and actual storage files, with an actual file not being added to the list of expected files.
        List<StorageFile> testExpectedFiles = new ArrayList<>();
        List<S3ObjectSummary> testActualFiles = Arrays.asList(createS3ObjectSummary(TARGET_S3_KEY, FILE_SIZE_1_KB));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Try to validate S3 files when unexpected S3 file exists.
        try
        {
            storageFileHelper.validateSourceS3Files(testExpectedFiles, testActualFiles, STORAGE_NAME, businessObjectDataKey);
            fail("Should throw an IllegalStateException when S3 contains unexpected S3 file.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String
                .format("Found unexpected S3 file \"%s\" in \"%s\" storage while validating registered S3 files. Business object data {%s}", TARGET_S3_KEY,
                    STORAGE_NAME, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    /**
     * Creates an S3 object summary.
     *
     * @param filePath the file path
     * @param fileSizeInBytes the file size in bytes
     *
     * @return the S3 object summary
     */
    private S3ObjectSummary createS3ObjectSummary(String filePath, long fileSizeInBytes)
    {
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(filePath);
        s3ObjectSummary.setSize(fileSizeInBytes);
        return s3ObjectSummary;
    }

    @Test
    public void testValidateStorageUnitS3Files() throws IOException
    {
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, LOCAL_FILES, FILE_SIZE_1_KB);

        List<String> actualS3Files = new ArrayList<>();
        for (String file : LOCAL_FILES)
        {
            actualS3Files.add(String.format("%s/%s", TEST_S3_KEY_PREFIX, file));
        }

        storageFileHelper.validateStorageUnitS3Files(storageUnit, actualS3Files, TEST_S3_KEY_PREFIX);
    }

    @Test
    public void testValidateStorageUnitS3FilesS3KeyPrefixMismatch() throws IOException
    {
        StorageUnit storageUnit = createStorageUnit("SOME_S3_KEY_PREFIX", LOCAL_FILES, FILE_SIZE_1_KB);

        // Try to validate S3 files when we have not registered S3 file.
        try
        {
            storageFileHelper.validateStorageUnitS3Files(storageUnit, new ArrayList<>(), TEST_S3_KEY_PREFIX);
            fail("Should throw a RuntimeException when registered S3 file does match S3 key prefix.");
        }
        catch (RuntimeException e)
        {
            String expectedErrMsg = String.format("Storage file S3 key prefix \"%s\" does not match the expected S3 key prefix \"%s\".",
                storageUnit.getStorageFiles().get(0).getFilePath(), TEST_S3_KEY_PREFIX);
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    @Test
    public void testValidateStorageUnitS3FilesRegisteredFileNoExists() throws IOException
    {
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, LOCAL_FILES, FILE_SIZE_1_KB);
        List<String> actualS3Files = new ArrayList<>();

        // Try to validate S3 files when actual S3 files do not exist.
        try
        {
            storageFileHelper.validateStorageUnitS3Files(storageUnit, actualS3Files, TEST_S3_KEY_PREFIX);
            fail("Should throw a RuntimeException when actual S3 files do not exist.");
        }
        catch (RuntimeException e)
        {
            String expectedErrMsg = String
                .format("Registered file \"%s\" does not exist in \"%s\" storage.", storageUnit.getStorageFiles().get(0).getFilePath(),
                    storageUnit.getStorage().getName());
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    @Test
    public void testValidateStorageUnitS3FilesNotRegisteredS3FileFound() throws IOException
    {
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, new ArrayList<>(), FILE_SIZE_1_KB);
        List<String> actualS3Files = Arrays.asList(String.format("%s/%s", TEST_S3_KEY_PREFIX, LOCAL_FILES.get(0)));

        // Try to validate S3 files when we have not registered S3 file.
        try
        {
            storageFileHelper.validateStorageUnitS3Files(storageUnit, actualS3Files, TEST_S3_KEY_PREFIX);
            fail("Should throw a RuntimeException when S3 contains unregistered S3 file.");
        }
        catch (RuntimeException e)
        {
            String expectedErrMsg = String.format("Found S3 file \"%s\" in \"%s\" storage not registered with this business object data.", actualS3Files.get(0),
                storageUnit.getStorage().getName());
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    @Test
    public void testValidateDownloadedS3Files() throws IOException
    {
        File targetLocalDirectory = Paths.get(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX).toFile();
        createLocalFiles(targetLocalDirectory.getPath(), FILE_SIZE_1_KB);
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, LOCAL_FILES, FILE_SIZE_1_KB);
        storageFileHelper.validateDownloadedS3Files(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX, storageUnit);
    }

    @Test
    public void testValidateDownloadedS3FilesZeroFiles() throws IOException
    {
        File targetLocalDirectory = Paths.get(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX).toFile();
        // Create an empty target local directory.
        assertTrue(targetLocalDirectory.mkdirs());
        // Create a storage unit entity without any storage files.
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, null, null);
        // Validate an empty set of the downloaded S3 files.
        storageFileHelper.validateDownloadedS3Files(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX, storageUnit);
    }

    @Test
    public void testValidateDownloadedS3FilesFileCountMismatch() throws IOException
    {
        File targetLocalDirectory = Paths.get(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX).toFile();
        createLocalFiles(targetLocalDirectory.getPath(), FILE_SIZE_1_KB);
        createLocalFile(targetLocalDirectory.getPath(), "EXTRA_FILE", FILE_SIZE_1_KB);
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, LOCAL_FILES, FILE_SIZE_1_KB);

        // Try to validate the local files, when number of files does not match to the storage unit information.
        try
        {
            storageFileHelper.validateDownloadedS3Files(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX, storageUnit);
            fail("Should throw a RuntimeException when number of local files does not match to the storage unit information.");
        }
        catch (RuntimeException e)
        {
            String expectedErrMsg = String
                .format("Number of downloaded files does not match the storage unit information (expected %d files, actual %d files).",
                    storageUnit.getStorageFiles().size(), LOCAL_FILES.size() + 1);
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    @Test
    public void testValidateDownloadedS3FilesFileNoExists() throws IOException
    {
        File targetLocalDirectory = Paths.get(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX).toFile();
        createLocalFile(targetLocalDirectory.getPath(), "ACTUAL_FILE", FILE_SIZE_1_KB);
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, Arrays.asList("EXPECTED_FILE"), FILE_SIZE_1_KB);

        // Try to validate non-existing local files.
        try
        {
            storageFileHelper.validateDownloadedS3Files(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX, storageUnit);
            fail("Should throw a RuntimeException when actual local files do not exist.");
        }
        catch (RuntimeException e)
        {
            String expectedErrMsg = String.format("Downloaded \"%s\" file doesn't exist.",
                Paths.get(LOCAL_TEMP_PATH.toString(), storageUnit.getStorageFiles().get(0).getFilePath()).toFile().getPath());
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    @Test
    public void testValidateDownloadedS3FilesFileSizeMismatch() throws IOException
    {
        File targetLocalDirectory = Paths.get(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX).toFile();
        createLocalFiles(targetLocalDirectory.getPath(), FILE_SIZE_1_KB);
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, LOCAL_FILES, FILE_SIZE_1_KB * 2);

        // Try to validate the local files, when actual file sizes do not not match to the storage unit information.
        try
        {
            storageFileHelper.validateDownloadedS3Files(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX, storageUnit);
            fail("Should throw a RuntimeException when actual file sizes do not not match to the storage unit information.");
        }
        catch (RuntimeException e)
        {
            String expectedErrMsg = String
                .format("Size of the downloaded \"%s\" S3 file does not match the expected value (expected %d bytes, actual %d bytes).",
                    Paths.get(LOCAL_TEMP_PATH.toString(), storageUnit.getStorageFiles().get(0).getFilePath()).toFile().getPath(), FILE_SIZE_1_KB * 2,
                    FILE_SIZE_1_KB);
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    /**
     * Creates test files of the specified size relative to the base directory.
     *
     * @param baseDirectory the local parent directory path, relative to which we want our file to be created
     * @param size the file size in bytes
     */
    private void createLocalFiles(String baseDirectory, long size) throws IOException
    {
        // Create local test files.
        for (String file : LOCAL_FILES)
        {
            createLocalFile(baseDirectory, file, size);
        }
    }

    /**
     * Creates a storage unit with the specified list of files.
     *
     * @param s3KeyPrefix the S3 key prefix.
     * @param files the list of files.
     * @param fileSizeBytes the file size in bytes.
     *
     * @return the storage unit with the list of file paths
     */
    private StorageUnit createStorageUnit(String s3KeyPrefix, List<String> files, Long fileSizeBytes)
    {
        StorageUnit storageUnit = new StorageUnit();

        Storage storage = new Storage();
        storageUnit.setStorage(storage);

        storage.setName("TEST_STORAGE");
        List<StorageFile> storageFiles = new ArrayList<>();
        storageUnit.setStorageFiles(storageFiles);
        if (!CollectionUtils.isEmpty(files))
        {
            for (String file : files)
            {
                StorageFile storageFile = new StorageFile();
                storageFiles.add(storageFile);
                storageFile.setFilePath(String.format("%s/%s", s3KeyPrefix, file));
                storageFile.setFileSizeBytes(fileSizeBytes);
            }
        }
        storageUnit.setStorageUnitStatus(StorageUnitStatusEntity.ENABLED);

        return storageUnit;
    }
}
