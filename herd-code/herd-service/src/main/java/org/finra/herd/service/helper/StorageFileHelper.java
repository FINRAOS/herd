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

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;

/**
 * A helper class for StorageFile related code.
 */
@Component
public class StorageFileHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageFileHelper.class);

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private JsonHelper jsonHelper;

    /**
     * Creates a storage file from the storage file entity.
     *
     * @param storageFileEntity the storage file entity
     *
     * @return the storage file
     */
    public StorageFile createStorageFileFromEntity(StorageFileEntity storageFileEntity)
    {
        StorageFile storageFile = new StorageFile();

        storageFile.setFilePath(storageFileEntity.getPath());
        storageFile.setFileSizeBytes(storageFileEntity.getFileSizeBytes());
        storageFile.setRowCount(storageFileEntity.getRowCount());

        return storageFile;
    }

    /**
     * Creates a list of storage files from the collection of storage file entities.
     *
     * @param storageFileEntities the collection of storage file entities
     *
     * @return the list of storage files
     */
    public List<StorageFile> createStorageFilesFromEntities(Collection<StorageFileEntity> storageFileEntities)
    {
        List<StorageFile> storageFiles = new ArrayList<>();

        for (StorageFileEntity storageFileEntity : storageFileEntities)
        {
            storageFiles.add(createStorageFileFromEntity(storageFileEntity));
        }

        return storageFiles;
    }

    /**
     * Creates a list of storage files from the list of S3 object summaries.
     *
     * @param s3ObjectSummaries the list of S3 object summaries
     *
     * @return the list of storage files
     */
    public List<StorageFile> createStorageFilesFromS3ObjectSummaries(List<S3ObjectSummary> s3ObjectSummaries)
    {
        List<StorageFile> storageFiles = new ArrayList<>();

        for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries)
        {
            storageFiles.add(new StorageFile(s3ObjectSummary.getKey(), s3ObjectSummary.getSize(), null));
        }

        return storageFiles;
    }

    /**
     * Retrieves and validates a list of storage files registered with the specified storage unit. This method makes sure that the list of storage files is not
     * empty and that all storage files match the expected s3 key prefix value.
     *
     * @param storageUnitEntity the storage unit entity the storage file paths to be validated
     * @param s3KeyPrefix the S3 key prefix that storage file paths are expected to start with
     * @param storageName the storage name
     * @param businessObjectDataKey the business object data key
     *
     * @return the list of storage files
     */
    public List<StorageFile> getAndValidateStorageFiles(StorageUnitEntity storageUnitEntity, String s3KeyPrefix, String storageName,
        BusinessObjectDataKey businessObjectDataKey)
    {
        // Check if the list of storage files is not empty.
        if (CollectionUtils.isEmpty(storageUnitEntity.getStorageFiles()))
        {
            throw new IllegalArgumentException(String
                .format("Business object data has no storage files registered in \"%s\" storage. Business object data: {%s}", storageName,
                    businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }

        // Retrieve storage files.
        List<StorageFile> storageFiles = createStorageFilesFromEntities(storageUnitEntity.getStorageFiles());

        // Validate storage file paths registered with this business object data in the specified storage.
        validateStorageFilePaths(getFilePathsFromStorageFiles(storageFiles), s3KeyPrefix, businessObjectDataKey, storageName);

        // Return the list of storage files.
        return storageFiles;
    }

    /**
     * Retrieves and validates a list of storage files registered with the specified storage unit. This method returns an empty list if storage unit has no
     * storage files.
     *
     * @param storageUnitEntity the storage unit entity the storage file paths to be validated
     * @param s3KeyPrefix the S3 key prefix that storage file paths are expected to start with
     * @param storageName the storage name
     * @param businessObjectDataKey the business object data key
     *
     * @return the list of storage files
     */
    public List<StorageFile> getAndValidateStorageFilesIfPresent(StorageUnitEntity storageUnitEntity, String s3KeyPrefix, String storageName,
        BusinessObjectDataKey businessObjectDataKey)
    {
        return CollectionUtils.isEmpty(storageUnitEntity.getStorageFiles()) ? new ArrayList<>() :
            getAndValidateStorageFiles(storageUnitEntity, s3KeyPrefix, storageName, businessObjectDataKey);
    }

    /**
     * Returns a list of file paths extracted from the specified list of S3 object summaries.
     *
     * @param s3ObjectSummaries the list of of S3 object summaries
     *
     * @return the list of file paths
     */
    public List<String> getFilePathsFromS3ObjectSummaries(List<S3ObjectSummary> s3ObjectSummaries)
    {
        List<String> filePaths = new ArrayList<>();

        for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries)
        {
            filePaths.add(s3ObjectSummary.getKey());
        }

        return filePaths;
    }

    /**
     * Returns a list of file paths extracted from the specified list of storage files.
     *
     * @param storageFiles the list of storage files
     *
     * @return the list of file paths
     */
    public List<String> getFilePathsFromStorageFiles(List<StorageFile> storageFiles)
    {
        List<String> filePaths = new ArrayList<>();

        for (StorageFile storageFile : storageFiles)
        {
            filePaths.add(storageFile.getFilePath());
        }

        return filePaths;
    }

    /**
     * Returns a list of files extracted from the specified list of storage files.
     *
     * @param storageFiles the list of storage files
     *
     * @return the list of files
     */
    public List<File> getFiles(List<StorageFile> storageFiles)
    {
        List<File> files = new ArrayList<>();

        for (StorageFile storageFile : storageFiles)
        {
            files.add(new File(storageFile.getFilePath()));
        }

        return files;
    }

    /**
     * Returns a map of file paths to the storage file entities build from the list of storage file entities.
     *
     * @param storageFileEntities the collection of storage file entities
     *
     * @return the map of file paths to storage file entities
     */
    public Map<String, StorageFileEntity> getStorageFileEntitiesMap(Collection<StorageFileEntity> storageFileEntities)
    {
        Map<String, StorageFileEntity> result = new HashMap<>();

        for (StorageFileEntity storageFileEntity : storageFileEntities)
        {
            result.put(storageFileEntity.getPath(), storageFileEntity);
        }

        return result;
    }

    /**
     * Returns a map of file paths to the storage files build from the list of S3 object summaries with map iteration order matching the original list order.
     *
     * @param s3ObjectSummaries the list of S3 object summaries
     *
     * @return the map of file paths to storage files
     */
    public Map<String, StorageFile> getStorageFilesMapFromS3ObjectSummaries(List<S3ObjectSummary> s3ObjectSummaries)
    {
        Map<String, StorageFile> result = new LinkedHashMap<>();

        for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries)
        {
            result.put(s3ObjectSummary.getKey(), new StorageFile(s3ObjectSummary.getKey(), s3ObjectSummary.getSize(), null));
        }

        return result;
    }

    /**
     * Validates a list of storage files. This will validate and trim appropriate fields.
     *
     * @param storageFiles the list of storage files
     */
    public void validateCreateRequestStorageFiles(List<StorageFile> storageFiles)
    {
        Set<String> storageFilePathValidationSet = new HashSet<>();

        for (StorageFile storageFile : storageFiles)
        {
            Assert.hasText(storageFile.getFilePath(), "A file path must be specified.");
            storageFile.setFilePath(storageFile.getFilePath().trim());

            Assert.notNull(storageFile.getFileSizeBytes(), "A file size must be specified.");

            // Ensure row count is not negative.
            if (storageFile.getRowCount() != null)
            {
                Assert.isTrue(storageFile.getRowCount() >= 0, "File \"" + storageFile.getFilePath() + "\" has a row count which is < 0.");
            }

            // Check for duplicates.
            if (storageFilePathValidationSet.contains(storageFile.getFilePath()))
            {
                throw new IllegalArgumentException(String.format("Duplicate storage file found: %s", storageFile.getFilePath()));
            }

            storageFilePathValidationSet.add(storageFile.getFilePath());
        }
    }

    /**
     * Validate downloaded S3 files per storage unit information.
     *
     * @param baseDirectory the local parent directory path, relative to which the files are expected to be located
     * @param s3KeyPrefix the S3 key prefix that was prepended to the S3 file paths, when they were uploaded to S3
     * @param storageUnit the storage unit that contains a list of storage files to be validated
     *
     * @throws IllegalStateException if files are not valid
     */
    public void validateDownloadedS3Files(String baseDirectory, String s3KeyPrefix, StorageUnit storageUnit) throws IllegalStateException
    {
        validateDownloadedS3Files(baseDirectory, s3KeyPrefix, storageUnit.getStorageFiles());
    }

    /**
     * Validate downloaded S3 files per specified list of storage files.
     *
     * @param baseDirectory the local parent directory path, relative to which the files are expected to be located
     * @param s3KeyPrefix the S3 key prefix that was prepended to the S3 file paths, when they were uploaded to S3
     * @param storageFiles the list of storage files
     *
     * @throws IllegalStateException if files are not valid
     */
    public void validateDownloadedS3Files(String baseDirectory, String s3KeyPrefix, List<StorageFile> storageFiles) throws IllegalStateException
    {
        // Build a target local directory path, which is the parent directory plus the S3 key prefix.
        File targetLocalDirectory = Paths.get(baseDirectory, s3KeyPrefix).toFile();

        // Get a list of all files within the target local directory and its subdirectories.
        Collection<File> actualLocalFiles = FileUtils.listFiles(targetLocalDirectory, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);

        // Validate the total file count.
        int storageFilesCount = CollectionUtils.isEmpty(storageFiles) ? 0 : storageFiles.size();
        if (storageFilesCount != actualLocalFiles.size())
        {
            throw new IllegalStateException(String
                .format("Number of downloaded files does not match the storage unit information (expected %d files, actual %d files).", storageFiles.size(),
                    actualLocalFiles.size()));
        }

        // Validate each downloaded file.
        if (storageFilesCount > 0)
        {
            for (StorageFile storageFile : storageFiles)
            {
                // Create a "real file" that points to the actual file on the file system.
                File localFile = Paths.get(baseDirectory, storageFile.getFilePath()).toFile();

                // Verify that the file exists.
                if (!localFile.isFile())
                {
                    throw new IllegalStateException(String.format("Downloaded \"%s\" file doesn't exist.", localFile));
                }

                // Validate the file size.
                if (localFile.length() != storageFile.getFileSizeBytes())
                {
                    throw new IllegalStateException(String
                        .format("Size of the downloaded \"%s\" S3 file does not match the expected value (expected %d bytes, actual %d bytes).",
                            localFile.getPath(), storageFile.getFileSizeBytes(), localFile.length()));
                }
            }
        }
    }

    /**
     * Validates registered S3 files per list of expected storage files. The validation ignores (does not fail) when detecting unregistered zero byte S3 files.
     *
     * @param expectedStorageFiles the list of expected S3 files represented by storage files
     * @param s3ObjectSummaries the list of actual S3 files represented by S3 object summaries
     * @param storageName the storage name
     * @param businessObjectDataKey the business object data key
     */
    public void validateRegisteredS3Files(List<StorageFile> expectedStorageFiles, List<S3ObjectSummary> s3ObjectSummaries, String storageName,
        BusinessObjectDataKey businessObjectDataKey)
    {
        // Get a set of actual S3 file paths.
        Set<String> actualS3FilePaths = new HashSet<>(getFilePathsFromS3ObjectSummaries(s3ObjectSummaries));

        // Validate existence and file size for all expected files.
        for (StorageFile expectedStorageFile : expectedStorageFiles)
        {
            if (!actualS3FilePaths.contains(expectedStorageFile.getFilePath()))
            {
                throw new ObjectNotFoundException(
                    String.format("Registered file \"%s\" does not exist in \"%s\" storage.", expectedStorageFile.getFilePath(), storageName));
            }
        }

        // Get a set of expected file paths.
        Set<String> expectedFilePaths = new HashSet<>(getFilePathsFromStorageFiles(expectedStorageFiles));

        // Create a JSON representation of the business object data key.
        String businessObjectDataKeyAsJson = jsonHelper.objectToJson(businessObjectDataKey);

        // Validate that no other files in S3 bucket except for expected storage files have the same S3 key prefix.
        // Please note that this validation ignores (does not fail on) any unregistered zero byte S3 files.
        for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries)
        {
            if (!expectedFilePaths.contains(s3ObjectSummary.getKey()))
            {
                // Ignore unregistered zero byte S3 files.
                if (s3ObjectSummary.getSize() == 0)
                {
                    LOGGER.info("Ignoring unregistered zero byte S3 file. s3Key=\"{}\" storageName=\"{}\" businessObjectDataKey={}", s3ObjectSummary.getKey(),
                        storageName, businessObjectDataKeyAsJson);
                }
                else
                {
                    throw new IllegalStateException(String
                        .format("Found unregistered non-empty S3 file \"%s\" in \"%s\" storage. Business object data {%s}", s3ObjectSummary.getKey(),
                            storageName, businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
                }
            }
        }
    }

    /**
     * Validates storage file against the actual S3 objects reported by S3.
     *
     * @param storageFile the storage file to be validated
     * @param s3BucketName the S3 bucket name
     * @param actualS3Keys the map of storage file paths to storage files as reported by S3
     * @param validateFileSize specifies whether file size validation is required or not
     */
    public void validateStorageFile(StorageFile storageFile, String s3BucketName, Map<String, StorageFile> actualS3Keys, boolean validateFileSize)
    {
        if (!actualS3Keys.containsKey(storageFile.getFilePath()))
        {
            throw new ObjectNotFoundException(String.format("File not found at s3://%s/%s location.", s3BucketName, storageFile.getFilePath()));
        }
        else if (validateFileSize)
        {
            // Validate the file size.
            StorageFile actualS3StorageFile = actualS3Keys.get(storageFile.getFilePath());
            Assert.isTrue(storageFile.getFileSizeBytes().equals(actualS3StorageFile.getFileSizeBytes()), String
                .format("Specified file size of %d bytes for \"%s\" storage file does not match file size of %d bytes reported by S3.",
                    storageFile.getFileSizeBytes(), storageFile.getFilePath(), actualS3StorageFile.getFileSizeBytes()));
        }
    }

    /**
     * Validates storage file entity against the actual S3 objects reported by S3.
     *
     * @param storageFileEntity the storage file to be validated
     * @param s3BucketName the S3 bucket name
     * @param actualS3Keys the map of storage file paths to storage files as reported by S3
     * @param validateFileSize specifies whether file size validation is required or not
     */
    public void validateStorageFileEntity(StorageFileEntity storageFileEntity, String s3BucketName, Map<String, StorageFile> actualS3Keys,
        boolean validateFileSize)
    {
        if (!actualS3Keys.containsKey(storageFileEntity.getPath()))
        {
            throw new ObjectNotFoundException(
                String.format("Previously registered storage file not found at s3://%s/%s location.", s3BucketName, storageFileEntity.getPath()));
        }
        else if (validateFileSize)
        {
            // Validate the file size.
            StorageFile actualS3StorageFile = actualS3Keys.get(storageFileEntity.getPath());
            Assert.isTrue(storageFileEntity.getFileSizeBytes() != null,
                String.format("Previously registered storage file \"%s\" has no file size specified.", storageFileEntity.getPath()));
            Assert.isTrue(storageFileEntity.getFileSizeBytes().equals(actualS3StorageFile.getFileSizeBytes()), String
                .format("Previously registered storage file \"%s\" has file size of %d bytes that does not match file size of %d bytes reported by S3.",
                    storageFileEntity.getPath(), storageFileEntity.getFileSizeBytes(), actualS3StorageFile.getFileSizeBytes()));
        }
    }

    /**
     * Validates a list of storage file paths. This method makes sure that all storage file paths match the expected s3 key prefix value.
     *
     * @param storageFilePaths the storage file paths to be validated
     * @param s3KeyPrefix the S3 key prefix that storage file paths are expected to start with
     * @param businessObjectDataKey the business object data key
     * @param storageName the name of the storage that storage files are stored in
     */
    public void validateStorageFilePaths(Collection<String> storageFilePaths, String s3KeyPrefix, BusinessObjectDataKey businessObjectDataKey,
        String storageName)
    {
        for (String storageFilePath : storageFilePaths)
        {
            Assert.isTrue(storageFilePath.startsWith(s3KeyPrefix), String
                .format("Storage file \"%s\" registered with business object data {%s} in \"%s\" storage does not match the expected S3 key prefix \"%s\".",
                    storageFilePath, businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey), storageName, s3KeyPrefix));
        }
    }

    /**
     * Validates S3 files per storage unit information.
     *
     * @param storageUnit the storage unit that contains S3 files to be validated
     * @param actualS3Files the list of the actual S3 files
     * @param s3KeyPrefix the S3 key prefix that was prepended to the S3 file paths, when they were uploaded to S3
     */
    public void validateStorageUnitS3Files(StorageUnit storageUnit, List<String> actualS3Files, String s3KeyPrefix)
    {
        // Validate that all files match the expected S3 key prefix and build a list of registered S3 files.
        List<String> registeredS3Files = new ArrayList<>();
        if (!CollectionUtils.isEmpty(storageUnit.getStorageFiles()))
        {
            for (StorageFile storageFile : storageUnit.getStorageFiles())
            {
                Assert.isTrue(storageFile.getFilePath().startsWith(s3KeyPrefix), String
                    .format("Storage file S3 key prefix \"%s\" does not match the expected S3 key prefix \"%s\".", storageFile.getFilePath(), s3KeyPrefix));
                registeredS3Files.add(storageFile.getFilePath());
            }
        }

        // Validate that all files exist in S3 managed bucket.
        if (!actualS3Files.containsAll(registeredS3Files))
        {
            registeredS3Files.removeAll(actualS3Files);
            throw new IllegalStateException(
                String.format("Registered file \"%s\" does not exist in \"%s\" storage.", registeredS3Files.get(0), storageUnit.getStorage().getName()));
        }

        // Validate that no other files in S3 managed bucket have the same S3 key prefix.
        if (!registeredS3Files.containsAll(actualS3Files))
        {
            actualS3Files.removeAll(registeredS3Files);
            throw new IllegalStateException(String
                .format("Found S3 file \"%s\" in \"%s\" storage not registered with this business object data.", actualS3Files.get(0),
                    storageUnit.getStorage().getName()));
        }
    }
}
