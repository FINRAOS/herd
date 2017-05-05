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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageFileEntity;

/**
 * A helper class for StorageFile related code.
 */
@Component
public class StorageFileHelper
{
    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

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
     * Validates copied S3 files per list of expected storage files.
     *
     * @param expectedStorageFiles the list of expected S3 files represented by storage files
     * @param actualS3Files the list of actual S3 files represented by S3 object summaries
     * @param storageName the storage name
     * @param businessObjectDataKey the business object data key
     */
    public void validateCopiedS3Files(List<StorageFile> expectedStorageFiles, List<S3ObjectSummary> actualS3Files, String storageName,
        BusinessObjectDataKey businessObjectDataKey)
    {
        validateS3Files(expectedStorageFiles, actualS3Files, storageName, businessObjectDataKey, "copied");
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
     * Validates registered S3 files per list of expected storage files.
     *
     * @param expectedStorageFiles the list of expected S3 files represented by storage files
     * @param actualS3Files the list of actual S3 files represented by S3 object summaries
     * @param storageName the storage name
     * @param businessObjectDataKey the business object data key
     */
    public void validateRegisteredS3Files(List<StorageFile> expectedStorageFiles, List<S3ObjectSummary> actualS3Files, String storageName,
        BusinessObjectDataKey businessObjectDataKey)
    {
        validateS3Files(expectedStorageFiles, actualS3Files, storageName, businessObjectDataKey, "registered");
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
     * Validates a list of storage files registered with specified business object data at specified storage. This method makes sure that all storage files
     * match the expected s3 key prefix value.
     *
     * @param storageFilePaths the storage file paths to be validated
     * @param s3KeyPrefix the S3 key prefix that storage file paths are expected to start with
     * @param businessObjectDataEntity the business object data entity
     * @param storageName the name of the storage that storage files are stored in
     *
     * @throws IllegalArgumentException if a storage file doesn't match the expected S3 key prefix.
     */
    public void validateStorageFiles(Collection<String> storageFilePaths, String s3KeyPrefix, BusinessObjectDataEntity businessObjectDataEntity,
        String storageName) throws IllegalArgumentException
    {
        for (String storageFilePath : storageFilePaths)
        {
            Assert.isTrue(storageFilePath.startsWith(s3KeyPrefix), String
                .format("Storage file \"%s\" registered with business object data {%s} in \"%s\" storage does not match the expected S3 key prefix \"%s\".",
                    storageFilePath, businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity), storageName, s3KeyPrefix));
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

    /**
     * Validates S3 files per list of expected storage files.
     *
     * @param expectedStorageFiles the list of expected S3 files represented by storage files
     * @param actualS3Files the list of actual S3 files represented by S3 object summaries
     * @param storageName the storage name
     * @param businessObjectDataKey the business object data key
     * @param fileDescription the file description (i.e. "registered" or "copied") to be used in the relative error messages
     */
    private void validateS3Files(List<StorageFile> expectedStorageFiles, List<S3ObjectSummary> actualS3Files, String storageName,
        BusinessObjectDataKey businessObjectDataKey, String fileDescription)
    {
        // Load all actual S3 files into a map for easy access.
        Map<String, StorageFile> actualFilesMap = getStorageFilesMapFromS3ObjectSummaries(actualS3Files);

        // Validate existence and file size for all expected files.
        for (StorageFile expectedFile : expectedStorageFiles)
        {
            if (!actualFilesMap.containsKey(expectedFile.getFilePath()))
            {
                throw new ObjectNotFoundException(String
                    .format("%s file \"%s\" does not exist in \"%s\" storage.", StringUtils.capitalize(fileDescription), expectedFile.getFilePath(),
                        storageName));
            }
            else
            {
                // Validate the file size.
                StorageFile actualFile = actualFilesMap.get(expectedFile.getFilePath());
                if (!Objects.equals(actualFile.getFileSizeBytes(), expectedFile.getFileSizeBytes()))
                {
                    throw new IllegalStateException(String
                        .format("Specified file size of %d bytes for %s \"%s\" S3 file in \"%s\" storage does not match file size of %d bytes reported by S3.",
                            expectedFile.getFileSizeBytes(), fileDescription, expectedFile.getFilePath(), storageName, actualFile.getFileSizeBytes()));
                }
            }
        }

        // Get a list of actual S3 file paths.
        List<String> actualFilePaths = new ArrayList<>(actualFilesMap.keySet());

        // Get a list of expected file paths.
        List<String> expectedFilePaths = getFilePathsFromStorageFiles(expectedStorageFiles);

        // Validate that no other files in S3 bucket except for expected files have the same S3 key prefix.
        if (!expectedFilePaths.containsAll(actualFilePaths))
        {
            actualFilePaths.removeAll(expectedFilePaths);
            throw new IllegalStateException(String
                .format("Found unexpected S3 file \"%s\" in \"%s\" storage while validating %s S3 files. Business object data {%s}", actualFilePaths.get(0),
                    storageName, fileDescription, businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }
    }
}
