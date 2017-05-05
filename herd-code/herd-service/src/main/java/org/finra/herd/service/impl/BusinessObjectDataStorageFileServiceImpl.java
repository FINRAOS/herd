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
package org.finra.herd.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.StorageFileDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.BusinessObjectDataStorageFileService;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageFileDaoHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;

/**
 * Service for business object data storage files.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDataStorageFileServiceImpl implements BusinessObjectDataStorageFileService
{
    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private StorageFileDao storageFileDao;

    @Autowired
    private StorageFileDaoHelper storageFileDaoHelper;

    @Autowired
    private StorageFileHelper storageFileHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StorageUnitDaoHelper storageUnitDaoHelper;

    /**
     * Adds files to Business object data storage.
     *
     * @param businessObjectDataStorageFilesCreateRequest the business object data storage files create request
     *
     * @return BusinessObjectDataStorageFilesCreateResponse
     */
    @NamespacePermission(fields = "#businessObjectDataStorageFilesCreateRequest.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataStorageFilesCreateResponse createBusinessObjectDataStorageFiles(
        BusinessObjectDataStorageFilesCreateRequest businessObjectDataStorageFilesCreateRequest)
    {
        return createBusinessObjectDataStorageFilesImpl(businessObjectDataStorageFilesCreateRequest);
    }

    /**
     * Adds files to Business object data storage.
     *
     * @param businessObjectDataStorageFilesCreateRequest the business object data storage files create request
     *
     * @return BusinessObjectDataStorageFilesCreateResponse
     */
    protected BusinessObjectDataStorageFilesCreateResponse createBusinessObjectDataStorageFilesImpl(
        BusinessObjectDataStorageFilesCreateRequest businessObjectDataStorageFilesCreateRequest)
    {
        // validate request
        validateBusinessObjectDataStorageFilesCreateRequest(businessObjectDataStorageFilesCreateRequest);

        // retrieve and validate that the business object data exists
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(getBusinessObjectDataKey(businessObjectDataStorageFilesCreateRequest));

        // Validate that business object data is in one of the pre-registered states.
        Assert.isTrue(BooleanUtils.isTrue(businessObjectDataEntity.getStatus().getPreRegistrationStatus()), String
            .format("Business object data status must be one of the pre-registration statuses. Business object data status {%s}, business object data {%s}",
                businessObjectDataEntity.getStatus().getCode(), businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));

        // retrieve and validate that the storage unit exists
        StorageUnitEntity storageUnitEntity =
            storageUnitDaoHelper.getStorageUnitEntity(businessObjectDataStorageFilesCreateRequest.getStorageName(), businessObjectDataEntity);

        // Validate the storage unit has an acceptable status for adding new files.
        Assert.isTrue(StorageUnitStatusEntity.ENABLED.equals(storageUnitEntity.getStatus().getCode()), String
            .format("Storage unit must be in the ENABLED status. Storage unit status {%s}, business object data {%s}", storageUnitEntity.getStatus().getCode(),
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));

        StorageEntity storageEntity = storageUnitEntity.getStorage();

        // Get the S3 validation flags.
        boolean validatePathPrefix = storageHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true);
        boolean validateFileExistence = storageHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true);
        boolean validateFileSize = storageHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), storageEntity,
                false, true);

        // Ensure that file size validation is not enabled without file existence validation.
        if (validateFileSize)
        {
            Assert.isTrue(validateFileExistence,
                String.format("Storage \"%s\" has file size validation enabled without file existence validation.", storageEntity.getName()));
        }

        // Process the add storage files request based on the auto-discovery of storage files being enabled or not.
        List<StorageFile> storageFiles;
        if (BooleanUtils.isTrue(businessObjectDataStorageFilesCreateRequest.isDiscoverStorageFiles()))
        {
            // Discover new storage files for this storage unit.
            storageFiles = discoverStorageFiles(storageUnitEntity);
        }
        else
        {
            // Get the list of storage files from the request.
            storageFiles = businessObjectDataStorageFilesCreateRequest.getStorageFiles();

            // Validate storage files.
            validateStorageFiles(storageFiles, storageUnitEntity, validatePathPrefix, validateFileExistence, validateFileSize);
        }

        // Add new storage files to the storage unit.
        storageFileDaoHelper.createStorageFileEntitiesFromStorageFiles(storageUnitEntity, storageFiles);

        // Construct and return the response.
        return createBusinessObjectDataStorageFilesCreateResponse(storageEntity, businessObjectDataEntity, storageFiles);
    }

    /**
     * Discovers new storage files in S3 for the specified storage unit.
     *
     * @param storageUnitEntity the storage unit entity
     *
     * @return the list of discovered storage files
     */
    private List<StorageFile> discoverStorageFiles(StorageUnitEntity storageUnitEntity)
    {
        // Retrieve all storage files already registered for this storage unit loaded in a map for easy access.
        Map<String, StorageFileEntity> storageFileEntities = storageFileHelper.getStorageFileEntitiesMap(storageUnitEntity.getStorageFiles());

        // Validate and get storage directory path from the storage unit.
        Assert.hasText(storageUnitEntity.getDirectoryPath(),
            "Business object data has no storage directory path which is required for auto-discovery of storage files.");
        String directoryPath = storageUnitEntity.getDirectoryPath();

        // Add a trailing slash to the storage directory path if it doesn't already have it.
        String directoryPathWithTrailingSlash = StringUtils.appendIfMissing(directoryPath, "/");

        // Retrieve all already registered storage files from the storage that start with the directory path.
        List<String> registeredStorageFilePaths =
            storageFileDao.getStorageFilesByStorageAndFilePathPrefix(storageUnitEntity.getStorage().getName(), directoryPathWithTrailingSlash);

        // Sanity check already registered storage files.
        if (storageFileEntities.size() != registeredStorageFilePaths.size())
        {
            throw new IllegalArgumentException(String.format(
                "Number of storage files (%d) already registered for the business object data in \"%s\" storage is not equal to " +
                    "the number of registered storage files (%d) matching \"%s\" S3 key prefix in the same storage.", storageFileEntities.size(),
                storageUnitEntity.getStorage().getName(), registeredStorageFilePaths.size(), directoryPathWithTrailingSlash));
        }

        // Get S3 bucket access parameters and set the key prefix to the directory path with a trailing slash.
        // Please note that since we got here, the directory path can not be empty.
        S3FileTransferRequestParamsDto params = storageHelper.getS3BucketAccessParams(storageUnitEntity.getStorage());
        params.setS3KeyPrefix(directoryPathWithTrailingSlash);

        // List S3 files ignoring 0 byte objects that represent S3 directories.
        // Please note that the map implementation returned by the helper method below
        // preserves the original order of files as returned by the S3 list command.
        Map<String, StorageFile> actualS3Keys = storageFileHelper.getStorageFilesMapFromS3ObjectSummaries(s3Service.listDirectory(params, true));

        // For the already registered storage files, validate file existence and file size against S3 keys and metadata reported by S3.
        for (Map.Entry<String, StorageFileEntity> entry : storageFileEntities.entrySet())
        {
            storageFileHelper.validateStorageFileEntity(entry.getValue(), params.getS3BucketName(), actualS3Keys, true);
        }

        // Remove all already registered storage files from the map of actual S3 keys.
        actualS3Keys.keySet().removeAll(storageFileEntities.keySet());

        // Validate that we have at least one unregistered storage file discovered in S3.
        Assert.notEmpty(actualS3Keys.keySet(),
            String.format("No unregistered storage files were discovered at s3://%s/%s location.", params.getS3BucketName(), directoryPathWithTrailingSlash));

        // Build and return a list of storage files.
        return new ArrayList<>(actualS3Keys.values());
    }

    /**
     * Validates a list of storage files to be added to the specified storage unit.
     *
     * @param storageFiles the list of storage files
     * @param storageUnitEntity the storage unit entity
     * @param validatePathPrefix the validate path prefix flag
     * @param validateFileExistence the validate file existence flag
     * @param validateFileSize the validate file size flag
     */
    private void validateStorageFiles(List<StorageFile> storageFiles, StorageUnitEntity storageUnitEntity, boolean validatePathPrefix,
        boolean validateFileExistence, boolean validateFileSize)
    {
        // Retrieve all storage files already registered for this storage unit loaded in a map for easy access.
        Map<String, StorageFileEntity> storageFileEntities = storageFileHelper.getStorageFileEntitiesMap(storageUnitEntity.getStorageFiles());

        // Perform validation of storage files listed in the request per storage directory path and/or validation flags.
        String directoryPath = null;
        String directoryPathWithTrailingSlash = null;
        if (StringUtils.isNotBlank(storageUnitEntity.getDirectoryPath()))
        {
            // Use the storage directory path from the storage unit.
            directoryPath = storageUnitEntity.getDirectoryPath();

            // Add a trailing slash to the storage directory path if it doesn't already have it.
            directoryPathWithTrailingSlash = StringUtils.appendIfMissing(directoryPath, "/");

            // If a storage directory path exists, then validate that all files being added are contained within that directory.
            for (StorageFile storageFile : storageFiles)
            {
                Assert.isTrue(storageFile.getFilePath().startsWith(directoryPathWithTrailingSlash), String
                    .format("Storage file path \"%s\" does not match the storage directory path \"%s\".", storageFile.getFilePath(),
                        directoryPathWithTrailingSlash));
            }
        }
        else if (validatePathPrefix || validateFileExistence)
        {
            // Use the expected S3 key prefix value as the storage directory path.
            directoryPath = s3KeyPrefixHelper
                .buildS3KeyPrefix(storageUnitEntity.getStorage(), storageUnitEntity.getBusinessObjectData().getBusinessObjectFormat(),
                    businessObjectDataHelper.getBusinessObjectDataKey(storageUnitEntity.getBusinessObjectData()));

            // Add a trailing slash to the expected S3 key prefix if it doesn't already have it.
            directoryPathWithTrailingSlash = StringUtils.appendIfMissing(directoryPath, "/");

            // Validate that all files are contained within the expected S3 key prefix.
            for (StorageFile storageFile : storageFiles)
            {
                Assert.isTrue(storageFile.getFilePath().startsWith(directoryPathWithTrailingSlash), String
                    .format("Specified storage file path \"%s\" does not match the expected S3 key prefix \"%s\".", storageFile.getFilePath(),
                        directoryPathWithTrailingSlash));
            }
        }

        // Validate that files in the request does not already exist in the database.
        if (StringUtils.isNotBlank(directoryPath))
        {
            // Get a list of request storage file paths.
            List<String> requestStorageFilePaths = storageFileHelper.getFilePathsFromStorageFiles(storageFiles);

            // Retrieve all already registered storage files from the storage that start with the directory path.
            List<String> registeredStorageFilePaths =
                storageFileDao.getStorageFilesByStorageAndFilePathPrefix(storageUnitEntity.getStorage().getName(), directoryPathWithTrailingSlash);

            // Check if request contains any of the already registered files.
            registeredStorageFilePaths.retainAll(requestStorageFilePaths);
            if (!CollectionUtils.isEmpty(registeredStorageFilePaths))
            {
                // Retrieve the storage file entity for the first "already registered" storage file.
                // Since the discovered storage file path exists in the database, we should not get a null back.
                StorageFileEntity storageFileEntity =
                    storageFileDao.getStorageFileByStorageNameAndFilePath(storageUnitEntity.getStorage().getName(), registeredStorageFilePaths.get(0));

                // Throw an exception reporting the information on the "already registered" storage file.
                throw new AlreadyExistsException(String
                    .format("S3 file \"%s\" in \"%s\" storage is already registered by the business object data {%s}.", registeredStorageFilePaths.get(0),
                        storageUnitEntity.getStorage().getName(),
                        businessObjectDataHelper.businessObjectDataEntityAltKeyToString(storageFileEntity.getStorageUnit().getBusinessObjectData())));
            }
        }
        else
        {
            // Since directory path is not available, we need to validate each storage file specified in the request individually.
            for (StorageFile storageFile : storageFiles)
            {
                // Ensure that the file is not already registered in this storage by some other business object data.
                StorageFileEntity storageFileEntity =
                    storageFileDao.getStorageFileByStorageNameAndFilePath(storageUnitEntity.getStorage().getName(), storageFile.getFilePath());
                if (storageFileEntity != null)
                {
                    throw new AlreadyExistsException(String
                        .format("S3 file \"%s\" in \"%s\" storage is already registered by the business object data {%s}.", storageFile.getFilePath(),
                            storageUnitEntity.getStorage().getName(),
                            businessObjectDataHelper.businessObjectDataEntityAltKeyToString(storageFileEntity.getStorageUnit().getBusinessObjectData())));
                }
            }
        }

        // Validate file existence.
        if (validateFileExistence)
        {
            // Get S3 bucket access parameters and set the key prefix to the directory path with a trailing slash.
            // Please note that since we got here, the directory path can not be empty.
            S3FileTransferRequestParamsDto params = storageHelper.getS3BucketAccessParams(storageUnitEntity.getStorage());
            params.setS3KeyPrefix(directoryPathWithTrailingSlash);

            // When listing S3 files, we ignore 0 byte objects that represent S3 directories.
            Map<String, StorageFile> actualS3Keys = storageFileHelper.getStorageFilesMapFromS3ObjectSummaries(s3Service.listDirectory(params, true));

            // For the already registered storage files, validate each storage file against S3 keys and metadata reported by S3.
            for (Map.Entry<String, StorageFileEntity> entry : storageFileEntities.entrySet())
            {
                storageFileHelper.validateStorageFileEntity(entry.getValue(), params.getS3BucketName(), actualS3Keys, validateFileSize);
            }

            // Validate each storage file listed in the request.
            for (StorageFile storageFile : storageFiles)
            {
                storageFileHelper.validateStorageFile(storageFile, params.getS3BucketName(), actualS3Keys, validateFileSize);
            }
        }
    }

    /**
     * Validates the given request without using any external dependencies (ex. DB). Throws appropriate exceptions when a validation error exists.
     *
     * @param businessObjectDataStorageFilesCreateRequest - request to validate
     */
    private void validateBusinessObjectDataStorageFilesCreateRequest(BusinessObjectDataStorageFilesCreateRequest businessObjectDataStorageFilesCreateRequest)
    {
        Assert.hasText(businessObjectDataStorageFilesCreateRequest.getNamespace(), "A namespace must be specified.");
        businessObjectDataStorageFilesCreateRequest.setNamespace(businessObjectDataStorageFilesCreateRequest.getNamespace().trim());

        Assert.hasText(businessObjectDataStorageFilesCreateRequest.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        businessObjectDataStorageFilesCreateRequest
            .setBusinessObjectDefinitionName(businessObjectDataStorageFilesCreateRequest.getBusinessObjectDefinitionName().trim());

        Assert.hasText(businessObjectDataStorageFilesCreateRequest.getBusinessObjectFormatUsage(), "A business object format usage must be specified.");
        businessObjectDataStorageFilesCreateRequest
            .setBusinessObjectFormatUsage(businessObjectDataStorageFilesCreateRequest.getBusinessObjectFormatUsage().trim());

        Assert.hasText(businessObjectDataStorageFilesCreateRequest.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        businessObjectDataStorageFilesCreateRequest
            .setBusinessObjectFormatFileType(businessObjectDataStorageFilesCreateRequest.getBusinessObjectFormatFileType().trim());

        Assert.notNull(businessObjectDataStorageFilesCreateRequest.getBusinessObjectFormatVersion(), "A business object format version must be specified.");

        Assert.hasText(businessObjectDataStorageFilesCreateRequest.getPartitionValue(), "A partition value must be specified.");
        businessObjectDataStorageFilesCreateRequest.setPartitionValue(businessObjectDataStorageFilesCreateRequest.getPartitionValue().trim());

        int subPartitionValuesCount = CollectionUtils.size(businessObjectDataStorageFilesCreateRequest.getSubPartitionValues());
        Assert.isTrue(subPartitionValuesCount <= BusinessObjectDataEntity.MAX_SUBPARTITIONS,
            String.format("Exceeded maximum number of allowed subpartitions: %d.", BusinessObjectDataEntity.MAX_SUBPARTITIONS));

        for (int i = 0; i < subPartitionValuesCount; i++)
        {
            Assert.hasText(businessObjectDataStorageFilesCreateRequest.getSubPartitionValues().get(i), "A subpartition value must be specified.");
            businessObjectDataStorageFilesCreateRequest.getSubPartitionValues()
                .set(i, businessObjectDataStorageFilesCreateRequest.getSubPartitionValues().get(i).trim());
        }

        Assert.notNull(businessObjectDataStorageFilesCreateRequest.getBusinessObjectDataVersion(), "A business object data version must be specified.");

        Assert.hasText(businessObjectDataStorageFilesCreateRequest.getStorageName(), "A storage name must be specified.");
        businessObjectDataStorageFilesCreateRequest.setStorageName(businessObjectDataStorageFilesCreateRequest.getStorageName().trim());

        if (BooleanUtils.isTrue(businessObjectDataStorageFilesCreateRequest.isDiscoverStorageFiles()))
        {
            // The auto-discovery of storage files is enabled, thus storage files can not be specified.
            Assert.isTrue(CollectionUtils.isEmpty(businessObjectDataStorageFilesCreateRequest.getStorageFiles()),
                "Storage files cannot be specified when discovery of storage files is enabled.");
        }
        else
        {
            // Since auto-discovery is disabled, at least one storage file must be specified.
            Assert.notEmpty(businessObjectDataStorageFilesCreateRequest.getStorageFiles(),
                "At least one storage file must be specified when discovery of storage files is not enabled.");

            // Validate each storage file in the request.
            Set<String> storageFilePathValidationSet = new HashSet<>();
            for (StorageFile storageFile : businessObjectDataStorageFilesCreateRequest.getStorageFiles())
            {
                Assert.hasText(storageFile.getFilePath(), "A file path must be specified.");
                storageFile.setFilePath(storageFile.getFilePath().trim());
                Assert.notNull(storageFile.getFileSizeBytes(), "A file size must be specified.");

                // Ensure row count is positive.
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
    }

    /**
     * Gets a business object data key from a specified business object data storage files create request.
     *
     * @param request the business object data storage files create request
     *
     * @return the business object data key
     */
    private BusinessObjectDataKey getBusinessObjectDataKey(BusinessObjectDataStorageFilesCreateRequest request)
    {
        return new BusinessObjectDataKey(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
            request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion(), request.getPartitionValue(), request.getSubPartitionValues(),
            request.getBusinessObjectDataVersion());
    }

    /**
     * Creates and populates the business object data storage files create response.
     *
     * @param storageEntity the storage entity
     * @param businessObjectDataEntity the business object data entity
     * @param storageFiles the list of storage files
     *
     * @return the business object data storage files create response
     */
    private BusinessObjectDataStorageFilesCreateResponse createBusinessObjectDataStorageFilesCreateResponse(StorageEntity storageEntity,
        BusinessObjectDataEntity businessObjectDataEntity, List<StorageFile> storageFiles)
    {
        BusinessObjectDataStorageFilesCreateResponse response = new BusinessObjectDataStorageFilesCreateResponse();

        response.setNamespace(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode());
        response.setBusinessObjectDefinitionName(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName());
        response.setBusinessObjectFormatUsage(businessObjectDataEntity.getBusinessObjectFormat().getUsage());
        response.setBusinessObjectFormatFileType(businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode());
        response.setBusinessObjectFormatVersion(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion());
        response.setPartitionValue(businessObjectDataEntity.getPartitionValue());
        response.setSubPartitionValues(businessObjectDataHelper.getSubPartitionValues(businessObjectDataEntity));
        response.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());
        response.setStorageName(storageEntity.getName());
        response.setStorageFiles(storageFiles);

        return response;
    }
}
