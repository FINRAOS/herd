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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.HerdFileUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.GlacierArchiveTransferRequestParamsDto;
import org.finra.herd.model.dto.GlacierArchiveTransferResultsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.dto.StoragePolicyTransitionParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.GlacierService;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.StoragePolicyProcessorHelperService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.HerdDaoHelper;
import org.finra.herd.service.helper.HerdHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StoragePolicyHelper;
import org.finra.herd.service.helper.StorageUnitHelper;
import org.finra.herd.service.helper.TarHelper;

/**
 * An implementation of the helper service class for the storage policy processor service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class StoragePolicyProcessorHelperServiceImpl implements StoragePolicyProcessorHelperService
{
    private static final Logger LOGGER = Logger.getLogger(StoragePolicyProcessorHelperServiceImpl.class);

    @Autowired
    protected HerdHelper herdHelper;

    @Autowired
    protected HerdDao herdDao;

    @Autowired
    protected HerdDaoHelper herdDaoHelper;

    @Autowired
    protected StorageDaoHelper storageDaoHelper;

    @Autowired
    protected ConfigurationHelper configurationHelper;

    @Autowired
    protected StoragePolicyHelper storagePolicyHelper;

    @Autowired
    protected BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    protected StorageUnitHelper storageUnitHelper;

    @Autowired
    protected StorageFileHelper storageFileHelper;

    @Autowired
    protected AwsHelper awsHelper;

    @Autowired
    protected S3Service s3Service;

    @Autowired
    protected GlacierService glacierService;

    @Autowired
    protected TarHelper tarHelper;

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public StoragePolicyTransitionParamsDto initiateStoragePolicyTransition(StoragePolicySelection storagePolicySelection)
    {
        return initiateStoragePolicyTransitionImpl(storagePolicySelection);
    }

    /**
     * Initiates a storage policy transition as per specified storage policy selection.
     *
     * @param storagePolicySelection the storage policy selection message
     *
     * @return the storage policy transition DTO that contains parameters needed to perform a storage policy transition
     */
    protected StoragePolicyTransitionParamsDto initiateStoragePolicyTransitionImpl(StoragePolicySelection storagePolicySelection)
    {
        // Validate and trim the storage policy selection message content.
        validateStoragePolicySelection(storagePolicySelection);

        // Get the business object data and storage policy keys.
        BusinessObjectDataKey businessObjectDataKey = storagePolicySelection.getBusinessObjectDataKey();
        StoragePolicyKey storagePolicyKey = storagePolicySelection.getStoragePolicyKey();

        // Retrieve the business object data entity and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = herdDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Validate the business object data.
        validateBusinessObjectData(businessObjectDataEntity, businessObjectDataKey);

        // Retrieve the storage policy and ensure it exists.
        StoragePolicyEntity storagePolicyEntity = storageDaoHelper.getStoragePolicyEntity(storagePolicyKey);

        // Get the source storage name.
        String sourceStorageName = storagePolicyEntity.getStorage().getName();

        // Validate the source storage.
        validateSourceStorage(storagePolicyEntity.getStorage(), storagePolicyKey);

        // Validate that storage policy filter storage has S3 bucket name configured.
        // Please note that since S3 bucket name attribute value is required we pass a "true" flag.
        String sourceBucketName = storageDaoHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storagePolicyEntity.getStorage(),
                true);

        // Get the destination storage name.
        String destinationStorageName = storagePolicyEntity.getDestinationStorage().getName();

        // Validate the destination storage.
        validateDestinationStorage(storagePolicyEntity.getDestinationStorage(), storagePolicyKey);

        // Validate that storage policy transition destination storage has Glacier vault name configured.
        // Please note that since Glacier vault name attribute value is required we pass a "true" flag.
        String destinationVaultName = storageDaoHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.GLACIER_ATTRIBUTE_NAME_VAULT_NAME),
                storagePolicyEntity.getDestinationStorage(), true);

        // Retrieve the source storage unit and ensure it exists.
        StorageUnitEntity sourceStorageUnitEntity = storageDaoHelper.getStorageUnitEntity(businessObjectDataEntity, sourceStorageName);

        // Validate the source storage unit.
        validateSourceStorageUnit(sourceStorageUnitEntity, sourceStorageName, businessObjectDataKey);

        // Try to retrieve the destination storage unit.
        StorageUnitEntity destinationStorageUnitEntity =
            herdDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, destinationStorageName);

        // Validate the destination storage unit, if one exists.
        validateDestinationStorageUnit(destinationStorageUnitEntity, destinationStorageName, businessObjectDataKey);

        // Get business object data key and S3 key prefix for this business object data.
        String s3KeyPrefix =
            businessObjectDataHelper.buildS3KeyPrefix(sourceStorageUnitEntity.getBusinessObjectData().getBusinessObjectFormat(), businessObjectDataKey);

        // Retrieve the storage files registered with this business object data in the specified storage.
        List<StorageFile> storageFiles = new ArrayList<>();
        long storageFilesSizeBytes = 0;
        for (StorageFileEntity storageFileEntity : sourceStorageUnitEntity.getStorageFiles())
        {
            storageFiles.add(storageFileHelper.createStorageFileFromEntity(storageFileEntity));
            storageFilesSizeBytes += storageFileEntity.getFileSizeBytes();
        }

        // Validate that we have storage files registered in the source storage.
        Assert.isTrue(!CollectionUtils.isEmpty(storageFiles), String
            .format("Business object data has no storage files registered in \"%s\" storage. Business object data: {%s}", sourceStorageName,
                herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Validate that the total size of storage files for the business object data is not greater than the threshold value configured in the system.
        validateTotalStorageFilesSize(sourceStorageName, businessObjectDataKey, storageFilesSizeBytes);

        // Validate storage file paths registered with this business object data in the specified storage.
        storageDaoHelper.validateStorageFiles(storageFileHelper.getFilePaths(storageFiles), s3KeyPrefix, sourceStorageUnitEntity.getBusinessObjectData(),
            sourceStorageUnitEntity.getStorage().getName());

        // Validate that this storage does not have any other registered storage files that
        // start with the S3 key prefix, but belong to other business object data instances.
        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        String s3KeyPrefixWithTrailingSlash = s3KeyPrefix + "/";
        Long registeredStorageFileCount = herdDao.getStorageFileCount(sourceStorageName, s3KeyPrefixWithTrailingSlash);
        if (registeredStorageFileCount != storageFiles.size())
        {
            throw new IllegalStateException(String
                .format("Found %d registered storage file(s) matching business object data S3 key prefix in the storage that is not equal to the number " +
                    "of storage files (%d) registered with the business object data in that storage. " +
                    "Storage: {%s}, s3KeyPrefix {%s}, business object data: {%s}", registeredStorageFileCount, storageFiles.size(), sourceStorageName,
                    s3KeyPrefix, herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }

        // Retrieve and ensure the ARCHIVING storage unit status entity exists.
        StorageUnitStatusEntity storageUnitStatusEntity = storageDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.ARCHIVING);

        // If not exists, create the destination storage unit with the ARCHIVING status. Otherwise, update the already existing storage unit.
        if (destinationStorageUnitEntity == null)
        {
            // Create the destination storage unit.
            destinationStorageUnitEntity = new StorageUnitEntity();
            businessObjectDataEntity.getStorageUnits().add(destinationStorageUnitEntity);
            destinationStorageUnitEntity.setStorage(storagePolicyEntity.getDestinationStorage());
            destinationStorageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
            destinationStorageUnitEntity.setStatus(storageUnitStatusEntity);
            herdDao.saveAndRefresh(destinationStorageUnitEntity);
        }

        // Set the parent storage unit on the destination storage unit.
        destinationStorageUnitEntity.setParentStorageUnit(sourceStorageUnitEntity);

        // Update the storage unit status. We make this call even for the newly created storage unit,
        // since this call also adds an entry to the storage unit status history table.
        storageUnitHelper.updateStorageUnitStatus(destinationStorageUnitEntity, storageUnitStatusEntity, StorageUnitStatusEntity.ARCHIVING);

        // Build the storage policy transition parameters DTO.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto = new StoragePolicyTransitionParamsDto();
        storagePolicyTransitionParamsDto.setBusinessObjectDataKey(businessObjectDataKey);
        storagePolicyTransitionParamsDto.setSourceStorageName(sourceStorageName);
        storagePolicyTransitionParamsDto.setSourceBucketName(sourceBucketName);
        storagePolicyTransitionParamsDto.setSourceStorageUnitId(sourceStorageUnitEntity.getId());
        storagePolicyTransitionParamsDto.setSourceS3KeyPrefix(s3KeyPrefix);
        storagePolicyTransitionParamsDto.setSourceStorageFiles(storageFiles);
        storagePolicyTransitionParamsDto.setSourceStorageFilesSizeBytes(storageFilesSizeBytes);
        storagePolicyTransitionParamsDto.setDestinationStorageName(destinationStorageName);
        storagePolicyTransitionParamsDto.setDestinationVaultName(destinationVaultName);

        return storagePolicyTransitionParamsDto;
    }

    /**
     * Validates the storage policy selection. This method also trims the request parameters.
     *
     * @param storagePolicySelection the storage policy selection
     */
    private void validateStoragePolicySelection(StoragePolicySelection storagePolicySelection)
    {
        Assert.notNull(storagePolicySelection, "A storage policy selection must be specified.");
        herdHelper.validateBusinessObjectDataKey(storagePolicySelection.getBusinessObjectDataKey(), true, true);
        storagePolicyHelper.validateStoragePolicyKey(storagePolicySelection.getStoragePolicyKey());
    }

    /**
     * Validate that business object data status is supported by the storage policy feature.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param businessObjectDataKey the business object data key
     */
    private void validateBusinessObjectData(BusinessObjectDataEntity businessObjectDataEntity, BusinessObjectDataKey businessObjectDataKey)
    {
        Assert.isTrue(StoragePolicySelectorServiceImpl.SUPPORTED_BUSINESS_OBJECT_DATA_STATUSES.contains(businessObjectDataEntity.getStatus().getCode()), String
            .format("Business object data status \"%s\" is not supported by the storage policy feature. Business object data: {%s}",
                businessObjectDataEntity.getStatus().getCode(), herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));
    }

    /**
     * Validates the source storage.
     *
     * @param storageEntity the storage entity
     * @param storagePolicyKey the storage policy key
     */
    private void validateSourceStorage(StorageEntity storageEntity, StoragePolicyKey storagePolicyKey)
    {
        // Validate that storage platform is S3 for the storage policy filter storage.
        Assert.isTrue(StoragePlatformEntity.S3.equals(storageEntity.getStoragePlatform().getName()), String
            .format("Storage platform for storage policy filter storage with name \"%s\" is not \"%s\". Storage policy: {%s}", storageEntity.getName(),
                StoragePlatformEntity.S3, storagePolicyHelper.storagePolicyKeyToString(storagePolicyKey)));

        // Validate that storage policy filter storage has the S3 path prefix validation enabled.
        if (!storageDaoHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true))
        {
            throw new IllegalStateException(String
                .format("Path prefix validation must be enabled on \"%s\" storage. Storage policy: {%s}", storageEntity.getName(),
                    storagePolicyHelper.storagePolicyKeyToString(storagePolicyKey)));
        }

        // Validate that storage policy filter storage has the S3 file existence validation enabled.
        if (!storageDaoHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true))
        {
            throw new IllegalStateException(String
                .format("File existence validation must be enabled on \"%s\" storage. Storage policy: {%s}", storageEntity.getName(),
                    storagePolicyHelper.storagePolicyKeyToString(storagePolicyKey)));
        }
    }

    /**
     * Validates the destination storage.
     *
     * @param storageEntity the destination storage entity
     * @param storagePolicyKey the storage policy key
     */
    private void validateDestinationStorage(StorageEntity storageEntity, StoragePolicyKey storagePolicyKey)
    {
        // Validate that storage platform is GLACIER for the destination storage.
        Assert.isTrue(StoragePlatformEntity.GLACIER.equals(storageEntity.getStoragePlatform().getName()), String
            .format("Storage platform for storage policy transition destination storage with name \"%s\" is not \"%s\". Storage policy: {%s}",
                storageEntity.getName(), StoragePlatformEntity.GLACIER, storagePolicyHelper.storagePolicyKeyToString(storagePolicyKey)));
    }

    /**
     * Validates that source storage unit status is ENABLED.
     *
     * @param storageUnitEntity the source storage unit entity, not null
     * @param storageName the source storage name
     * @param businessObjectDataKey the business object data key
     */
    private void validateSourceStorageUnit(StorageUnitEntity storageUnitEntity, String storageName, BusinessObjectDataKey businessObjectDataKey)
    {
        Assert.isTrue(StorageUnitStatusEntity.ENABLED.equals(storageUnitEntity.getStatus().getCode()), String.format(
            "Source storage unit status is \"%s\", but must be \"%s\" for storage policy transition to proceed. Storage: {%s}, business object data: {%s}",
            storageUnitEntity.getStatus().getCode(), StorageUnitStatusEntity.ENABLED, storageName,
            herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));
    }

    /**
     * Validate that if destination storage unit exists it is in DISABLED state and has no more than one storage file.
     *
     * @param storageUnitEntity the destination storage unit entity, may be null
     * @param storageName the destination storage name
     * @param businessObjectDataKey the business object data key
     */
    private void validateDestinationStorageUnit(StorageUnitEntity storageUnitEntity, String storageName, BusinessObjectDataKey businessObjectDataKey)
    {
        if (storageUnitEntity != null)
        {
            // Validate that destination storage unit is in DISABLED state.
            if (!StorageUnitStatusEntity.DISABLED.equals(storageUnitEntity.getStatus().getCode()))
            {
                throw new AlreadyExistsException(String
                    .format("Destination storage unit already exists and has \"%s\" status. Storage: {%s}, business object data: {%s}",
                        storageUnitEntity.getStatus().getCode(), storageName, herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));
            }

            // Throw an exception if destination storage unit has more than one storage file.
            if (storageUnitEntity.getStorageFiles().size() > 1)
            {
                throw new IllegalStateException(String.format(
                    "Destination storage unit has %d storage files, but must have none or just one storage file. Storage: {%s}, business object data: {%s}",
                    storageUnitEntity.getStorageFiles().size(), storageName, herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));
            }
        }
    }

    /**
     * Validates that the total size of storage files for the business object data is not greater than the threshold value configured in the system.
     *
     * @param storageName the storage name
     * @param businessObjectDataKey the business object data key
     * @param storageFilesSizeBytes the total size of the storage files in bytes
     */
    private void validateTotalStorageFilesSize(String storageName, BusinessObjectDataKey businessObjectDataKey, long storageFilesSizeBytes)
    {
        // Get the threshold value configured in the system.
        Integer storageFilesSizeThresholdGb =
            configurationHelper.getProperty(ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_SIZE_THRESHOLD_GB, Integer.class);

        // Perform the check.
        if (storageFilesSizeThresholdGb != null)
        {
            long storageFilesSizeThresholdBytes = HerdFileUtils.BYTES_PER_GB * storageFilesSizeThresholdGb;
            Assert.isTrue(storageFilesSizeBytes <= storageFilesSizeThresholdBytes, String.format(
                "Total size of storage files (%d bytes) for business object data in \"%s\" storage is greater " +
                    "than the configured threshold of %d GB (%d bytes) as per \"%s\" configuration entry. Business object data: {%s}", storageFilesSizeBytes,
                storageName, storageFilesSizeThresholdGb, storageFilesSizeThresholdBytes,
                ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_SIZE_THRESHOLD_GB.getKey(), herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public StoragePolicyTransitionParamsDto executeStoragePolicyTransition(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        return executeStoragePolicyTransitionImpl(storagePolicyTransitionParamsDto);
    }

    /**
     * Executes a storage policy transition as per specified storage policy selection.
     *
     * @param storagePolicyTransitionParamsDto the storage policy transition DTO that contains parameters needed to perform a storage policy transition
     *
     * @return the storage policy transition DTO updated with the policy transition results
     */
    protected StoragePolicyTransitionParamsDto executeStoragePolicyTransitionImpl(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        Path localTempDirPath = null;
        File tarFile = null;

        try
        {
            // Get a temporary directory name configured in the system configuration.
            String rootTempDir = configurationHelper.getProperty(ConfigurationValue.STORAGE_POLICY_PROCESSOR_TEMP_DIR);

            // If the temporary directory is not configured, default it to the system provided temporary directory.
            if (StringUtils.isBlank(rootTempDir))
            {
                rootTempDir = Files.createTempDirectory(null).toString();
            }

            // Add a unique temporary subfolder created using source storage unit id and the system timestamp.
            String timestamp = new SimpleDateFormat("yyyyMMddhhmm", Locale.US).format(new Date());
            String tempSubfolderName = String.format("%d-%s", storagePolicyTransitionParamsDto.getSourceStorageUnitId(), timestamp);
            localTempDirPath = Paths.get(rootTempDir, tempSubfolderName);

            // Crate a temporary directory.
            LOGGER.info(String.format("Creating \"%s\" local temporary directory ...", localTempDirPath.toString()));
            if (!localTempDirPath.toFile().mkdir())
            {
                String errorMessage = String.format("Failed to create \"%s\" local temporary directory.", localTempDirPath.toString());
                localTempDirPath = null;
                throw new IllegalStateException(errorMessage);
            }

            // Create an S3 file transfer parameters DTO to access the source S3 bucket.
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = storageDaoHelper.getS3FileTransferRequestParamsDto();

            // Get the list of S3 files matching the expected S3 key prefix.
            s3FileTransferRequestParamsDto.setS3BucketName(storagePolicyTransitionParamsDto.getSourceBucketName());
            // Get an optional S3 endpoint configuration value to be used for AWS S3 services.
            s3FileTransferRequestParamsDto.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
            // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
            s3FileTransferRequestParamsDto.setS3KeyPrefix(storagePolicyTransitionParamsDto.getSourceS3KeyPrefix() + "/");
            // When listing S3 files, we ignore 0 byte objects that represent S3 directories.
            List<String> actualS3Files = storageFileHelper.getFilePaths(s3Service.listDirectory(s3FileTransferRequestParamsDto, true));

            // Validate S3 files before we start the download.
            herdHelper.validateS3Files(storagePolicyTransitionParamsDto.getSourceStorageName(), storagePolicyTransitionParamsDto.getSourceStorageFiles(),
                actualS3Files, storagePolicyTransitionParamsDto.getSourceS3KeyPrefix());

            // Download S3 files to the temporary local directory.
            s3FileTransferRequestParamsDto.setLocalPath(localTempDirPath.toString());
            s3FileTransferRequestParamsDto.setRecursive(true);
            s3Service.downloadDirectory(s3FileTransferRequestParamsDto);

            // Validate the downloaded S3 files.
            herdHelper.validateDownloadedS3Files(s3FileTransferRequestParamsDto.getLocalPath(), storagePolicyTransitionParamsDto.getSourceS3KeyPrefix(),
                storagePolicyTransitionParamsDto.getSourceStorageFiles());

            // Log a list of files downloaded to the target local directory.
            if (LOGGER.isInfoEnabled())
            {
                logDownloadedS3Files(localTempDirPath.toFile());
            }

            // Create a TAR file name for this archive using source storage unit id and the system timestamp.
            String tarFileName = tempSubfolderName + ".tar";
            Path tarFilePath = Paths.get(rootTempDir, tarFileName);
            tarFile = tarFilePath.toFile();

            // Create a TAR archive.
            tarHelper.createTarArchive(tarFile, localTempDirPath);

            // Log the TAR file contents.
            if (LOGGER.isInfoEnabled())
            {
                tarHelper.logTarFileContents(tarFile);
            }

            // Sanity check the TAR file size.
            tarHelper.validateTarFileSize(tarFile, storagePolicyTransitionParamsDto.getSourceStorageFilesSizeBytes(),
                storagePolicyTransitionParamsDto.getSourceStorageName(), storagePolicyTransitionParamsDto.getBusinessObjectDataKey());

            // Upload the archive to Glacier.
            GlacierArchiveTransferRequestParamsDto glacierArchiveTransferRequestParamsDto = new GlacierArchiveTransferRequestParamsDto();
            AwsParamsDto awsParamsDto = awsHelper.getAwsParamsDto();
            glacierArchiveTransferRequestParamsDto.setHttpProxyHost(awsParamsDto.getHttpProxyHost());
            glacierArchiveTransferRequestParamsDto.setHttpProxyPort(awsParamsDto.getHttpProxyPort());
            glacierArchiveTransferRequestParamsDto.setVaultName(storagePolicyTransitionParamsDto.getDestinationVaultName());
            glacierArchiveTransferRequestParamsDto.setLocalFilePath(tarFilePath.toString());
            GlacierArchiveTransferResultsDto glacierArchiveTransferResultsDto = glacierService.uploadArchive(glacierArchiveTransferRequestParamsDto);

            // Update the storagePolicyTransitionParamsDto with the upload results.
            StorageFile destinationStorageFile = new StorageFile();
            storagePolicyTransitionParamsDto.setDestinationStorageFile(destinationStorageFile);
            destinationStorageFile.setFilePath(tarFileName);
            destinationStorageFile.setFileSizeBytes(tarFile.length());
            destinationStorageFile.setArchiveId(glacierArchiveTransferResultsDto.getArchiveId());

            return storagePolicyTransitionParamsDto;
        }
        catch (InterruptedException | IOException e)
        {
            LOGGER.error("Failed to execute storage policy transition.", e);
            throw new IllegalStateException(e);
        }
        finally
        {
            if (localTempDirPath != null)
            {
                LOGGER.info(String.format("Deleting \"%s\" local temporary directory ...", localTempDirPath.toString()));
                HerdFileUtils.deleteDirectoryIgnoreException(localTempDirPath.toFile());
            }

            if (tarFile != null && tarFile.exists())
            {
                LOGGER.info(String.format("Deleting \"%s\" local TAR archive file copy ...", tarFile.getPath()));
                HerdFileUtils.deleteFileIgnoreException(tarFile);
            }
        }
    }

    /**
     * Logs all files found in the specified local directory.
     *
     * @param directory the target local directory
     */
    private void logDownloadedS3Files(File directory)
    {
        Collection<File> files = HerdFileUtils.listFiles(directory, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        LOGGER.info(String.format("Downloaded %d S3 files in \"%s\" temporary local directory:", files.size(), directory.getPath()));

        for (File file : files)
        {
            LOGGER.info(String.format("    %s", file.getPath()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void completeStoragePolicyTransition(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        completeStoragePolicyTransitionImpl(storagePolicyTransitionParamsDto);
    }

    /**
     * Completes a storage policy transition as per specified storage policy selection.
     *
     * @param storagePolicyTransitionParamsDto the storage policy transition DTO that contains parameters needed to complete a storage policy transition
     */
    protected void completeStoragePolicyTransitionImpl(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        // Get the business object data key.
        BusinessObjectDataKey businessObjectDataKey = storagePolicyTransitionParamsDto.getBusinessObjectDataKey();

        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = herdDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Validate that business object data status is supported by the storage policy feature.
        String businessObjectDataStatus = businessObjectDataEntity.getStatus().getCode();
        Assert.isTrue(StoragePolicySelectorServiceImpl.SUPPORTED_BUSINESS_OBJECT_DATA_STATUSES.contains(businessObjectDataStatus), String
            .format("Business object data status \"%s\" is not supported by the storage policy feature. Business object data: {%s}", businessObjectDataStatus,
                herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Retrieve the source storage unit and ensure it exists.
        StorageUnitEntity sourceStorageUnitEntity =
            storageDaoHelper.getStorageUnitEntity(businessObjectDataEntity, storagePolicyTransitionParamsDto.getSourceStorageName());

        // Validate that source storage unit status is ENABLED.
        Assert.isTrue(StorageUnitStatusEntity.ENABLED.equals(sourceStorageUnitEntity.getStatus().getCode()), String.format(
            "Source storage unit status is \"%s\", but must be \"%s\" for storage policy transition to proceed. Storage: {%s}, business object data: {%s}",
            sourceStorageUnitEntity.getStatus().getCode(), StorageUnitStatusEntity.ENABLED, storagePolicyTransitionParamsDto.getSourceStorageName(),
            herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Retrieve the destination storage unit and ensure it exists.
        StorageUnitEntity destinationStorageUnitEntity =
            storageDaoHelper.getStorageUnitEntity(businessObjectDataEntity, storagePolicyTransitionParamsDto.getDestinationStorageName());

        // Validate that destination storage unit status is ARCHIVING.
        Assert.isTrue(StorageUnitStatusEntity.ARCHIVING.equals(destinationStorageUnitEntity.getStatus().getCode()), String.format(
            "Destination storage unit status is \"%s\", but must be \"%s\" for storage policy transition to proceed. Storage: {%s}, business object data: {%s}",
            destinationStorageUnitEntity.getStatus().getCode(), StorageUnitStatusEntity.ARCHIVING, storagePolicyTransitionParamsDto.getDestinationStorageName(),
            herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Save the destination storage file info.
        StorageFileEntity destinationStorageFileEntity;
        if (destinationStorageUnitEntity.getStorageFiles().size() == 0)
        {
            // Create a new storage file entity and add it to the destination storage unit.
            destinationStorageFileEntity = new StorageFileEntity();
            destinationStorageFileEntity.setStorageUnit(destinationStorageUnitEntity);
        }
        else if (destinationStorageUnitEntity.getStorageFiles().size() == 1)
        {
            // Get the existing destination storage file entity.
            destinationStorageFileEntity = destinationStorageUnitEntity.getStorageFiles().iterator().next();
        }
        else
        {
            // Throw an exception if destination storage unit has no more than one storage file.
            throw new IllegalStateException(String
                .format("Destination storage unit has %d storage files, but must have none or just one storage file. Storage: {%s}, business object data: {%s}",
                    destinationStorageUnitEntity.getStorageFiles().size(), storagePolicyTransitionParamsDto.getDestinationStorageName(),
                    herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }
        destinationStorageFileEntity.setPath(storagePolicyTransitionParamsDto.getDestinationStorageFile().getFilePath());
        destinationStorageFileEntity.setFileSizeBytes(storagePolicyTransitionParamsDto.getDestinationStorageFile().getFileSizeBytes());
        destinationStorageFileEntity.setRowCount(null);
        destinationStorageFileEntity.setArchiveId(storagePolicyTransitionParamsDto.getDestinationStorageFile().getArchiveId());
        herdDao.saveAndRefresh(destinationStorageFileEntity);

        // Change the destination storage unit status to ENABLED.
        String reason = StorageUnitStatusEntity.ARCHIVING;
        storageUnitHelper.updateStorageUnitStatus(destinationStorageUnitEntity, StorageUnitStatusEntity.ENABLED, reason);

        // Change the source storage unit status to DISABLED.
        storageUnitHelper.updateStorageUnitStatus(sourceStorageUnitEntity, StorageUnitStatusEntity.DISABLED, reason);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void executeStoragePolicyTransitionAfterStep(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        executeStoragePolicyTransitionAfterStepImpl(storagePolicyTransitionParamsDto);
    }

    /**
     * Executes a step after the storage policy transition is completed.
     *
     * @param storagePolicyTransitionParamsDto the storage policy transition DTO that contains parameters needed for the storage policy transition after step
     */
    protected void executeStoragePolicyTransitionAfterStepImpl(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        // Delete the source S3 data.

        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        String s3KeyPrefixWithTrailingSlash = storagePolicyTransitionParamsDto.getSourceS3KeyPrefix() + "/";

        LOGGER.info(String.format("Deleting source S3 data objects matching \"%s\" prefix from \"%s\" S3 bucket ...", s3KeyPrefixWithTrailingSlash,
            storagePolicyTransitionParamsDto.getSourceBucketName()));

        // Create an S3 file transfer parameters DTO to access the source S3 bucket.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = storageDaoHelper.getS3FileTransferRequestParamsDto();
        // Set the source S3 bucket name.
        s3FileTransferRequestParamsDto.setS3BucketName(storagePolicyTransitionParamsDto.getSourceBucketName());
        // Set an optional S3 endpoint configuration value to be used for AWS S3 services.
        s3FileTransferRequestParamsDto.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
        // Set the S3 key prefix with a trailing '/' character.
        s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefixWithTrailingSlash);

        // Try to delete a list of all keys/objects from S3 managed bucket matching the expected S3 key prefix.
        // Please note that when deleting S3 files, we also delete all 0 byte objects that represent S3 directories.
        s3Service.deleteDirectoryIgnoreException(s3FileTransferRequestParamsDto);
    }
}
