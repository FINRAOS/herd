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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.BusinessObjectDataStorageFileService;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.HerdDaoHelper;
import org.finra.herd.service.helper.HerdHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageFileHelper;

/**
 * Service for business object data storage files.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDataStorageFileServiceImpl implements BusinessObjectDataStorageFileService
{
    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private HerdDaoHelper herdDaoHelper;

    @Autowired
    private HerdDao herdDao;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private HerdHelper herdHelper;

    @Autowired
    private StorageFileHelper storageFileHelper;

    @Autowired
    protected ConfigurationHelper configurationHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    /*
     * TODO The validation logic is repeated from
     * org.finra.herd.service.impl.BusinessObjectDataServiceImpl.createBusinessObjectDataEntity().
     * Could be moved to a common place.
     */

    /**
     * Adds files to Business object data storage.
     *
     * @param businessObjectDataStorageFilesCreateRequest the business object data storage files create request
     *
     * @return BusinessObjectDataStorageFilesCreateResponse
     */
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
        BusinessObjectDataEntity businessObjectDataEntity = herdDaoHelper.getBusinessObjectDataEntity(
            new BusinessObjectDataKey(businessObjectDataStorageFilesCreateRequest.getNamespace(),
                businessObjectDataStorageFilesCreateRequest.getBusinessObjectDefinitionName(),
                businessObjectDataStorageFilesCreateRequest.getBusinessObjectFormatUsage(),
                businessObjectDataStorageFilesCreateRequest.getBusinessObjectFormatFileType(),
                businessObjectDataStorageFilesCreateRequest.getBusinessObjectFormatVersion(), businessObjectDataStorageFilesCreateRequest.getPartitionValue(),
                businessObjectDataStorageFilesCreateRequest.getSubPartitionValues(),
                businessObjectDataStorageFilesCreateRequest.getBusinessObjectDataVersion()));

        // retrieve and validate that the storage unit exists
        StorageUnitEntity storageUnitEntity =
            herdDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, businessObjectDataStorageFilesCreateRequest.getStorageName());
        if (storageUnitEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Storage unit not found for business data {%s} and storage name \"%s\".",
                herdHelper.businessObjectDataKeyToString(getBusinessObjectDataKey(businessObjectDataStorageFilesCreateRequest)),
                businessObjectDataStorageFilesCreateRequest.getStorageName()));
        }

        // validate that files in the request does not already exist in the DB
        for (StorageFile storageFile : businessObjectDataStorageFilesCreateRequest.getStorageFiles())
        {
            // Ensure that the file is not already registered in this storage by some other business object data.
            StorageFileEntity storageFileEntity =
                herdDao.getStorageFileByStorageNameAndFilePath(storageUnitEntity.getStorage().getName(), storageFile.getFilePath());
            if (storageFileEntity != null)
            {
                throw new AlreadyExistsException(String
                    .format("S3 file \"%s\" in \"%s\" storage is already registered by the business object data {%s}.", storageFile.getFilePath(),
                        storageUnitEntity.getStorage().getName(),
                        herdDaoHelper.businessObjectDataEntityAltKeyToString(storageFileEntity.getStorageUnit().getBusinessObjectData())));
            }
        }

        StorageEntity storageEntity = storageUnitEntity.getStorage();

        boolean isS3StoragePlatform = storageEntity.getStoragePlatform().getName().equals(StoragePlatformEntity.S3);

        // Get the S3 validation flags.
        boolean validatePathPrefix = storageDaoHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true);
        boolean validateFileExistence = storageDaoHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true);

        if ((validatePathPrefix || validateFileExistence) && isS3StoragePlatform)
        {
            // Perform expected path and file existence validation.

            // validate S3 prefix for each file
            String expectedS3KeyPrefix = businessObjectDataHelper
                .buildS3KeyPrefix(businessObjectDataEntity.getBusinessObjectFormat(), herdDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity));

            // Add a trailing backslash if it doesn't already exist.
            if (!expectedS3KeyPrefix.endsWith("/"))
            {
                expectedS3KeyPrefix += "/";
            }

            if (validatePathPrefix)
            {
                for (StorageFile storageFile : businessObjectDataStorageFilesCreateRequest.getStorageFiles())
                {
                    Assert.isTrue(storageFile.getFilePath().startsWith(expectedS3KeyPrefix), String
                        .format("Specified storage file path \"%s\" does not match the expected S3 key prefix \"%s\".", storageFile.getFilePath(),
                            expectedS3KeyPrefix));
                }
            }

            if (validateFileExistence)
            {
                // Get the validate file parameters.
                S3FileTransferRequestParamsDto params =
                    businessObjectDataHelper.getFileValidationParams(storageEntity, expectedS3KeyPrefix, storageUnitEntity, validatePathPrefix);

                // When listing S3 files, we ignore 0 byte objects that represent S3 directories.
                List<String> actualS3Keys = storageFileHelper.getFilePaths(s3Service.listDirectory(params, true));

                for (StorageFile requestStorageFile : businessObjectDataStorageFilesCreateRequest.getStorageFiles())
                {
                    if (!actualS3Keys.contains(requestStorageFile.getFilePath()))
                    {
                        throw new ObjectNotFoundException(
                            String.format("File not found at s3://%s/%s location.", params.getS3BucketName(), requestStorageFile.getFilePath()));
                    }
                }
            }
        }
        else if (storageUnitEntity.getDirectoryPath() != null)
        {
            // If a directory exists, then validate that all files are contained within that directory.
            for (StorageFile storageFile : businessObjectDataStorageFilesCreateRequest.getStorageFiles())
            {
                Assert.isTrue(storageFile.getFilePath().startsWith(storageUnitEntity.getDirectoryPath()), String
                    .format("Storage file path \"%s\" does not match the storage directory path \"%s\".", storageFile.getFilePath(),
                        storageUnitEntity.getDirectoryPath()));
            }
        }
        // If the validation flags don't exist and no directory is specified, then no storage validations occur.

        // Add new files to existing storage
        for (StorageFile storageFile : businessObjectDataStorageFilesCreateRequest.getStorageFiles())
        {
            StorageFileEntity storageFileEntity = new StorageFileEntity();
            storageFileEntity.setFileSizeBytes(storageFile.getFileSizeBytes());
            storageFileEntity.setPath(storageFile.getFilePath());
            storageFileEntity.setRowCount(storageFile.getRowCount());
            storageFileEntity.setStorageUnit(storageUnitEntity);
            herdDao.saveAndRefresh(storageFileEntity);
        }

        // Construct and return response
        BusinessObjectDataStorageFilesCreateResponse businessObjectDataStorageFilesCreateResponse = new BusinessObjectDataStorageFilesCreateResponse();
        businessObjectDataStorageFilesCreateResponse
            .setNamespace(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode());
        businessObjectDataStorageFilesCreateResponse
            .setBusinessObjectDefinitionName(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName());
        businessObjectDataStorageFilesCreateResponse.setBusinessObjectFormatUsage(businessObjectDataEntity.getBusinessObjectFormat().getUsage());
        businessObjectDataStorageFilesCreateResponse
            .setBusinessObjectFormatFileType(businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode());
        businessObjectDataStorageFilesCreateResponse
            .setBusinessObjectFormatVersion(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion());
        businessObjectDataStorageFilesCreateResponse.setPartitionValue(businessObjectDataEntity.getPartitionValue());
        businessObjectDataStorageFilesCreateResponse.setSubPartitionValues(herdHelper.getSubPartitionValues(businessObjectDataEntity));
        businessObjectDataStorageFilesCreateResponse.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());
        businessObjectDataStorageFilesCreateResponse.setStorageName(storageEntity.getName());
        // For the files, just echo back the requested files
        businessObjectDataStorageFilesCreateResponse.setStorageFiles(businessObjectDataStorageFilesCreateRequest.getStorageFiles());

        return businessObjectDataStorageFilesCreateResponse;
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

        int subPartitionValuesCount = herdHelper.getCollectionSize(businessObjectDataStorageFilesCreateRequest.getSubPartitionValues());
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

        Assert.notEmpty(businessObjectDataStorageFilesCreateRequest.getStorageFiles(), "At least one storage file must be specified.");

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
}
