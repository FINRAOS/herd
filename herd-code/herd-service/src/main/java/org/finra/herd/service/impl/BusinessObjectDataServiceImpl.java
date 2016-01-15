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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.StsDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailability;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdl;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDownloadCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStatus;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusInformation;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataUploadCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataVersion;
import org.finra.herd.model.api.xml.BusinessObjectDataVersions;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.api.xml.LatestAfterPartitionValue;
import org.finra.herd.model.api.xml.LatestBeforePartitionValue;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.CustomDdlEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.BusinessObjectDataService;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.AwsPolicyBuilder;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectDataInvalidateUnregisteredHelper;
import org.finra.herd.service.helper.DdlGeneratorFactory;
import org.finra.herd.service.helper.HerdDaoHelper;
import org.finra.herd.service.helper.HerdHelper;
import org.finra.herd.service.helper.KmsActions;
import org.finra.herd.service.helper.StorageDaoHelper;

/**
 * The business object data service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDataServiceImpl implements BusinessObjectDataService
{
    /**
     * A status reason of "not registered".
     */
    public static final String REASON_NOT_REGISTERED = "NOT_REGISTERED";

    /**
     * A status reason of "no enabled storage unit".
     */
    public static final String REASON_NO_ENABLED_STORAGE_UNIT = "NO_ENABLED_STORAGE_UNIT";

    /**
     * A status reason of "archived".
     */
    public static final String REASON_ARCHIVED = "ARCHIVED";

    private static final Logger LOGGER = Logger.getLogger(BusinessObjectDataServiceImpl.class);

    @Autowired
    private HerdDao herdDao;

    @Autowired
    private HerdDaoHelper herdDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private HerdHelper herdHelper;

    @Autowired
    private DdlGeneratorFactory ddlGeneratorFactory;

    @Autowired
    private BusinessObjectDataInvalidateUnregisteredHelper businessObjectDataInvalidateUnregisteredHelper;

    @Autowired
    protected ConfigurationHelper configurationHelper;

    @Autowired
    protected StsDao stsDao;

    @Autowired
    protected AwsHelper awsHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    /**
     * Gets the S3 key prefix. This method starts a new transaction.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectFormatPartitionKey the business object format partition key
     * @param createNewVersion specifies if it is OK to return an S3 key prefix for a new business object data version that is not an initial version. This
     * parameter is ignored, when the business object data version is specified.
     *
     * @return the retrieved business object data information
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public S3KeyPrefixInformation getS3KeyPrefix(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey, Boolean createNewVersion)
    {
        return getS3KeyPrefixImpl(businessObjectDataKey, businessObjectFormatPartitionKey, createNewVersion);
    }

    /**
     * Gets the S3 key prefix.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectFormatPartitionKey the business object format partition key
     * @param createNewVersion specifies if it is OK to return an S3 key prefix for a new business object data version that is not an initial version. This
     * parameter is ignored, when the business object data version is specified.
     *
     * @return the S3 key prefix
     */
    protected S3KeyPrefixInformation getS3KeyPrefixImpl(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey,
        Boolean createNewVersion)
    {
        String businessObjectFormatPartitionKeyLocal = businessObjectFormatPartitionKey;

        // Validate and trim the business object data key.
        herdHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, false);

        // If specified, trim the partition key parameter.
        if (businessObjectFormatPartitionKeyLocal != null)
        {
            businessObjectFormatPartitionKeyLocal = businessObjectFormatPartitionKeyLocal.trim();
        }

        // Get the business object format for the specified parameters and make sure it exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDaoHelper.getBusinessObjectFormatEntity(
            new BusinessObjectFormatKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                businessObjectDataKey.getBusinessObjectFormatVersion()));

        // If specified, ensure that partition key matches what's configured within the business object format.
        if (StringUtils.isNotBlank(businessObjectFormatPartitionKeyLocal))
        {
            Assert.isTrue(businessObjectFormatEntity.getPartitionKey().equalsIgnoreCase(businessObjectFormatPartitionKeyLocal),
                "Partition key \"" + businessObjectFormatPartitionKeyLocal + "\" doesn't match configured business object format partition key \"" +
                    businessObjectFormatEntity.getPartitionKey() + "\".");
        }

        // If the business object data version is not specified, get the next business object data version value.
        if (businessObjectDataKey.getBusinessObjectDataVersion() == null)
        {
            // Get the latest data version for this business object data, if it exists.
            BusinessObjectDataEntity latestVersionBusinessObjectDataEntity = herdDao.getBusinessObjectDataByAltKey(
                new BusinessObjectDataKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                    businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                    businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(),
                    businessObjectDataKey.getSubPartitionValues(), null));

            // Throw an error if this business object data already exists and createNewVersion flag is not set.
            if (latestVersionBusinessObjectDataEntity != null && !createNewVersion)
            {
                throw new AlreadyExistsException("Initial version of the business object data already exists.");
            }

            businessObjectDataKey.setBusinessObjectDataVersion(
                latestVersionBusinessObjectDataEntity == null ? BusinessObjectDataEntity.BUSINESS_OBJECT_DATA_INITIAL_VERSION :
                    latestVersionBusinessObjectDataEntity.getVersion() + 1);
        }

        // Build the S3 key prefix string.
        String s3KeyPrefix = businessObjectDataHelper.buildS3KeyPrefix(businessObjectFormatEntity, businessObjectDataKey);

        // Create and return the S3 key prefix.
        S3KeyPrefixInformation s3KeyPrefixInformation = new S3KeyPrefixInformation();
        s3KeyPrefixInformation.setS3KeyPrefix(s3KeyPrefix);
        return s3KeyPrefixInformation;
    }

    /**
     * Creates a new business object data from the request information. Creates its own transaction.
     *
     * @param request the request.
     *
     * @return the newly created and persisted business object data.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectData createBusinessObjectData(BusinessObjectDataCreateRequest request)
    {
        return businessObjectDataHelper.createBusinessObjectData(request);
    }

    /**
     * Retrieves existing business object data entry information. This method starts a new transaction.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectFormatPartitionKey the business object format partition key
     *
     * @return the retrieved business object data information
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectData getBusinessObjectData(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey)
    {
        return getBusinessObjectDataImpl(businessObjectDataKey, businessObjectFormatPartitionKey);
    }

    /**
     * Retrieves existing business object data entry information. This method does not start a new transaction and instead continues with existing transaction,
     * if any.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectFormatPartitionKey the business object format partition key
     *
     * @return the retrieved business object data information
     */
    protected BusinessObjectData getBusinessObjectDataImpl(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey)
    {
        String businessObjectFormatPartitionKeyLocal = businessObjectFormatPartitionKey;

        // Validate and trim the business object data key.
        herdHelper.validateBusinessObjectDataKey(businessObjectDataKey, false, false);

        // If specified, trim the partition key parameter.
        if (businessObjectFormatPartitionKeyLocal != null)
        {
            businessObjectFormatPartitionKeyLocal = businessObjectFormatPartitionKeyLocal.trim();
        }

        // Get the business object data based on the specified parameters. If a business object data
        // version isn't specified, the latest VALID business object data version is returned.
        BusinessObjectDataEntity businessObjectDataEntity =
            herdDaoHelper.getBusinessObjectDataEntityByKeyAndStatus(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID);

        // If specified, ensure the partition key matches what's configured within the business object format.
        if (StringUtils.isNotBlank(businessObjectFormatPartitionKeyLocal))
        {
            String configuredPartitionKey = businessObjectDataEntity.getBusinessObjectFormat().getPartitionKey();
            Assert.isTrue(configuredPartitionKey.equalsIgnoreCase(businessObjectFormatPartitionKeyLocal), String
                .format("Partition key \"%s\" doesn't match configured business object format partition key \"%s\".", businessObjectFormatPartitionKeyLocal,
                    configuredPartitionKey));
        }

        // Create and return the business object definition object from the persisted entity.
        return businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity);
    }

    /**
     * Retrieves a list of existing business object data versions, if any.
     *
     * @param businessObjectDataKey the business object data key with possibly missing business object format and/or data version values
     *
     * @return the retrieved business object data versions
     */
    @Override
    public BusinessObjectDataVersions getBusinessObjectDataVersions(BusinessObjectDataKey businessObjectDataKey)
    {
        // Validate and trim the business object data key.
        herdHelper.validateBusinessObjectDataKey(businessObjectDataKey, false, false);

        // Get the business object data versions based on the specified parameters.
        List<BusinessObjectDataEntity> businessObjectDataEntities = herdDao.getBusinessObjectDataEntities(businessObjectDataKey);

        // Create the response.
        BusinessObjectDataVersions businessObjectDataVersions = new BusinessObjectDataVersions();
        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities)
        {
            BusinessObjectDataVersion businessObjectDataVersion = new BusinessObjectDataVersion();
            BusinessObjectDataKey businessObjectDataVersionKey = herdDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity);
            businessObjectDataVersion.setBusinessObjectDataKey(businessObjectDataVersionKey);
            businessObjectDataVersion.setStatus(businessObjectDataEntity.getStatus().getCode());
            businessObjectDataVersions.getBusinessObjectDataVersions().add(businessObjectDataVersion);
        }

        return businessObjectDataVersions;
    }

    /**
     * Deletes an existing business object data.
     *
     * @param businessObjectDataKey the business object data key
     * @param deleteFiles specifies if data files should be deleted or not
     *
     * @return the deleted business object data information
     */
    @Override
    public BusinessObjectData deleteBusinessObjectData(BusinessObjectDataKey businessObjectDataKey, Boolean deleteFiles)
    {
        // Validate and trim the business object data key.
        herdHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true);

        // Validate the mandatory deleteFiles flag.
        Assert.notNull(deleteFiles, "A delete files flag must be specified.");

        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = herdDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Check if we are allowed to delete this business object data.
        if (!businessObjectDataEntity.getBusinessObjectDataChildren().isEmpty())
        {
            throw new IllegalArgumentException(String
                .format("Can not delete a business object data that has children associated with it. Business object data: {%s}",
                    herdDaoHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        // If the flag is set, clean up the data files from all storages of S3 storage platform type.
        LOGGER.info(String.format("deleteFiles flag is set to \"%s\"", deleteFiles.toString()));
        if (deleteFiles)
        {
            // Loop over all storage units for this business object data.
            for (StorageUnitEntity storageUnitEntity : businessObjectDataEntity.getStorageUnits())
            {
                StorageEntity storageEntity = storageUnitEntity.getStorage();

                // Currently, we only support data file deletion from S3 platform type.
                if (storageEntity.getStoragePlatform().getName().equals(StoragePlatformEntity.S3))
                {
                    LOGGER.info(String.format("Deleting data files from \"%s\" storage for business object data {%s}...", storageEntity.getName(),
                        herdDaoHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));

                    // Get the S3 validation flags.
                    boolean validatePathPrefix = storageDaoHelper
                        .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX),
                            storageEntity, false, true);

                    // If this storage conforms to the path prefix validation, then delete all keys found under the S3 key prefix.
                    if (validatePathPrefix)
                    {
                        // Build the S3 key prefix as per S3 Naming Convention Wiki page.
                        String s3KeyPrefix =
                            businessObjectDataHelper.buildS3KeyPrefix(businessObjectDataEntity.getBusinessObjectFormat(), businessObjectDataKey);

                        // Get S3 bucket access parameters, such as bucket name, AWS access key ID, AWS secret access key, etc...
                        S3FileTransferRequestParamsDto params = storageDaoHelper.getS3BucketAccessParams(storageEntity);
                        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
                        params.setS3KeyPrefix(s3KeyPrefix + "/");
                        // Delete a list of all keys/objects from S3 managed bucket matching the expected S3 key prefix.
                        // Please note that when deleting S3 files, we also delete all 0 byte objects that represent S3 directories.
                        s3Service.deleteDirectory(params);
                    }
                    // For a non S3 prefixed paths, delete the files explicitly or if only directory is registered, delete all files/subfolders found under it.
                    else
                    {
                        // Get S3 bucket access parameters, such as bucket name, AWS access key ID, AWS secret access key, etc...
                        S3FileTransferRequestParamsDto params = storageDaoHelper.getS3BucketAccessParams(storageEntity);

                        // If only directory is registered delete all files/sub-folders found under it.
                        if (StringUtils.isNotBlank(storageUnitEntity.getDirectoryPath()) && storageUnitEntity.getStorageFiles().isEmpty())
                        {
                            // Since the directory path represents a directory, we add a trailing '/' character to it.
                            params.setS3KeyPrefix(storageUnitEntity.getDirectoryPath() + "/");
                            // Delete a list of all keys/objects from S3 bucket matching the directory path.
                            // Please note that when deleting S3 files, we also delete all 0 byte objects that represent S3 directories.
                            s3Service.deleteDirectory(params);
                        }
                        // Delete the files explicitly.
                        else
                        {
                            // Create a list of files to delete.
                            List<File> files = new ArrayList<>();
                            for (StorageFileEntity storageFileEntity : storageUnitEntity.getStorageFiles())
                            {
                                files.add(new File(storageFileEntity.getPath()));
                            }
                            params.setFiles(files);
                            s3Service.deleteFileList(params);
                        }
                    }
                }
                else
                {
                    LOGGER.info(String.format(
                        "Skipping data file removal for a storage unit from \"%s\" storage since it is not an S3 storage platform. Business object data {%s}",
                        storageEntity.getName(), herdDaoHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
                }
            }
        }

        // Create the business object data object from the entity.
        BusinessObjectData deletedBusinessObjectData = businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity);

        // Delete this business object data.
        herdDao.delete(businessObjectDataEntity);

        // If this business object data version is the latest, set the latest flag on the previous version of this object data, if it exists.
        if (businessObjectDataEntity.getLatestVersion())
        {
            // Get the maximum version for this business object data, if it exists.
            Integer maxBusinessObjectDataVersion = herdDao.getBusinessObjectDataMaxVersion(businessObjectDataKey);

            if (maxBusinessObjectDataVersion != null)
            {
                // Retrieve the previous version business object data entity. Since we successfully got the maximum
                // version for this business object data, the retrieved entity is not expected to be null.
                BusinessObjectDataEntity previousVersionBusinessObjectDataEntity = herdDao.getBusinessObjectDataByAltKey(
                    new BusinessObjectDataKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                        businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                        businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(),
                        businessObjectDataKey.getSubPartitionValues(), maxBusinessObjectDataVersion));

                // Update the previous version business object data entity.
                previousVersionBusinessObjectDataEntity.setLatestVersion(true);
                herdDao.saveAndRefresh(previousVersionBusinessObjectDataEntity);
            }
        }

        // Return the deleted business object data.
        return deletedBusinessObjectData;
    }

    /**
     * Performs a search and returns a list of business object data key values and relative statuses for a range of requested business object data. Creates its
     * own transaction.
     *
     * @param request the business object data availability request
     *
     * @return the business object data availability information
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataAvailability checkBusinessObjectDataAvailability(BusinessObjectDataAvailabilityRequest request)
    {
        return checkBusinessObjectDataAvailabilityImpl(request);
    }

    /**
     * Performs an availability check for a collection of business object data.
     *
     * @param request the business object data availability collection request
     *
     * @return the business object data availability information
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataAvailabilityCollectionResponse checkBusinessObjectDataAvailabilityCollection(
        BusinessObjectDataAvailabilityCollectionRequest request)
    {
        return checkBusinessObjectDataAvailabilityCollectionImpl(request);
    }

    /**
     * Performs a search and returns a list of business object data key values and relative statuses for a range of requested business object data.
     *
     * @param request the business object data availability request
     *
     * @return the business object data availability information
     */
    protected BusinessObjectDataAvailability checkBusinessObjectDataAvailabilityImpl(BusinessObjectDataAvailabilityRequest request)
    {
        // By default, validate and trim the request.
        return checkBusinessObjectDataAvailabilityImpl(request, false);
    }

    /**
     * Performs a search and returns a list of business object data key values and relative statuses for a range of requested business object data.
     *
     * @param request the business object data availability request
     * @param skipRequestValidation specifies whether to skip the request validation and trimming
     *
     * @return the business object data availability information
     */
    protected BusinessObjectDataAvailability checkBusinessObjectDataAvailabilityImpl(BusinessObjectDataAvailabilityRequest request,
        boolean skipRequestValidation)
    {
        // Perform the validation.
        if (!skipRequestValidation)
        {
            validateBusinessObjectDataAvailabilityRequest(request);
        }

        // Get business object format key from the request.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion());

        // Make sure that specified business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);

        // Validate that specified storages exist.
        List<String> storageNames = new ArrayList<>();
        if (StringUtils.isNotBlank(request.getStorageName()))
        {
            storageNames.add(request.getStorageName());
        }
        if (!CollectionUtils.isEmpty(request.getStorageNames()))
        {
            storageNames.addAll(request.getStorageNames());
        }
        for (String storageName : storageNames)
        {
            storageDaoHelper.getStorageEntity(storageName);
        }

        // Build partition filters based on the specified partition value filters.
        List<List<String>> partitionFilters = businessObjectDataHelper
            .buildPartitionFilters(request.getPartitionValueFilters(), request.getPartitionValueFilter(), businessObjectFormatKey,
                request.getBusinessObjectDataVersion(), storageNames, businessObjectFormatEntity);

        // Retrieve a list of storage unit entities for the specified partition values
        // The entities will be sorted by partition value that is identified by partition column position.
        // If a business object data version isn't specified, the latest VALID business object data version is returned.
        // Business object data availability works across all storage platform types, so the storage platform type is not specified in the herdDao call.
        // We want to select only "available" storage units, so we pass "true" for selectOnlyAvailableStorageUnits parameter.
        List<StorageUnitEntity> availableStorageUnitEntities = herdDao
            .getStorageUnitsByPartitionFiltersAndStorages(businessObjectFormatKey, partitionFilters, request.getBusinessObjectDataVersion(),
                BusinessObjectDataStatusEntity.VALID, storageNames, null, StoragePlatformEntity.GLACIER, true);

        // Create business object data availability object instance and initialise it with request field values.
        BusinessObjectDataAvailability businessObjectDataAvailability = createBusinessObjectDataAvailability(request);

        // Create "available" and "not available" business object data status lists.
        List<BusinessObjectDataStatus> availableStatuses = new ArrayList<>();
        businessObjectDataAvailability.setAvailableStatuses(availableStatuses);
        List<BusinessObjectDataStatus> notAvailableStatuses = new ArrayList<>();
        businessObjectDataAvailability.setNotAvailableStatuses(notAvailableStatuses);

        // Build a list of matched available partition filters and populate the available statuses list.
        // Please note that each request partition filter might result in multiple available business object data entities.
        // If storage names are not specified, fail on "duplicate" business object data (same business object data
        // instance registered with multiple storages). Otherwise, remove possible "duplicates".
        List<List<String>> matchedAvailablePartitionFilters = new ArrayList<>();
        Map<BusinessObjectDataEntity, StorageUnitEntity> businessObjectDataToStorageUnitMap = new HashMap<>();
        for (StorageUnitEntity storageUnitEntity : availableStorageUnitEntities)
        {
            BusinessObjectDataEntity businessObjectDataEntity = storageUnitEntity.getBusinessObjectData();

            if (businessObjectDataToStorageUnitMap.containsKey(businessObjectDataEntity))
            {
                // If storage names are not specified, fail on a business object data registered in multiple storages.
                // Otherwise, ignore that storage unit.
                if (CollectionUtils.isEmpty(storageNames))
                {
                    throw new IllegalArgumentException(String.format("Found business object data registered in more than one storage. " +
                        "Please specify storage(s) in the request to resolve this. Business object data {%s}",
                        herdDaoHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
                }
            }
            else
            {
                matchedAvailablePartitionFilters.add(herdDaoHelper.getPartitionFilter(storageUnitEntity.getBusinessObjectData(), partitionFilters.get(0)));

                if (StoragePlatformEntity.GLACIER.equals(storageUnitEntity.getStorage().getStoragePlatform().getName()))
                {
                    // For a Glacier storage, add the storage unit to the "not-available" statuses list with the "ARCHIVED" reason.
                    BusinessObjectDataStatus businessObjectDataStatus = createAvailableBusinessObjectDataStatus(businessObjectDataEntity);
                    businessObjectDataStatus.setReason(REASON_ARCHIVED);
                    notAvailableStatuses.add(businessObjectDataStatus);
                }
                else
                {
                    // For a non-Glacier storage, add the storage unit to the "available" statuses list.
                    availableStatuses.add(createAvailableBusinessObjectDataStatus(businessObjectDataEntity));
                }

                businessObjectDataToStorageUnitMap.put(storageUnitEntity.getBusinessObjectData(), storageUnitEntity);
            }
        }

        // Get a list of unmatched partition filters.
        List<List<String>> unmatchedPartitionFilters = new ArrayList<>(partitionFilters);
        unmatchedPartitionFilters.removeAll(matchedAvailablePartitionFilters);

        // We still need to try to retrieve business object data per list of unmatched filters regardless of business object data and/or storage unit statuses.
        // This is done to populate not-available statuses with legitimate reasons.
        // Business object data availability works across all storage platform types, so the storage platform type is not specified in the herdDao call.
        // We want to select any existing storage units regardless of their status, so we pass "false" for selectOnlyAvailableStorageUnits parameter.
        List<StorageUnitEntity> notAvailableStorageUnitEntities = herdDao
            .getStorageUnitsByPartitionFiltersAndStorages(businessObjectFormatKey, unmatchedPartitionFilters, request.getBusinessObjectDataVersion(), null,
                storageNames, null, StoragePlatformEntity.GLACIER, false);

        // For all unmatched filters, select "available" storage units in any storages of the GLACIER storage platform type.
        // This is done to be able to check if business object data with a "non-available" storage unit is actually archived.
        // We want to select only "available" storage units, so we pass "true" for selectOnlyAvailableStorageUnits parameter.
        List<StorageUnitEntity> archivedStorageUnitEntities = herdDao
            .getStorageUnitsByPartitionFiltersAndStorages(businessObjectFormatKey, unmatchedPartitionFilters, request.getBusinessObjectDataVersion(), null,
                null, StoragePlatformEntity.GLACIER, null, true);

        // Populate a set of archived business object data entities for easy access. Please note that business object data might be archived in more than
        // one Glacier storage.
        Set<BusinessObjectDataEntity> archivedBusinessObjectDataEntities = new HashSet<>();
        for (StorageUnitEntity storageUnitEntity : archivedStorageUnitEntities)
        {
            if (!archivedBusinessObjectDataEntities.contains(storageUnitEntity.getBusinessObjectData()))
            {
                archivedBusinessObjectDataEntities.add(storageUnitEntity.getBusinessObjectData());
            }
        }

        // Build a list of matched "not-available" partition filters and populate the not-available statuses list.
        // Please note that each request partition filter might result in multiple available business object data entities.
        List<List<String>> matchedNotAvailablePartitionFilters = new ArrayList<>();
        for (StorageUnitEntity storageUnitEntity : notAvailableStorageUnitEntities)
        {
            matchedNotAvailablePartitionFilters.add(herdDaoHelper.getPartitionFilter(storageUnitEntity.getBusinessObjectData(), partitionFilters.get(0)));
            notAvailableStatuses.add(createNotAvailableBusinessObjectDataStatus(storageUnitEntity, archivedBusinessObjectDataEntities));
        }

        // Update the list of unmatched partition filters.
        unmatchedPartitionFilters.removeAll(matchedNotAvailablePartitionFilters);

        // Populate the "not available" statuses per remaining unmatched filters.
        for (List<String> unmatchedPartitionFilter : unmatchedPartitionFilters)
        {
            notAvailableStatuses.add(createNotAvailableBusinessObjectDataStatus(request, unmatchedPartitionFilter, REASON_NOT_REGISTERED));
        }

        return businessObjectDataAvailability;
    }

    /**
     * Performs an availability check for a collection of business object data.
     *
     * @param businessObjectDataAvailabilityCollectionRequest the business object data availability collection requests
     *
     * @return the business object data availability information
     */
    protected BusinessObjectDataAvailabilityCollectionResponse checkBusinessObjectDataAvailabilityCollectionImpl(
        BusinessObjectDataAvailabilityCollectionRequest businessObjectDataAvailabilityCollectionRequest)
    {
        // Perform the validation of the entire request, before we start processing the individual requests that requires the database access.
        validateBusinessObjectDataAvailabilityCollectionRequest(businessObjectDataAvailabilityCollectionRequest);

        // Process the individual requests and build the response.
        BusinessObjectDataAvailabilityCollectionResponse businessObjectDataAvailabilityCollectionResponse =
            new BusinessObjectDataAvailabilityCollectionResponse();
        List<BusinessObjectDataAvailability> businessObjectDataAvailabilityResponses = new ArrayList<>();
        businessObjectDataAvailabilityCollectionResponse.setBusinessObjectDataAvailabilityResponses(businessObjectDataAvailabilityResponses);
        boolean isAllDataAvailable = true;
        boolean isAllDataNotAvailable = true;
        for (BusinessObjectDataAvailabilityRequest request : businessObjectDataAvailabilityCollectionRequest.getBusinessObjectDataAvailabilityRequests())
        {
            // Please note that when calling to process individual availability requests, we ask to skip the request validation and trimming step.
            BusinessObjectDataAvailability businessObjectDataAvailability = checkBusinessObjectDataAvailabilityImpl(request, true);
            businessObjectDataAvailabilityResponses.add(businessObjectDataAvailability);
            isAllDataAvailable = isAllDataAvailable && businessObjectDataAvailability.getNotAvailableStatuses().isEmpty();
            isAllDataNotAvailable = isAllDataNotAvailable && businessObjectDataAvailability.getAvailableStatuses().isEmpty();
        }
        businessObjectDataAvailabilityCollectionResponse.setIsAllDataAvailable(isAllDataAvailable);
        businessObjectDataAvailabilityCollectionResponse.setIsAllDataNotAvailable(isAllDataNotAvailable);

        return businessObjectDataAvailabilityCollectionResponse;
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system to perform queries for a range of requested business object data in the
     * specified storage. This method starts a new transaction.
     *
     * @param request the business object data DDL request
     *
     * @return the business object data DDL information
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataDdl generateBusinessObjectDataDdl(BusinessObjectDataDdlRequest request)
    {
        return generateBusinessObjectDataDdlImpl(request, false);
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system to perform queries for a collection of business object data in the specified
     * storages. This method starts a new transaction.
     *
     * @param request the business object data DDL collection request
     *
     * @return the business object data DDL information
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataDdlCollectionResponse generateBusinessObjectDataDdlCollection(BusinessObjectDataDdlCollectionRequest request)
    {
        return generateBusinessObjectDataDdlCollectionImpl(request);
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system to perform queries for a range of requested business object data in the
     * specified storage.
     *
     * @param request the business object data DDL request
     * @param skipRequestValidation specifies whether to skip the request validation and trimming
     *
     * @return the business object data DDL information
     */
    protected BusinessObjectDataDdl generateBusinessObjectDataDdlImpl(BusinessObjectDataDdlRequest request, boolean skipRequestValidation)
    {
        // Perform the validation.
        if (!skipRequestValidation)
        {
            validateBusinessObjectDataDdlRequest(request);
        }

        // Get the business object format entity for the specified parameters and make sure it exists.
        // Please note that when format version is not specified, we should get back the latest format version.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDaoHelper.getBusinessObjectFormatEntity(
            new BusinessObjectFormatKey(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()));

        // Validate that format has schema information.
        Assert.notEmpty(businessObjectFormatEntity.getSchemaColumns(), String.format(
            "Business object format with namespace \"%s\", business object definition name \"%s\", format usage \"%s\", format file type \"%s\"," +
                " and format version \"%s\" doesn't have schema information.",
            businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
            businessObjectFormatEntity.getFileType().getCode(), businessObjectFormatEntity.getBusinessObjectFormatVersion()));

        // If it was specified, retrieve the custom DDL and ensure it exists.
        CustomDdlEntity customDdlEntity = null;
        if (StringUtils.isNotBlank(request.getCustomDdlName()))
        {
            CustomDdlKey customDdlKey = new CustomDdlKey(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
                businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
                businessObjectFormatEntity.getFileType().getCode(), businessObjectFormatEntity.getBusinessObjectFormatVersion(), request.getCustomDdlName());
            customDdlEntity = herdDaoHelper.getCustomDdlEntity(customDdlKey);
        }

        // Validate that specified storages exist and of a proper storage platform type.
        List<String> storageNames = new ArrayList<>();
        if (StringUtils.isNotBlank(request.getStorageName()))
        {
            storageNames.add(request.getStorageName());
        }
        if (!CollectionUtils.isEmpty(request.getStorageNames()))
        {
            storageNames.addAll(request.getStorageNames());
        }
        List<StorageEntity> storageEntities = new ArrayList<>();
        for (String storageName : storageNames)
        {
            StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageName);

            // Only S3 storage platform is currently supported.
            Assert.isTrue(storageEntity.getStoragePlatform().getName().equals(StoragePlatformEntity.S3),
                String.format("Cannot generate DDL for \"%s\" storage platform.", storageEntity.getStoragePlatform().getName()));

            storageEntities.add(storageEntity);
        }

        // Validate that all storages have S3 bucket name configured.
        Map<StorageEntity, String> s3BucketNames = new HashMap<>();
        for (StorageEntity storageEntity : storageEntities)
        {
            // Please note that since S3 bucket name attribute value is required we pass a "true" flag.
            String s3BucketName = storageDaoHelper
                .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageEntity, true);
            s3BucketNames.put(storageEntity, s3BucketName);
        }

        // Create and initialize a business object data DDL object instance.
        BusinessObjectDataDdl businessObjectDataDdl = createBusinessObjectDataDdl(request);
        businessObjectDataDdl.setDdl(ddlGeneratorFactory.getDdlGenerator(request.getOutputFormat())
            .generateCreateTableDdl(request, businessObjectFormatEntity, customDdlEntity, storageNames, storageEntities, s3BucketNames));

        return businessObjectDataDdl;
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system to perform queries for a collection of business object data in the specified
     * storages.
     *
     * @param businessObjectDataDdlCollectionRequest the business object data DDL collection request
     *
     * @return the business object data DDL information
     */
    protected BusinessObjectDataDdlCollectionResponse generateBusinessObjectDataDdlCollectionImpl(
        BusinessObjectDataDdlCollectionRequest businessObjectDataDdlCollectionRequest)
    {
        // Perform the validation of the entire request, before we start processing the individual requests that requires the database access.
        validateBusinessObjectDataDdlCollectionRequest(businessObjectDataDdlCollectionRequest);

        // Process the individual requests and build the response.
        BusinessObjectDataDdlCollectionResponse businessObjectDataDdlCollectionResponse = new BusinessObjectDataDdlCollectionResponse();
        List<BusinessObjectDataDdl> businessObjectDataDdlResponses = new ArrayList<>();
        businessObjectDataDdlCollectionResponse.setBusinessObjectDataDdlResponses(businessObjectDataDdlResponses);
        List<String> ddls = new ArrayList<>();
        for (BusinessObjectDataDdlRequest request : businessObjectDataDdlCollectionRequest.getBusinessObjectDataDdlRequests())
        {
            // Please note that when calling to process individual ddl requests, we ask to skip the request validation and trimming step.
            BusinessObjectDataDdl businessObjectDataDdl = generateBusinessObjectDataDdlImpl(request, true);
            businessObjectDataDdlResponses.add(businessObjectDataDdl);
            ddls.add(businessObjectDataDdl.getDdl());
        }
        businessObjectDataDdlCollectionResponse.setDdlCollection(StringUtils.join(ddls, "\n\n"));

        return businessObjectDataDdlCollectionResponse;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BusinessObjectDataStatusInformation getBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey)
    {
        String businessObjectFormatPartitionKeyLocal = businessObjectFormatPartitionKey;

        // Validate and trim the business object data key.
        herdHelper.validateBusinessObjectDataKey(businessObjectDataKey, false, false);

        // If specified, trim the partition key parameter.
        if (businessObjectFormatPartitionKeyLocal != null)
        {
            businessObjectFormatPartitionKeyLocal = businessObjectFormatPartitionKeyLocal.trim();
        }

        // Get the business object data based on the specified parameters.
        BusinessObjectDataEntity businessObjectDataEntity = herdDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // If specified, ensure that partition key matches what's configured within the business object format.
        if (StringUtils.isNotBlank(businessObjectFormatPartitionKeyLocal))
        {
            String configuredPartitionKey = businessObjectDataEntity.getBusinessObjectFormat().getPartitionKey();
            Assert.isTrue(configuredPartitionKey.equalsIgnoreCase(businessObjectFormatPartitionKeyLocal), String
                .format("Partition key \"%s\" doesn't match configured business object format partition key \"%s\".", businessObjectFormatPartitionKeyLocal,
                    configuredPartitionKey));
        }

        // Create and return the business object data status information object.
        BusinessObjectDataStatusInformation businessObjectDataStatusInformation = new BusinessObjectDataStatusInformation();
        businessObjectDataStatusInformation.setBusinessObjectDataKey(herdDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity));
        businessObjectDataStatusInformation.setStatus(businessObjectDataEntity.getStatus().getCode());

        return businessObjectDataStatusInformation;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BusinessObjectDataStatusUpdateResponse updateBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataStatusUpdateRequest request)
    {
        // Validate and trim the business object data key.
        herdHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true);

        // Validate status
        Assert.hasText(request.getStatus(), "A business object data status must be specified.");
        request.setStatus(request.getStatus().trim());

        // Retrieve and ensure that a business object data exists with the specified key.
        BusinessObjectDataEntity businessObjectDataEntity = herdDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Get the current status value.
        String previousBusinessObjectDataStatus = businessObjectDataEntity.getStatus().getCode();

        // Update the entity with the new values.
        businessObjectDataHelper.updateBusinessObjectDataStatus(businessObjectDataEntity, request.getStatus());

        // Create and return the business object data status response object.
        BusinessObjectDataStatusUpdateResponse response = new BusinessObjectDataStatusUpdateResponse();
        response.setBusinessObjectDataKey(herdDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity));
        response.setStatus(businessObjectDataEntity.getStatus().getCode());
        response.setPreviousStatus(previousBusinessObjectDataStatus);

        return response;
    }

    /**
     * Validates a business object data availability collection request. This method also trims appropriate request parameters.
     *
     * @param businessObjectDataAvailabilityCollectionRequest the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectDataAvailabilityCollectionRequest(
        BusinessObjectDataAvailabilityCollectionRequest businessObjectDataAvailabilityCollectionRequest)
    {
        Assert.notNull(businessObjectDataAvailabilityCollectionRequest, "A business object data availability collection request must be specified.");

        Assert.isTrue(!CollectionUtils.isEmpty(businessObjectDataAvailabilityCollectionRequest.getBusinessObjectDataAvailabilityRequests()),
            "At least one business object data availability request must be specified.");

        for (BusinessObjectDataAvailabilityRequest request : businessObjectDataAvailabilityCollectionRequest.getBusinessObjectDataAvailabilityRequests())
        {
            validateBusinessObjectDataAvailabilityRequest(request);
        }
    }

    /**
     * Validates the business object data availability request. This method also trims appropriate request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectDataAvailabilityRequest(BusinessObjectDataAvailabilityRequest request)
    {
        Assert.notNull(request, "A business object data availability request must be specified.");

        // Validate and trim the request parameters.
        Assert.hasText(request.getNamespace(), "A namespace must be specified.");
        request.setNamespace(request.getNamespace().trim());

        Assert.hasText(request.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().trim());

        Assert.hasText(request.getBusinessObjectFormatUsage(), "A business object format usage name must be specified.");
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().trim());

        Assert.hasText(request.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().trim());

        // Validate the partition value filters. Allow partition value tokens to be specified.
        validatePartitionValueFilters(request.getPartitionValueFilters(), request.getPartitionValueFilter(), true);

        // Make sure that request does not contain both a list of storage names and a standalone storage name.
        Assert.isTrue(request.getStorageNames() == null || request.getStorageName() == null,
            "A list of storage names and a standalone storage name cannot be both specified.");

        // Trim the standalone storage name, if specified.
        if (request.getStorageName() != null)
        {
            Assert.hasText(request.getStorageName(), "A storage name must be specified.");
            request.setStorageName(request.getStorageName().trim());
        }

        // Validate and trim the list of storage names.
        if (!CollectionUtils.isEmpty(request.getStorageNames()))
        {
            for (int i = 0; i < request.getStorageNames().size(); i++)
            {
                Assert.hasText(request.getStorageNames().get(i), "A storage name must be specified.");
                request.getStorageNames().set(i, request.getStorageNames().get(i).trim());
            }
        }
    }

    /**
     * Validates a business object data DDL collection request. This method also trims appropriate request parameters.
     *
     * @param businessObjectDataDdlCollectionRequest the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectDataDdlCollectionRequest(BusinessObjectDataDdlCollectionRequest businessObjectDataDdlCollectionRequest)
    {
        Assert.notNull(businessObjectDataDdlCollectionRequest, "A business object data DDL collection request must be specified.");

        Assert.isTrue(!CollectionUtils.isEmpty(businessObjectDataDdlCollectionRequest.getBusinessObjectDataDdlRequests()),
            "At least one business object data DDL request must be specified.");

        for (BusinessObjectDataDdlRequest request : businessObjectDataDdlCollectionRequest.getBusinessObjectDataDdlRequests())
        {
            validateBusinessObjectDataDdlRequest(request);
        }
    }

    /**
     * Validates the business object data DDL request. This method also trims appropriate request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectDataDdlRequest(BusinessObjectDataDdlRequest request)
    {
        Assert.notNull(request, "A business object data DDL request must be specified.");

        // Validate and trim the request parameters.
        Assert.hasText(request.getNamespace(), "A namespace must be specified.");
        request.setNamespace(request.getNamespace().trim());

        Assert.hasText(request.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().trim());

        Assert.hasText(request.getBusinessObjectFormatUsage(), "A business object format usage name must be specified.");
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().trim());

        Assert.hasText(request.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().trim());

        // Validate the partition value filters. Do not allow partition value tokens to be specified.
        validatePartitionValueFilters(request.getPartitionValueFilters(), request.getPartitionValueFilter(), false);

        // Make sure that request does not contain both a list of storage names and a standalone storage name.
        Assert.isTrue(request.getStorageNames() == null || request.getStorageName() == null,
            "A list of storage names and a standalone storage name cannot be both specified.");

        // Trim the standalone storage name, if specified.
        if (request.getStorageName() != null)
        {
            Assert.hasText(request.getStorageName(), "A storage name must be specified.");
            request.setStorageName(request.getStorageName().trim());
        }

        // Validate and trim the list of storage names.
        if (!CollectionUtils.isEmpty(request.getStorageNames()))
        {
            for (int i = 0; i < request.getStorageNames().size(); i++)
            {
                Assert.hasText(request.getStorageNames().get(i), "A storage name must be specified.");
                request.getStorageNames().set(i, request.getStorageNames().get(i).trim());
            }
        }

        Assert.notNull(request.getOutputFormat(), "An output format must be specified.");

        Assert.hasText(request.getTableName(), "A table name must be specified.");
        request.setTableName(request.getTableName().trim());

        if (StringUtils.isNotBlank(request.getCustomDdlName()))
        {
            request.setCustomDdlName(request.getCustomDdlName().trim());
        }
    }

    /**
     * Validates a list of partition value filters or a standalone partition filter.  This method makes sure that a partition value filter contains exactly one
     * partition value range or a non-empty partition value list.  This method also makes sure that there is no more than one partition value range specified
     * across all partition value filters.
     *
     * @param partitionValueFilters the list of partition value filters to validate
     * @param standalonePartitionValueFilter the standalone partition value filter to validate
     * @param allowPartitionValueTokens specifies whether the partition value filter is allowed to contain partition value tokens
     */
    private void validatePartitionValueFilters(List<PartitionValueFilter> partitionValueFilters, PartitionValueFilter standalonePartitionValueFilter,
        boolean allowPartitionValueTokens)
    {
        // Make sure that request does not contain both a list of partition value filters and a standalone partition value filter.
        Assert.isTrue(partitionValueFilters == null || standalonePartitionValueFilter == null,
            "A list of partition value filters and a standalone partition value filter cannot be both specified.");

        List<PartitionValueFilter> partitionValueFiltersToValidate = new ArrayList<>();

        if (partitionValueFilters != null)
        {
            partitionValueFiltersToValidate.addAll(partitionValueFilters);
        }

        if (standalonePartitionValueFilter != null)
        {
            partitionValueFiltersToValidate.add(standalonePartitionValueFilter);
        }

        // Make sure that at least one partition value filter is specified.
        Assert.notEmpty(partitionValueFiltersToValidate, "At least one partition value filter must be specified.");

        // Validate and trim partition value filters.
        int partitionValueRangesCount = 0;
        for (PartitionValueFilter partitionValueFilter : partitionValueFiltersToValidate)
        {
            // Partition key is required when request contains a partition value filter list.
            if (partitionValueFilters != null)
            {
                Assert.hasText(partitionValueFilter.getPartitionKey(), "A partition key must be specified.");
            }

            // Trim partition key value.
            if (StringUtils.isNotBlank(partitionValueFilter.getPartitionKey()))
            {
                partitionValueFilter.setPartitionKey(partitionValueFilter.getPartitionKey().trim());
            }

            PartitionValueRange partitionValueRange = partitionValueFilter.getPartitionValueRange();
            List<String> partitionValues = partitionValueFilter.getPartitionValues();
            LatestBeforePartitionValue latestBeforePartitionValue = partitionValueFilter.getLatestBeforePartitionValue();
            LatestAfterPartitionValue latestAfterPartitionValue = partitionValueFilter.getLatestAfterPartitionValue();

            // Validate that we have exactly one partition filter option specified.
            List<Boolean> partitionFilterOptions =
                Arrays.asList(partitionValueRange != null, partitionValues != null, latestBeforePartitionValue != null, latestAfterPartitionValue != null);
            Assert.isTrue(Collections.frequency(partitionFilterOptions, Boolean.TRUE) == 1, "Exactly one partition value filter option must be specified.");

            if (partitionValueRange != null)
            {
                // A "partition value range" filter option is specified.

                // Only one partition value range is allowed across all partition value filters.
                partitionValueRangesCount++;
                Assert.isTrue(partitionValueRangesCount < 2, "Cannot specify more than one partition value range.");

                // Validate start partition value for the partition value range.
                Assert.hasText(partitionValueRange.getStartPartitionValue(), "A start partition value for the partition value range must be specified.");
                partitionValueRange.setStartPartitionValue(partitionValueRange.getStartPartitionValue().trim());

                // Validate end partition value for the partition value range.
                Assert.hasText(partitionValueRange.getEndPartitionValue(), "An end partition value for the partition value range must be specified.");
                partitionValueRange.setEndPartitionValue(partitionValueRange.getEndPartitionValue().trim());

                // Validate that partition value tokens are not specified as start and end partition values.
                // This check is required, regardless if partition value tokens are allowed or not.
                Assert.isTrue(!partitionValueRange.getStartPartitionValue().equals(MAX_PARTITION_VALUE_TOKEN) &&
                    !partitionValueRange.getStartPartitionValue().equals(MIN_PARTITION_VALUE_TOKEN) &&
                    !partitionValueRange.getEndPartitionValue().equals(MAX_PARTITION_VALUE_TOKEN) &&
                    !partitionValueRange.getEndPartitionValue().equals(MIN_PARTITION_VALUE_TOKEN),
                    "A partition value token cannot be specified with a partition value range.");

                // Using string compare, validate that start partition value is less than or equal to end partition value.
                Assert.isTrue(partitionValueRange.getStartPartitionValue().compareTo(partitionValueRange.getEndPartitionValue()) <= 0, String
                    .format("The start partition value \"%s\" cannot be greater than the end partition value \"%s\".",
                        partitionValueRange.getStartPartitionValue(), partitionValueRange.getEndPartitionValue()));
            }
            else if (partitionValues != null)
            {
                // A "partition value list" filter option is specified.

                // Validate that the list contains at least one partition value.
                Assert.isTrue(!partitionValues.isEmpty(), "At least one partition value must be specified.");

                for (int i = 0; i < partitionValues.size(); i++)
                {
                    String partitionValue = partitionValues.get(i);
                    Assert.hasText(partitionValue, "A partition value must be specified.");
                    partitionValue = partitionValue.trim();

                    // When partition value tokens are not allowed, validate that they are not specified as one of partition values.
                    if (!allowPartitionValueTokens)
                    {
                        Assert.isTrue(!partitionValue.equals(MAX_PARTITION_VALUE_TOKEN) && !partitionValue.equals(MIN_PARTITION_VALUE_TOKEN),
                            "A partition value token cannot be specified as one of partition values.");
                    }

                    partitionValues.set(i, partitionValue);
                }
            }
            else if (latestBeforePartitionValue != null)
            {
                // A "latest before partition value" filter option is specified.
                Assert.hasText(latestBeforePartitionValue.getPartitionValue(), "A partition value must be specified.");
                latestBeforePartitionValue.setPartitionValue(latestBeforePartitionValue.getPartitionValue().trim());
            }
            else
            {
                // A "latest after partition value" filter option is specified.
                Assert.hasText(latestAfterPartitionValue.getPartitionValue(), "A partition value must be specified.");
                latestAfterPartitionValue.setPartitionValue(latestAfterPartitionValue.getPartitionValue().trim());
            }
        }
    }

    /**
     * Creates business object data availability object instance and initialise it with the business object data availability request field values.
     *
     * @param request the business object data availability request
     *
     * @return the newly created BusinessObjectDataAvailability object instance
     */
    private BusinessObjectDataAvailability createBusinessObjectDataAvailability(BusinessObjectDataAvailabilityRequest request)
    {
        BusinessObjectDataAvailability businessObjectDataAvailability = new BusinessObjectDataAvailability();

        businessObjectDataAvailability.setNamespace(request.getNamespace());
        businessObjectDataAvailability.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName());
        businessObjectDataAvailability.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage());
        businessObjectDataAvailability.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType());
        businessObjectDataAvailability.setBusinessObjectFormatVersion(request.getBusinessObjectFormatVersion());

        businessObjectDataAvailability.setPartitionValueFilters(request.getPartitionValueFilters());
        businessObjectDataAvailability.setPartitionValueFilter(request.getPartitionValueFilter());

        businessObjectDataAvailability.setBusinessObjectDataVersion(request.getBusinessObjectDataVersion());

        businessObjectDataAvailability.setStorageNames(request.getStorageNames());
        businessObjectDataAvailability.setStorageName(request.getStorageName());

        return businessObjectDataAvailability;
    }

    /**
     * Creates a business object data status instance from the business object data entity.
     *
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the business object data status instance
     */
    private BusinessObjectDataStatus createAvailableBusinessObjectDataStatus(BusinessObjectDataEntity businessObjectDataEntity)
    {
        BusinessObjectDataStatus businessObjectDataStatus = new BusinessObjectDataStatus();

        businessObjectDataStatus.setBusinessObjectFormatVersion(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion());
        businessObjectDataStatus.setPartitionValue(businessObjectDataEntity.getPartitionValue());
        businessObjectDataStatus.setSubPartitionValues(herdHelper.getSubPartitionValues(businessObjectDataEntity));
        businessObjectDataStatus.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());
        businessObjectDataStatus.setReason(businessObjectDataEntity.getStatus().getCode());

        return businessObjectDataStatus;
    }

    /**
     * Creates a business object data status instance from the storage unit entity.
     *
     * @param storageUnitEntity the storage unit entity
     * @param archivedBusinessObjectDataEntities the set of archived business object data entities, not null
     *
     * @return the business object data status instance
     */
    private BusinessObjectDataStatus createNotAvailableBusinessObjectDataStatus(StorageUnitEntity storageUnitEntity,
        Set<BusinessObjectDataEntity> archivedBusinessObjectDataEntities)
    {
        // Get the business object entity.
        BusinessObjectDataEntity businessObjectDataEntity = storageUnitEntity.getBusinessObjectData();

        // Create and populate the business object data status instance.
        BusinessObjectDataStatus businessObjectDataStatus = new BusinessObjectDataStatus();

        businessObjectDataStatus.setBusinessObjectFormatVersion(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion());
        businessObjectDataStatus.setPartitionValue(businessObjectDataEntity.getPartitionValue());
        businessObjectDataStatus.setSubPartitionValues(herdHelper.getSubPartitionValues(businessObjectDataEntity));
        businessObjectDataStatus.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());

        if (storageUnitEntity.getStatus().getAvailable())
        {
            // Storage unit is "available", so business object data is selected as "non-available" due to its business object data status.
            businessObjectDataStatus.setReason(businessObjectDataEntity.getStatus().getCode());
        }
        else if (archivedBusinessObjectDataEntities.contains(storageUnitEntity.getBusinessObjectData()))
        {
            // Storage unit is not "available", but business object data is archived.
            businessObjectDataStatus.setReason(BusinessObjectDataServiceImpl.REASON_ARCHIVED);
        }
        else
        {
            businessObjectDataStatus.setReason(BusinessObjectDataServiceImpl.REASON_NO_ENABLED_STORAGE_UNIT);
        }

        return businessObjectDataStatus;
    }

    /**
     * Creates the business object data status.
     *
     * @param businessObjectDataAvailabilityRequest the business object data availability request
     * @param unmatchedPartitionFilter the partition filter that got no matched business object data instances
     * @param reason the reason for the business object data not being available
     *
     * @return the business object data status
     */
    private BusinessObjectDataStatus createNotAvailableBusinessObjectDataStatus(BusinessObjectDataAvailabilityRequest businessObjectDataAvailabilityRequest,
        List<String> unmatchedPartitionFilter, String reason)
    {
        BusinessObjectDataStatus businessObjectDataStatus = new BusinessObjectDataStatus();

        // Populate business object data status values using the business object data availability request.
        businessObjectDataStatus.setBusinessObjectFormatVersion(businessObjectDataAvailabilityRequest.getBusinessObjectFormatVersion());

        // When list of partition value filters is used, we populate primary and/or sub-partition values.
        if (businessObjectDataAvailabilityRequest.getPartitionValueFilters() != null)
        {
            // Replace all null partition values with an empty string.
            herdHelper.replaceAllNullsWithEmptyString(unmatchedPartitionFilter);

            // Populate primary and sub-partition values from the unmatched partition filter.
            businessObjectDataStatus.setPartitionValue(unmatchedPartitionFilter.get(0));
            businessObjectDataStatus.setSubPartitionValues(unmatchedPartitionFilter.subList(1, unmatchedPartitionFilter.size()));
        }
        // Otherwise, for backwards compatibility, populate primary partition value only per expected single partition value from the unmatched filter.
        else
        {
            // Since the availability request contains a standalone partition value filter,
            // the unmatched partition filter is expected to contain only a single partition value.
            for (String partitionValue : unmatchedPartitionFilter)
            {
                if (partitionValue != null)
                {
                    businessObjectDataStatus.setPartitionValue(partitionValue);
                    break;
                }
            }
        }
        businessObjectDataStatus.setBusinessObjectDataVersion(businessObjectDataAvailabilityRequest.getBusinessObjectDataVersion());
        businessObjectDataStatus.setReason(reason);

        return businessObjectDataStatus;
    }

    /**
     * Creates business object data ddl object instance and initialise it with the business object data ddl request field values.
     *
     * @param request the business object data ddl request
     *
     * @return the newly created BusinessObjectDataDdl object instance
     */
    private BusinessObjectDataDdl createBusinessObjectDataDdl(BusinessObjectDataDdlRequest request)
    {
        BusinessObjectDataDdl businessObjectDataDdl = new BusinessObjectDataDdl();

        businessObjectDataDdl.setNamespace(request.getNamespace());
        businessObjectDataDdl.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName());
        businessObjectDataDdl.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage());
        businessObjectDataDdl.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType());
        businessObjectDataDdl.setBusinessObjectFormatVersion(request.getBusinessObjectFormatVersion());

        businessObjectDataDdl.setPartitionValueFilters(request.getPartitionValueFilters());
        businessObjectDataDdl.setPartitionValueFilter(request.getPartitionValueFilter());

        businessObjectDataDdl.setBusinessObjectDataVersion(request.getBusinessObjectDataVersion());

        businessObjectDataDdl.setStorageNames(request.getStorageNames());
        businessObjectDataDdl.setStorageName(request.getStorageName());

        businessObjectDataDdl.setOutputFormat(request.getOutputFormat());
        businessObjectDataDdl.setTableName(request.getTableName());
        businessObjectDataDdl.setCustomDdlName(request.getCustomDdlName());

        return businessObjectDataDdl;
    }

    /**
     * Delegates implementation to {@link org.finra.herd.service.helper.BusinessObjectDataInvalidateUnregisteredHelper}. Starts a new transaction. Meant for
     * Activiti wrapper usage.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataInvalidateUnregisteredResponse invalidateUnregisteredBusinessObjectData(
        BusinessObjectDataInvalidateUnregisteredRequest businessObjectDataInvalidateUnregisteredRequest)
    {
        return invalidateUnregisteredBusinessObjectDataImpl(businessObjectDataInvalidateUnregisteredRequest);
    }

    /**
     * Delegates implementation to {@link org.finra.herd.service.helper.BusinessObjectDataInvalidateUnregisteredHelper}. Keeps current transaction context.
     *
     * @param businessObjectDataInvalidateUnregisteredRequest {@link org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest}
     *
     * @return {@link BusinessObjectDataInvalidateUnregisteredResponse}
     */
    protected BusinessObjectDataInvalidateUnregisteredResponse invalidateUnregisteredBusinessObjectDataImpl(
        BusinessObjectDataInvalidateUnregisteredRequest businessObjectDataInvalidateUnregisteredRequest)
    {
        return businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(businessObjectDataInvalidateUnregisteredRequest);
    }

    @Override
    public BusinessObjectDataUploadCredential getBusinessObjectDataUploadCredential(BusinessObjectDataKey businessObjectDataKey, Boolean createNewVersion,
        String storageName)
    {
        BusinessObjectDataUploadCredential businessObjectDataUploadCredential = new BusinessObjectDataUploadCredential();
        businessObjectDataUploadCredential.setAwsCredential(getBusinessObjectDataS3Credential(businessObjectDataKey, createNewVersion, storageName, true));
        businessObjectDataUploadCredential.setAwsKmsKeyId(getStorageKmsKeyId(storageDaoHelper.getStorageEntity(storageName.trim())));
        return businessObjectDataUploadCredential;
    }

    @Override
    public BusinessObjectDataDownloadCredential getBusinessObjectDataDownloadCredential(BusinessObjectDataKey businessObjectDataKey, String storageName)
    {
        BusinessObjectDataDownloadCredential businessObjectDataDownloadCredential = new BusinessObjectDataDownloadCredential();
        businessObjectDataDownloadCredential.setAwsCredential(getBusinessObjectDataS3Credential(businessObjectDataKey, null, storageName, false));
        return businessObjectDataDownloadCredential;
    }

    /**
     * Creates and returns a set of AWS credentials which can be used to access the S3 object indicated by the given business object data and storage.
     *
     * @param businessObjectDataKey Business object data key
     * @param createNewVersion true to create credentials for the next version up from the latest business object data, otherwise, uses specified data version
     * in data key.
     * @param storageName Name of storage to access
     * @param isUpload true if this credential is to upload, false to download
     *
     * @return Credentials which has the permissions to perform the specified actions at the specified storage.
     */
    private AwsCredential getBusinessObjectDataS3Credential(BusinessObjectDataKey businessObjectDataKey, Boolean createNewVersion, String storageName,
        boolean isUpload)
    {
        Assert.isTrue(StringUtils.isNotBlank(storageName), "storageName must be specified");
        Assert.isTrue(businessObjectDataKey.getBusinessObjectDataVersion() != null || createNewVersion != null,
            "One of businessObjectDataVersion or createNewVersion must be specified.");
        Assert.isTrue(businessObjectDataKey.getBusinessObjectDataVersion() == null || !Boolean.TRUE.equals(createNewVersion),
            "createNewVersion must be false or unspecified when businessObjectDataVersion is specified.");

        /*
         * Choose configurations based on whether this is an upload or download operation.
         */
        ConfigurationValue roleArnConfigurationValue;
        ConfigurationValue defaultSessionDurationConfigurationValue;
        ConfigurationValue sessionDurationConfigurationValue;
        S3Actions[] s3Actions;
        KmsActions[] kmsActions;

        if (isUpload)
        {
            roleArnConfigurationValue = ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN;
            defaultSessionDurationConfigurationValue = ConfigurationValue.AWS_S3_DEFAULT_UPLOAD_SESSION_DURATION_SECS;
            sessionDurationConfigurationValue = ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_SESSION_DURATION_SECS;
            s3Actions = new S3Actions[] {S3Actions.PutObject, S3Actions.DeleteObject};
            kmsActions = new KmsActions[] {KmsActions.GENERATE_DATA_KEY, KmsActions.DECRYPT};
        }
        else
        {
            roleArnConfigurationValue = ConfigurationValue.S3_ATTRIBUTE_NAME_DOWNLOAD_ROLE_ARN;
            defaultSessionDurationConfigurationValue = ConfigurationValue.AWS_S3_DEFAULT_DOWNLOAD_SESSION_DURATION_SECS;
            sessionDurationConfigurationValue = ConfigurationValue.S3_ATTRIBUTE_NAME_DOWNLOAD_SESSION_DURATION_SECS;
            s3Actions = new S3Actions[] {S3Actions.GetObject};
            kmsActions = new KmsActions[] {KmsActions.DECRYPT};
        }

        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageName.trim());
        String roleArn = storageDaoHelper.getStorageAttributeValueByName(configurationHelper.getProperty(roleArnConfigurationValue), storageEntity, true);
        Integer durationSeconds = storageDaoHelper
            .getStorageAttributeIntegerValueByName(configurationHelper.getProperty(sessionDurationConfigurationValue), storageEntity,
                configurationHelper.getProperty(defaultSessionDurationConfigurationValue, Integer.class));
        String bucketName = storageDaoHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageEntity, true);

        S3KeyPrefixInformation s3KeyPrefixInformation = getS3KeyPrefixImpl(businessObjectDataKey, null, createNewVersion);
        /*
         * Policy is different based on whether this is meant for downloading or uploading.
         * However, both uploader and downloader requires a ListBucket at the bucket level.
         */
        AwsPolicyBuilder awsPolicyBuilder =
            new AwsPolicyBuilder().withS3Prefix(bucketName, s3KeyPrefixInformation.getS3KeyPrefix(), s3Actions).withS3(bucketName, null, S3Actions.ListObjects);

        /*
         * Only add KMS policies if the storage specifies a KMS ID
         */
        String kmsKeyId = getStorageKmsKeyId(storageEntity);
        if (kmsKeyId != null)
        {
            awsPolicyBuilder.withKms(kmsKeyId.trim(), kmsActions);
        }

        Credentials credentials = stsDao
            .getTemporarySecurityCredentials(awsHelper.getAwsParamsDto(), businessObjectDataKey.getNamespace(), roleArn, durationSeconds,
                awsPolicyBuilder.build());

        AwsCredential awsCredential = new AwsCredential();
        awsCredential.setAwsAccessKey(credentials.getAccessKeyId());
        awsCredential.setAwsSecretKey(credentials.getSecretAccessKey());
        awsCredential.setAwsSessionToken(credentials.getSessionToken());
        awsCredential.setAwsSessionExpirationTime(HerdDateUtils.getXMLGregorianCalendarValue(credentials.getExpiration()));
        return awsCredential;
    }

    private String getStorageKmsKeyId(StorageEntity storageEntity)
    {
        return storageDaoHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID), storageEntity, false, true);
    }
}
