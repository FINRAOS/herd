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

import java.util.Date;
import java.util.UUID;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.commons.collections4.IterableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.StsDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.PublishJmsMessages;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.DownloadSingleInitiationResponse;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.UploadSingleCredentialExtensionResponse;
import org.finra.herd.model.api.xml.UploadSingleInitiationRequest;
import org.finra.herd.model.api.xml.UploadSingleInitiationResponse;
import org.finra.herd.model.dto.CompleteUploadSingleParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.UploadDownloadHelperService;
import org.finra.herd.service.UploadDownloadService;
import org.finra.herd.service.helper.AttributeHelper;
import org.finra.herd.service.helper.AwsPolicyBuilder;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.KmsActions;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;

/**
 * The upload download service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class UploadDownloadServiceImpl implements UploadDownloadService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadDownloadServiceImpl.class);

    @Autowired
    private AttributeHelper attributeHelper;

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Autowired
    private StsDao stsDao;

    @Autowired
    private UploadDownloadHelperService uploadDownloadHelperService;

    @PublishJmsMessages
    @NamespacePermission(fields = {"#uploadSingleInitiationRequest?.sourceBusinessObjectFormatKey?.namespace",
        "#uploadSingleInitiationRequest?.targetBusinessObjectFormatKey?.namespace"}, permissions = NamespacePermissionEnum.WRITE)
    @Override
    public UploadSingleInitiationResponse initiateUploadSingle(UploadSingleInitiationRequest uploadSingleInitiationRequest)
    {
        // Validate and trim the request parameters.
        validateUploadSingleInitiationRequest(uploadSingleInitiationRequest);

        // Get the business object format for the specified parameters and make sure it exists.
        BusinessObjectFormatEntity sourceBusinessObjectFormatEntity =
            businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(uploadSingleInitiationRequest.getSourceBusinessObjectFormatKey());

        // Get the target business object format entity for the specified parameters and make sure it exists.
        BusinessObjectFormatEntity targetBusinessObjectFormatEntity =
            businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(uploadSingleInitiationRequest.getTargetBusinessObjectFormatKey());

        // Get the S3 managed "loading dock" storage entity and make sure it exists.
        StorageEntity sourceStorageEntity = storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE);

        // Get S3 bucket name for the storage. Please note that since those values are required we pass a "true" flag.
        String s3BucketName = storageHelper.getStorageBucketName(sourceStorageEntity);

        // Get the S3 managed "external" storage entity and make sure it exists.
        String targetStorageName;
        if (uploadSingleInitiationRequest.getTargetStorageName() != null)
        {
            targetStorageName = uploadSingleInitiationRequest.getTargetStorageName();
        }
        else
        {
            targetStorageName = configurationHelper.getProperty(ConfigurationValue.S3_EXTERNAL_STORAGE_NAME_DEFAULT);
        }
        StorageEntity targetStorageEntity = storageDaoHelper.getStorageEntity(targetStorageName);

        assertTargetStorageEntityValid(targetStorageEntity);

        // Generate a random UUID value.
        String uuid = UUID.randomUUID().toString();

        // Create business object data key with partition value set to the generated UUID.
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey(uploadSingleInitiationRequest.getSourceBusinessObjectFormatKey().getNamespace(),
            uploadSingleInitiationRequest.getSourceBusinessObjectFormatKey().getBusinessObjectDefinitionName(),
            uploadSingleInitiationRequest.getSourceBusinessObjectFormatKey().getBusinessObjectFormatUsage(),
            uploadSingleInitiationRequest.getSourceBusinessObjectFormatKey().getBusinessObjectFormatFileType(),
            uploadSingleInitiationRequest.getSourceBusinessObjectFormatKey().getBusinessObjectFormatVersion(), uuid, null,
            BusinessObjectDataEntity.BUSINESS_OBJECT_DATA_INITIAL_VERSION);

        // Get a file upload specific S3 key prefix for the source storage based on the generated UUID.
        String sourceStorageDirectoryPath = s3KeyPrefixHelper.buildS3KeyPrefix(sourceStorageEntity, sourceBusinessObjectFormatEntity, businessObjectDataKey);
        String sourceStorageFilePath = String.format("%s/%s", sourceStorageDirectoryPath, uploadSingleInitiationRequest.getFile().getFileName());

        // Create a business object data create request.
        BusinessObjectDataCreateRequest sourceBusinessObjectDataCreateRequest = businessObjectDataHelper
            .createBusinessObjectDataCreateRequest(sourceBusinessObjectFormatEntity, uuid, BusinessObjectDataStatusEntity.UPLOADING,
                uploadSingleInitiationRequest.getBusinessObjectDataAttributes(), sourceStorageEntity, sourceStorageDirectoryPath, sourceStorageFilePath,
                uploadSingleInitiationRequest.getFile().getFileSizeBytes(), null);

        // Create a new business object data instance. Set the flag to false, since for the file upload service the file size value is optional.
        BusinessObjectData sourceBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(sourceBusinessObjectDataCreateRequest, false);

        // Get a file upload specific S3 key prefix for the target storage based on the generated UUID.
        String targetStorageDirectoryPath = s3KeyPrefixHelper.buildS3KeyPrefix(targetStorageEntity, targetBusinessObjectFormatEntity, businessObjectDataKey);
        String targetStorageFilePath = String.format("%s/%s", targetStorageDirectoryPath, uploadSingleInitiationRequest.getFile().getFileName());

        uploadDownloadHelperService.assertS3ObjectKeyDoesNotExist(storageHelper.getStorageBucketName(targetStorageEntity), targetStorageFilePath);

        // Create a target business object data based on the source business object data and target business object format.
        BusinessObjectDataCreateRequest targetBusinessObjectDataCreateRequest = businessObjectDataHelper
            .createBusinessObjectDataCreateRequest(targetBusinessObjectFormatEntity, uuid, BusinessObjectDataStatusEntity.UPLOADING,
                uploadSingleInitiationRequest.getBusinessObjectDataAttributes(), targetStorageEntity, targetStorageDirectoryPath, targetStorageFilePath,
                uploadSingleInitiationRequest.getFile().getFileSizeBytes(), null);

        // Create a target business object data instance. Set the flag to false, since for the file upload service the file size value is optional.
        BusinessObjectData targetBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(targetBusinessObjectDataCreateRequest, false);

        // Get decrypted AWS ARN of the role that is required to provide access to S3_MANAGED_LOADING_DOCK storage.
        String awsRoleArn = getStorageUploadRoleArn(sourceStorageEntity);

        // Get expiration interval for the pre-signed URL to be generated.
        Integer awsRoleDurationSeconds = getStorageUploadSessionDuration(sourceStorageEntity);

        String awsKmsKeyId = storageHelper.getStorageKmsKeyId(sourceStorageEntity);

        // Get the temporary security credentials to access S3_MANAGED_STORAGE.
        Credentials assumedSessionCredentials = stsDao
            .getTemporarySecurityCredentials(awsHelper.getAwsParamsDto(), String.valueOf(sourceBusinessObjectData.getId()), awsRoleArn, awsRoleDurationSeconds,
                createUploaderPolicy(s3BucketName, sourceStorageFilePath, awsKmsKeyId));

        // Create the response.
        UploadSingleInitiationResponse response = new UploadSingleInitiationResponse();
        response.setSourceBusinessObjectData(sourceBusinessObjectData);
        response.setTargetBusinessObjectData(targetBusinessObjectData);
        response.setFile(uploadSingleInitiationRequest.getFile());
        response.setUuid(uuid);
        response.setAwsAccessKey(assumedSessionCredentials.getAccessKeyId());
        response.setAwsSecretKey(assumedSessionCredentials.getSecretAccessKey());
        response.setAwsSessionToken(assumedSessionCredentials.getSessionToken());
        response.setAwsSessionExpirationTime(HerdDateUtils.getXMLGregorianCalendarValue(assumedSessionCredentials.getExpiration()));
        response.setAwsKmsKeyId(awsKmsKeyId);
        response.setTargetStorageName(targetStorageName);

        return response;
    }

    /**
     * Asserts that the given target storage entity has valid attributes.
     *
     * @param targetStorageEntity Target storage entity.
     */
    private void assertTargetStorageEntityValid(StorageEntity targetStorageEntity)
    {
        try
        {
            // Assert that the target storage has a bucket name
            storageHelper.getStorageBucketName(targetStorageEntity);
        }
        catch (IllegalStateException e)
        {
            throw new IllegalArgumentException(e.getMessage(), e);
        }

        try
        {
            // Assert that the target storage has a KMS key ID
            storageHelper.getStorageKmsKeyId(targetStorageEntity);
        }
        catch (IllegalStateException e)
        {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * Creates a restricted policy JSON string which only allows PutObject to the given bucket name and object key, and allows GenerateDataKey and Decrypt for
     * the given key ID. The Decrypt is required for multipart upload with KMS encryption.
     *
     * @param s3BucketName - The S3 bucket name to restrict uploads to
     * @param s3Key - The S3 object key to restrict the uploads to
     * @param awsKmsKeyId - The KMS key ID to allow access
     *
     * @return the policy JSON string
     */
    @SuppressWarnings("PMD.CloseResource") // These are not SQL statements so they don't need to be closed.
    private Policy createUploaderPolicy(String s3BucketName, String s3Key, String awsKmsKeyId)
    {
        return new AwsPolicyBuilder().withS3(s3BucketName, s3Key, S3Actions.PutObject).withKms(awsKmsKeyId, KmsActions.GENERATE_DATA_KEY, KmsActions.DECRYPT)
            .build();
    }

    /**
     * Creates a restricted policy JSON string which only allows GetObject to the given bucket name and object key, and allows Decrypt for the given key ID.
     *
     * @param s3BucketName - The S3 bucket name to restrict uploads to
     * @param s3Key - The S3 object key to restrict the uploads to
     * @param awsKmsKeyId - The KMS key ID to allow access
     *
     * @return the policy JSON string
     */
    @SuppressWarnings("PMD.CloseResource") // These are not SQL statements so they don't need to be closed.
    private Policy createDownloaderPolicy(String s3BucketName, String s3Key, String awsKmsKeyId)
    {
        return new AwsPolicyBuilder().withS3(s3BucketName, s3Key, S3Actions.GetObject).withKms(awsKmsKeyId, KmsActions.DECRYPT).build();
    }

    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public CompleteUploadSingleMessageResult performCompleteUploadSingleMessage(String objectKey)
    {
        return performCompleteUploadSingleMessageImpl(objectKey);
    }

    /**
     * Performs the completion of upload single file. Runs in new transaction and logs the error if an error occurs.
     *
     * @param objectKey the object key.
     *
     * @return CompleteUploadSingleMessageResult
     */
    protected CompleteUploadSingleMessageResult performCompleteUploadSingleMessageImpl(String objectKey)
    {
        // Create an instance of the result message for complete upload single operation.
        CompleteUploadSingleMessageResult completeUploadSingleMessageResult = new CompleteUploadSingleMessageResult();

        // Create an instance of complete upload single parameters DTO.
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto = new CompleteUploadSingleParamsDto();

        // Prepare for the file move.
        uploadDownloadHelperService.prepareForFileMove(objectKey, completeUploadSingleParamsDto);

        // Update the result message.
        completeUploadSingleMessageResult.setSourceBusinessObjectDataKey(completeUploadSingleParamsDto.getSourceBusinessObjectDataKey());
        completeUploadSingleMessageResult.setSourceOldBusinessObjectDataStatus(completeUploadSingleParamsDto.getSourceOldStatus());
        completeUploadSingleMessageResult.setSourceNewBusinessObjectDataStatus(completeUploadSingleParamsDto.getSourceNewStatus());
        completeUploadSingleMessageResult.setTargetBusinessObjectDataKey(completeUploadSingleParamsDto.getTargetBusinessObjectDataKey());
        completeUploadSingleMessageResult.setTargetOldBusinessObjectDataStatus(completeUploadSingleParamsDto.getTargetOldStatus());
        completeUploadSingleMessageResult.setTargetNewBusinessObjectDataStatus(completeUploadSingleParamsDto.getTargetNewStatus());

        // If both source and target business object data have RE-ENCRYPTING status, continue the processing.
        if (BusinessObjectDataStatusEntity.RE_ENCRYPTING.equals(completeUploadSingleParamsDto.getSourceNewStatus()) &&
            BusinessObjectDataStatusEntity.RE_ENCRYPTING.equals(completeUploadSingleParamsDto.getTargetNewStatus()))
        {
            // Move the S3 file from the source to the target bucket.
            uploadDownloadHelperService.performFileMove(completeUploadSingleParamsDto);

            // Execute the steps required to complete the processing of the complete upload single message.
            uploadDownloadHelperService.executeFileMoveAfterSteps(completeUploadSingleParamsDto);

            // Update the result message.
            completeUploadSingleMessageResult.setSourceNewBusinessObjectDataStatus(completeUploadSingleParamsDto.getSourceNewStatus());
            completeUploadSingleMessageResult.setTargetNewBusinessObjectDataStatus(completeUploadSingleParamsDto.getTargetNewStatus());
        }

        // Log the result of the complete upload single operation.
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("completeUploadSingleMessageResult={}", jsonHelper.objectToJson(completeUploadSingleMessageResult));
        }

        return completeUploadSingleMessageResult;
    }

    /*
     * The result of completeUploadSingleMessage, contains the source and target business object data key, old and new status.
     */
    public static class CompleteUploadSingleMessageResult
    {
        private BusinessObjectDataKey sourceBusinessObjectDataKey;

        private String sourceOldBusinessObjectDataStatus;

        private String sourceNewBusinessObjectDataStatus;

        private BusinessObjectDataKey targetBusinessObjectDataKey;

        private String targetOldBusinessObjectDataStatus;

        private String targetNewBusinessObjectDataStatus;

        public BusinessObjectDataKey getSourceBusinessObjectDataKey()
        {
            return sourceBusinessObjectDataKey;
        }

        public void setSourceBusinessObjectDataKey(BusinessObjectDataKey sourceBusinessObjectDataKey)
        {
            this.sourceBusinessObjectDataKey = sourceBusinessObjectDataKey;
        }

        public String getSourceOldBusinessObjectDataStatus()
        {
            return sourceOldBusinessObjectDataStatus;
        }

        public void setSourceOldBusinessObjectDataStatus(String sourceOldBusinessObjectDataStatus)
        {
            this.sourceOldBusinessObjectDataStatus = sourceOldBusinessObjectDataStatus;
        }

        public String getSourceNewBusinessObjectDataStatus()
        {
            return sourceNewBusinessObjectDataStatus;
        }

        public void setSourceNewBusinessObjectDataStatus(String sourceNewBusinessObjectDataStatus)
        {
            this.sourceNewBusinessObjectDataStatus = sourceNewBusinessObjectDataStatus;
        }

        public BusinessObjectDataKey getTargetBusinessObjectDataKey()
        {
            return targetBusinessObjectDataKey;
        }

        public void setTargetBusinessObjectDataKey(BusinessObjectDataKey targetBusinessObjectDataKey)
        {
            this.targetBusinessObjectDataKey = targetBusinessObjectDataKey;
        }

        public String getTargetOldBusinessObjectDataStatus()
        {
            return targetOldBusinessObjectDataStatus;
        }

        public void setTargetOldBusinessObjectDataStatus(String targetOldBusinessObjectDataStatus)
        {
            this.targetOldBusinessObjectDataStatus = targetOldBusinessObjectDataStatus;
        }

        public String getTargetNewBusinessObjectDataStatus()
        {
            return targetNewBusinessObjectDataStatus;
        }

        public void setTargetNewBusinessObjectDataStatus(String targetNewBusinessObjectDataStatus)
        {
            this.targetNewBusinessObjectDataStatus = targetNewBusinessObjectDataStatus;
        }
    }

    /**
     * Validates the upload single initiation request. This method also trims the request parameters.
     *
     * @param request the upload single initiation request
     */
    private void validateUploadSingleInitiationRequest(UploadSingleInitiationRequest request)
    {
        Assert.notNull(request, "An upload single initiation request must be specified.");

        // Validate and trim the source business object format key.
        businessObjectFormatHelper.validateBusinessObjectFormatKey(request.getSourceBusinessObjectFormatKey());

        // Validate and trim the target business object format key.
        businessObjectFormatHelper.validateBusinessObjectFormatKey(request.getTargetBusinessObjectFormatKey());

        // Validate and trim the attributes.
        attributeHelper.validateAttributes(request.getBusinessObjectDataAttributes());

        // Validate and trim the file information.
        Assert.notNull(request.getFile(), "File information must be specified.");
        Assert.hasText(request.getFile().getFileName(), "A file name must be specified.");
        request.getFile().setFileName(request.getFile().getFileName().trim());

        String targetStorageName = request.getTargetStorageName();
        if (targetStorageName != null)
        {
            request.setTargetStorageName(targetStorageName.trim());
        }
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public DownloadSingleInitiationResponse initiateDownloadSingle(String namespace, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue, Integer businessObjectDataVersion)
    {
        // Create the business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, null, businessObjectDataVersion);

        // Validate the parameters
        businessObjectDataHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true);

        // Retrieve the persisted business object data
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Make sure the status of the business object data is VALID
        businessObjectDataHelper.assertBusinessObjectDataStatusEquals(BusinessObjectDataStatusEntity.VALID, businessObjectDataEntity);

        // Get the external storage registered against this data
        // Validate that the storage unit exists
        StorageUnitEntity storageUnitEntity = IterableUtils.get(businessObjectDataEntity.getStorageUnits(), 0);

        // Validate that the storage unit contains only 1 file
        assertHasOneStorageFile(storageUnitEntity);

        String s3BucketName = storageHelper.getStorageBucketName(storageUnitEntity.getStorage());
        String s3ObjectKey = IterableUtils.get(storageUnitEntity.getStorageFiles(), 0).getPath();

        // Get the temporary credentials
        Credentials downloaderCredentials =
            getExternalDownloaderCredentials(storageUnitEntity.getStorage(), String.valueOf(businessObjectDataEntity.getId()), s3ObjectKey);

        // Generate a pre-signed URL
        Date expiration = downloaderCredentials.getExpiration();
        S3FileTransferRequestParamsDto s3BucketAccessParams = storageHelper.getS3BucketAccessParams(storageUnitEntity.getStorage());
        String presignedUrl = s3Dao.generateGetObjectPresignedUrl(s3BucketName, s3ObjectKey, expiration, s3BucketAccessParams);

        // Construct and return the response
        DownloadSingleInitiationResponse response = new DownloadSingleInitiationResponse();
        response.setBusinessObjectData(businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity));
        response.setAwsAccessKey(downloaderCredentials.getAccessKeyId());
        response.setAwsSecretKey(downloaderCredentials.getSecretAccessKey());
        response.setAwsSessionToken(downloaderCredentials.getSessionToken());
        response.setAwsSessionExpirationTime(HerdDateUtils.getXMLGregorianCalendarValue(expiration));
        response.setPreSignedUrl(presignedUrl);
        return response;
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public UploadSingleCredentialExtensionResponse extendUploadSingleCredentials(String namespace, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue,
        Integer businessObjectDataVersion)
    {
        // Create the business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, null, businessObjectDataVersion);

        // Get the business object data for the key.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Ensure the status of the business object data is "uploading" in order to extend credentials.
        if (!(businessObjectDataEntity.getStatus().getCode().equals(BusinessObjectDataStatusEntity.UPLOADING)))
        {
            throw new IllegalArgumentException(String.format(String
                .format("Business object data {%s} has a status of \"%s\" and must be \"%s\" to extend " + "credentials.",
                    businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey), businessObjectDataEntity.getStatus().getCode(),
                    BusinessObjectDataStatusEntity.UPLOADING)));
        }

        // Get the S3 managed "loading dock" storage entity and make sure it exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE);

        String s3BucketName = storageHelper.getStorageBucketName(storageEntity);

        // Get the storage unit entity for this business object data in the S3 managed "loading dock" storage and make sure it exists.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, businessObjectDataEntity);

        // Validate that the storage unit contains exactly one storage file.
        assertHasOneStorageFile(storageUnitEntity);

        // Get the storage file entity.
        StorageFileEntity storageFileEntity = IterableUtils.get(storageUnitEntity.getStorageFiles(), 0);

        // Get the storage file path.
        String storageFilePath = storageFileEntity.getPath();

        String awsRoleArn = getStorageUploadRoleArn(storageEntity);

        Integer awsRoleDurationSeconds = getStorageUploadSessionDuration(storageEntity);

        String awsKmsKeyId = storageHelper.getStorageKmsKeyId(storageEntity);

        // Get the temporary security credentials to access S3_MANAGED_STORAGE.
        Credentials assumedSessionCredentials = stsDao
            .getTemporarySecurityCredentials(awsHelper.getAwsParamsDto(), String.valueOf(businessObjectDataEntity.getId()), awsRoleArn, awsRoleDurationSeconds,
                createUploaderPolicy(s3BucketName, storageFilePath, awsKmsKeyId));

        // Create the response.
        UploadSingleCredentialExtensionResponse response = new UploadSingleCredentialExtensionResponse();
        response.setAwsAccessKey(assumedSessionCredentials.getAccessKeyId());
        response.setAwsSecretKey(assumedSessionCredentials.getSecretAccessKey());
        response.setAwsSessionToken(assumedSessionCredentials.getSessionToken());
        response.setAwsSessionExpirationTime(HerdDateUtils.getXMLGregorianCalendarValue(assumedSessionCredentials.getExpiration()));

        return response;
    }

    /**
     * Asserts that the given storage unit entity contains exactly one storage file.
     *
     * @param storageUnitEntity - storage unit to check
     *
     * @throws IllegalArgumentException when the number of storage files is not 1
     */
    private void assertHasOneStorageFile(StorageUnitEntity storageUnitEntity)
    {
        Assert.isTrue(storageUnitEntity.getStorageFiles().size() == 1, String
            .format("Found %d registered storage files when expecting one in \"%s\" storage for the business object data {%s}.",
                storageUnitEntity.getStorageFiles().size(), storageUnitEntity.getStorage().getName(),
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(storageUnitEntity.getBusinessObjectData())));
    }

    /**
     * Gets a temporary session token that is only good for downloading the specified object key from the given bucket for a limited amount of time.
     *
     * @param storageEntity The storage entity of the external storage
     * @param sessionName the session name to use for the temporary credentials.
     * @param s3ObjectKey the S3 object key of the path to the data in the bucket.
     *
     * @return {@link Credentials} temporary session token
     */
    private Credentials getExternalDownloaderCredentials(StorageEntity storageEntity, String sessionName, String s3ObjectKey)
    {
        return stsDao.getTemporarySecurityCredentials(awsHelper.getAwsParamsDto(), sessionName, getStorageDownloadRoleArn(storageEntity),
            getStorageDownloadSessionDuration(storageEntity),
            createDownloaderPolicy(storageHelper.getStorageBucketName(storageEntity), s3ObjectKey, storageHelper.getStorageKmsKeyId(storageEntity)));
    }

    /**
     * Gets the storage's upload session duration in seconds. Defaults to the configured default value if not defined.
     *
     * @param storageEntity The storage entity
     *
     * @return Upload session duration in seconds
     */
    private Integer getStorageUploadSessionDuration(StorageEntity storageEntity)
    {
        return storageHelper
            .getStorageAttributeIntegerValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_SESSION_DURATION_SECS),
                storageEntity, configurationHelper.getProperty(ConfigurationValue.AWS_S3_DEFAULT_UPLOAD_SESSION_DURATION_SECS, Integer.class));
    }

    /**
     * Gets the storage's upload role ARN. Throws if not defined.
     *
     * @param storageEntity The storage entity
     *
     * @return Upload role ARN
     */
    private String getStorageUploadRoleArn(StorageEntity storageEntity)
    {
        return storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN), storageEntity, true);
    }

    /**
     * Gets the storage's download session duration in seconds. Defaults to the configured default value if not defined.
     *
     * @param storageEntity The storage entity
     *
     * @return Download session duration in seconds
     */
    private Integer getStorageDownloadSessionDuration(StorageEntity storageEntity)
    {
        return storageHelper
            .getStorageAttributeIntegerValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_DOWNLOAD_SESSION_DURATION_SECS),
                storageEntity, configurationHelper.getProperty(ConfigurationValue.AWS_S3_DEFAULT_DOWNLOAD_SESSION_DURATION_SECS, Integer.class));
    }

    /**
     * Gets the storage's download role ARN. Throws if not defined.
     *
     * @param storageEntity The storage entity
     *
     * @return Download role ARN
     */
    private String getStorageDownloadRoleArn(StorageEntity storageEntity)
    {
        return storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_DOWNLOAD_ROLE_ARN), storageEntity, true);
    }
}
