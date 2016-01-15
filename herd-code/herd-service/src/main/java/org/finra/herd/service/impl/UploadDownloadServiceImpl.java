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
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.StsDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.DownloadSingleInitiationResponse;
import org.finra.herd.model.api.xml.UploadSingleCredentialExtensionResponse;
import org.finra.herd.model.api.xml.UploadSingleInitiationRequest;
import org.finra.herd.model.api.xml.UploadSingleInitiationResponse;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.UploadDownloadAsyncService;
import org.finra.herd.service.UploadDownloadHelperService;
import org.finra.herd.service.UploadDownloadService;
import org.finra.herd.service.helper.AwsPolicyBuilder;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.HerdDaoHelper;
import org.finra.herd.service.helper.HerdHelper;
import org.finra.herd.service.helper.KmsActions;
import org.finra.herd.service.helper.StorageDaoHelper;

/**
 * The upload download service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class UploadDownloadServiceImpl implements UploadDownloadService
{
    private static final Logger LOGGER = Logger.getLogger(UploadDownloadServiceImpl.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private HerdHelper herdHelper;

    @Autowired
    private HerdDaoHelper herdDaoHelper;

    @Autowired
    private StsDao stsDao;

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private UploadDownloadHelperService uploadDownloadHelperService;

    @Autowired
    private UploadDownloadAsyncService uploadDownloadAsyncService;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    /**
     * {@inheritDoc}
     */
    @Override
    public UploadSingleInitiationResponse initiateUploadSingle(UploadSingleInitiationRequest uploadSingleInitiationRequest)
    {
        // Validate and trim the request parameters.
        validateUploadSingleInitiationRequest(uploadSingleInitiationRequest);

        // Get the business object format for the specified parameters and make sure it exists.
        BusinessObjectFormatEntity sourceBusinessObjectFormatEntity =
            herdDaoHelper.getBusinessObjectFormatEntity(uploadSingleInitiationRequest.getSourceBusinessObjectFormatKey());

        // Get the target business object format entity for the specified parameters and make sure it exists.
        BusinessObjectFormatEntity targetBusinessObjectFormatEntity =
            herdDaoHelper.getBusinessObjectFormatEntity(uploadSingleInitiationRequest.getTargetBusinessObjectFormatKey());

        // Get the S3 managed "loading dock" storage entity and make sure it exists.
        StorageEntity sourceStorageEntity = storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE);

        // Get S3 bucket name for the storage. Please note that since those values are required we pass a "true" flag.
        String s3BucketName = getStorageBucketName(sourceStorageEntity);

        // Get the S3 managed "external" storage entity and make sure it exists.
        StorageEntity targetStorageEntity = storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE);

        // Generate a random UUID value.
        String uuid = UUID.randomUUID().toString();

        // Create source business object data key with partition value set to the generated UUID.
        BusinessObjectDataKey sourceBusinessObjectDataKey =
            new BusinessObjectDataKey(uploadSingleInitiationRequest.getSourceBusinessObjectFormatKey().getNamespace(),
                uploadSingleInitiationRequest.getSourceBusinessObjectFormatKey().getBusinessObjectDefinitionName(),
                uploadSingleInitiationRequest.getSourceBusinessObjectFormatKey().getBusinessObjectFormatUsage(),
                uploadSingleInitiationRequest.getSourceBusinessObjectFormatKey().getBusinessObjectFormatFileType(),
                uploadSingleInitiationRequest.getSourceBusinessObjectFormatKey().getBusinessObjectFormatVersion(), uuid, null,
                BusinessObjectDataEntity.BUSINESS_OBJECT_DATA_INITIAL_VERSION);

        // Get a file upload specific S3 key prefix based on the generated UUID.
        String storageDirectoryPath = businessObjectDataHelper.buildFileUploadS3KeyPrefix(sourceBusinessObjectFormatEntity, sourceBusinessObjectDataKey);
        String storageFilePath = String.format("%s/%s", storageDirectoryPath, uploadSingleInitiationRequest.getFile().getFileName());

        // Create a business object data create request.
        BusinessObjectDataCreateRequest sourceBusinessObjectDataCreateRequest = businessObjectDataHelper
            .createBusinessObjectDataCreateRequest(sourceBusinessObjectFormatEntity, uuid, BusinessObjectDataStatusEntity.UPLOADING,
                uploadSingleInitiationRequest.getBusinessObjectDataAttributes(), sourceStorageEntity, storageDirectoryPath, storageFilePath,
                uploadSingleInitiationRequest.getFile().getFileSizeBytes(), null);

        // Create a new business object data instance. Set the flag to false, since for the file upload service the file size value is optional.
        BusinessObjectData sourceBusinessObjectData = businessObjectDataHelper.createBusinessObjectData(sourceBusinessObjectDataCreateRequest, false);

        // Create a target business object data based on the source business object data and target business object format.
        BusinessObjectDataCreateRequest targetBusinessObjectDataCreateRequest = businessObjectDataHelper
            .createBusinessObjectDataCreateRequest(targetBusinessObjectFormatEntity, uuid, BusinessObjectDataStatusEntity.UPLOADING,
                uploadSingleInitiationRequest.getBusinessObjectDataAttributes(), targetStorageEntity, storageDirectoryPath, storageFilePath,
                uploadSingleInitiationRequest.getFile().getFileSizeBytes(), null);

        // Create a target business object data instance. Set the flag to false, since for the file upload service the file size value is optional.
        BusinessObjectData targetBusinessObjectData = businessObjectDataHelper.createBusinessObjectData(targetBusinessObjectDataCreateRequest, false);

        // Get decrypted AWS ARN of the role that is required to provide access to S3_MANAGED_LOADING_DOCK storage.
        String awsRoleArn = getStorageUploadRoleArn(sourceStorageEntity);

        // Get expiration interval for the pre-signed URL to be generated.
        Integer awsRoleDurationSeconds = getStorageUploadSessionDuration(sourceStorageEntity);

        String awsKmsKeyId = getStorageKmsKeyId(sourceStorageEntity);

        // Get the temporary security credentials to access S3_MANAGED_STORAGE.
        Credentials assumedSessionCredentials = stsDao
            .getTemporarySecurityCredentials(awsHelper.getAwsParamsDto(), String.valueOf(sourceBusinessObjectData.getId()), awsRoleArn, awsRoleDurationSeconds,
                createUploaderPolicy(s3BucketName, storageFilePath, awsKmsKeyId));

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

        return response;
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

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CompleteUploadSingleMessageResult performCompleteUploadSingleMessage(String objectKey)
    {
        return performCompleteUploadSingleMessageImpl(objectKey);
    }

    /**
     * Implementation of the complete upload single message.
     *
     * @param objectKey the object key (i.e. filename).
     *
     * @return the complete upload single message result.
     */
    protected CompleteUploadSingleMessageResult performCompleteUploadSingleMessageImpl(String objectKey)
    {
        BusinessObjectDataKey sourceBusinessObjectDataKey = null;
        BusinessObjectDataKey targetBusinessObjectDataKey = null;

        String sourceOldStatus = null;
        String sourceNewStatus = null;
        String targetOldStatus = null;
        String targetNewStatus = null;

        StorageFileEntity storageFileEntity = null;
        String s3ManagedLoadingDockBucketName = null;
        try
        {
            // Obtain the source business object data entities.
            BusinessObjectDataEntity sourceBusinessObjectDataEntity =
                storageDaoHelper.getStorageFileEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, objectKey).getStorageUnit().getBusinessObjectData();
            sourceBusinessObjectDataKey = herdDaoHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity);

            sourceOldStatus = sourceBusinessObjectDataEntity.getStatus().getCode();

            // Obtain the target business object data entities.
            BusinessObjectDataEntity targetBusinessObjectDataEntity =
                storageDaoHelper.getStorageFileEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE, objectKey).getStorageUnit().getBusinessObjectData();
            targetBusinessObjectDataKey = herdDaoHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity);

            targetOldStatus = targetBusinessObjectDataEntity.getStatus().getCode();

            // Verify that source business object data status is "UPLOADING".
            assertBusinessObjectDataStatusEquals(BusinessObjectDataStatusEntity.UPLOADING, sourceBusinessObjectDataEntity);

            // Verify that source business object data status is "UPLOADING".
            assertBusinessObjectDataStatusEquals(BusinessObjectDataStatusEntity.UPLOADING, targetBusinessObjectDataEntity);

            // Get the S3 managed "loading dock" storage entity and make sure it exists.
            StorageEntity s3ManagedLoadingDockStorageEntity = storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE);

            // Get bucket name for S3 managed "loading dock" storage. Please note that since this attribute value is required we pass a "true" flag.
            s3ManagedLoadingDockBucketName = getStorageBucketName(s3ManagedLoadingDockStorageEntity);

            // Get the storage unit entity for this business object data in the S3 managed "loading dock" storage and make sure it exists.
            StorageUnitEntity storageUnitEntity =
                storageDaoHelper.getStorageUnitEntity(sourceBusinessObjectDataEntity, StorageEntity.MANAGED_LOADING_DOCK_STORAGE);

            // Get the storage file entity.
            storageFileEntity = storageUnitEntity.getStorageFiles().iterator().next();

            AwsParamsDto awsParamsDto = awsHelper.getAwsParamsDto();
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
                S3FileTransferRequestParamsDto.builder().s3BucketName(s3ManagedLoadingDockBucketName).s3KeyPrefix(storageFileEntity.getPath())
                    .httpProxyHost(awsParamsDto.getHttpProxyHost()).httpProxyPort(awsParamsDto.getHttpProxyPort()).build();

            // Get the metadata for the S3 object.
            s3Dao.validateS3File(s3FileTransferRequestParamsDto, storageFileEntity.getFileSizeBytes());

            // Get the S3 managed "external" storage entity and make sure it exists.
            StorageEntity s3ManagedExternalStorageEntity = storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE);

            // Get AWS KMS External Key ID.
            String awsKmsExternalKeyId = getStorageKmsKeyId(s3ManagedExternalStorageEntity);

            String s3ManagedExternalBucketName = getStorageBucketName(s3ManagedExternalStorageEntity);

            // Change the status of the source business object data to RE-ENCRYPTING.
            businessObjectDataHelper.updateBusinessObjectDataStatus(sourceBusinessObjectDataEntity, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
            sourceNewStatus = BusinessObjectDataStatusEntity.RE_ENCRYPTING;

            // Change the status of the target business object data to RE-ENCRYPTING.
            businessObjectDataHelper.updateBusinessObjectDataStatus(targetBusinessObjectDataEntity, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
            targetNewStatus = BusinessObjectDataStatusEntity.RE_ENCRYPTING;

            // Asynchronous call to move file and re-encryption and update statuses.
            uploadDownloadAsyncService
                .performFileMoveAsync(sourceBusinessObjectDataKey, targetBusinessObjectDataKey, s3ManagedLoadingDockBucketName, s3ManagedExternalBucketName,
                    storageFileEntity.getPath(), awsKmsExternalKeyId, awsHelper.getAwsParamsDto());
        }
        catch (RuntimeException ex)
        {
            // Either source/target business object data does not exist or not in UPLOADING state.

            if (sourceBusinessObjectDataKey != null)
            {
                // Update the source to DELETED
                try
                {
                    // Set the source business object data status to DELETED in new transaction as we want to throw the original exception which will mark the
                    // transaction to rollback.
                    uploadDownloadHelperService.updateBusinessObjectDataStatus(sourceBusinessObjectDataKey, BusinessObjectDataStatusEntity.DELETED);
                    sourceNewStatus = BusinessObjectDataStatusEntity.DELETED;
                }
                catch (Exception e)
                {
                    sourceNewStatus = null;
                    LOGGER.error(String.format("Failed to update source business object data status to \"DELETED\" for source business object data %s.",
                        herdHelper.businessObjectDataKeyToString(sourceBusinessObjectDataKey)), e);
                }
            }

            if (targetBusinessObjectDataKey != null)
            {
                // Update the target INVALID
                try
                {
                    // Set the target business object data status to INVALID in new transaction as we want to throw the original exception which will mark the
                    // transaction to rollback.
                    uploadDownloadHelperService.updateBusinessObjectDataStatus(targetBusinessObjectDataKey, BusinessObjectDataStatusEntity.INVALID);
                    targetNewStatus = BusinessObjectDataStatusEntity.INVALID;
                }
                catch (Exception e)
                {
                    targetNewStatus = null;
                    LOGGER.error(String.format("Failed to update target business object data status to \"INVALID\" for target business object data %s.",
                        herdHelper.businessObjectDataKeyToString(targetBusinessObjectDataKey)), e);
                }
            }

            // Delete the file from S3 if storage file information exists.
            if (storageFileEntity != null && !StringUtils.isEmpty(storageFileEntity.getPath()))
            {
                try
                {
                    // Delete the source file from S3.
                    AwsParamsDto awsParams = awsHelper.getAwsParamsDto();

                    S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
                        S3FileTransferRequestParamsDto.builder().s3BucketName(s3ManagedLoadingDockBucketName).s3KeyPrefix(storageFileEntity.getPath())
                            .httpProxyHost(awsParams.getHttpProxyHost()).httpProxyPort(awsParams.getHttpProxyPort()).build();

                    s3Dao.deleteFile(s3FileTransferRequestParamsDto);
                }
                catch (Exception e)
                {
                    LOGGER.error(String.format("Failed to delete the source business object data file: \"%s\" for source business object data %s.",
                        storageFileEntity.getPath(), herdHelper.businessObjectDataKeyToString(sourceBusinessObjectDataKey)), e);
                }
            }

            // Log the error.
            LOGGER.error(String.format("Failed to process upload single completion request for file: \"%s\".", objectKey), ex);
        }

        return generateCompleteUploadSingleMessageResult(sourceBusinessObjectDataKey, targetBusinessObjectDataKey, sourceOldStatus, sourceNewStatus,
            targetOldStatus, targetNewStatus);
    }

    private CompleteUploadSingleMessageResult generateCompleteUploadSingleMessageResult(BusinessObjectDataKey sourceBusinessObjectDataKey,
        BusinessObjectDataKey targetBusinessObjectDataKey, String sourceOldStatus, String sourceNewStatus, String targetOldStatus, String targetNewStatus)
    {
        CompleteUploadSingleMessageResult completeUploadSingleMessageResult = new CompleteUploadSingleMessageResult();

        completeUploadSingleMessageResult.setSourceBusinessObjectDataKey(sourceBusinessObjectDataKey);
        completeUploadSingleMessageResult.setSourceOldStatus(sourceOldStatus);
        completeUploadSingleMessageResult.setSourceNewStatus(sourceNewStatus);

        completeUploadSingleMessageResult.setTargetBusinessObjectDataKey(targetBusinessObjectDataKey);
        completeUploadSingleMessageResult.setTargetOldStatus(targetOldStatus);
        completeUploadSingleMessageResult.setTargetNewStatus(targetNewStatus);

        LOGGER.debug(String.format(
            "completeUploadSingleMessageResult- SourceBusinessObjectDataKey: \"%s\", sourceOldStatus: \"%s\", sourceNewStatus: \"%s\", " +
                "TargetBusinessObjectDataKey: \"%s\", targetOldStatus: \"%s\", targetNewStatus: \"%s\"",
            herdHelper.businessObjectDataKeyToString(completeUploadSingleMessageResult.getSourceBusinessObjectDataKey()),
            completeUploadSingleMessageResult.getSourceOldStatus(), completeUploadSingleMessageResult.getSourceNewStatus(),
            herdHelper.businessObjectDataKeyToString(completeUploadSingleMessageResult.getTargetBusinessObjectDataKey()),
            completeUploadSingleMessageResult.getTargetOldStatus(), completeUploadSingleMessageResult.getTargetNewStatus()));

        return completeUploadSingleMessageResult;
    }

    /*
     * The result of completeUploadSingleMessage, contains the source and target business object data key, old and new status.
     */
    public static class CompleteUploadSingleMessageResult
    {
        private BusinessObjectDataKey sourceBusinessObjectDataKey;
        private String sourceOldStatus;
        private String sourceNewStatus;

        private BusinessObjectDataKey targetBusinessObjectDataKey;
        private String targetOldStatus;
        private String targetNewStatus;

        public BusinessObjectDataKey getSourceBusinessObjectDataKey()
        {
            return sourceBusinessObjectDataKey;
        }

        public void setSourceBusinessObjectDataKey(BusinessObjectDataKey sourceBusinessObjectDataKey)
        {
            this.sourceBusinessObjectDataKey = sourceBusinessObjectDataKey;
        }

        public String getSourceOldStatus()
        {
            return sourceOldStatus;
        }

        public void setSourceOldStatus(String sourceOldStatus)
        {
            this.sourceOldStatus = sourceOldStatus;
        }

        public String getSourceNewStatus()
        {
            return sourceNewStatus;
        }

        public void setSourceNewStatus(String sourceNewStatus)
        {
            this.sourceNewStatus = sourceNewStatus;
        }

        public BusinessObjectDataKey getTargetBusinessObjectDataKey()
        {
            return targetBusinessObjectDataKey;
        }

        public void setTargetBusinessObjectDataKey(BusinessObjectDataKey targetBusinessObjectDataKey)
        {
            this.targetBusinessObjectDataKey = targetBusinessObjectDataKey;
        }

        public String getTargetOldStatus()
        {
            return targetOldStatus;
        }

        public void setTargetOldStatus(String targetOldStatus)
        {
            this.targetOldStatus = targetOldStatus;
        }

        public String getTargetNewStatus()
        {
            return targetNewStatus;
        }

        public void setTargetNewStatus(String targetNewStatus)
        {
            this.targetNewStatus = targetNewStatus;
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
        herdHelper.validateBusinessObjectFormatKey(request.getSourceBusinessObjectFormatKey());

        // Validate and trim the target business object format key.
        herdHelper.validateBusinessObjectFormatKey(request.getTargetBusinessObjectFormatKey());

        // Validate and trim the attributes.
        herdHelper.validateAttributes(request.getBusinessObjectDataAttributes());

        // Validate and trim the file information.
        Assert.notNull(request.getFile(), "File information must be specified.");
        Assert.hasText(request.getFile().getFileName(), "A file name must be specified.");
        request.getFile().setFileName(request.getFile().getFileName().trim());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DownloadSingleInitiationResponse initiateDownloadSingle(String namespace, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue, Integer businessObjectDataVersion)
    {
        // Create the business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, null, businessObjectDataVersion);

        // Validate the parameters
        herdHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true);

        // Retrieve the persisted business objecty data
        BusinessObjectDataEntity businessObjectDataEntity = herdDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Make sure the status of the business object data is VALID
        assertBusinessObjectDataStatusEquals(BusinessObjectDataStatusEntity.VALID, businessObjectDataEntity);

        // Get the external storage registered against this data
        // Validate that the storage unit exists
        StorageUnitEntity storageUnitEntity = storageDaoHelper.getStorageUnitEntity(businessObjectDataEntity, StorageEntity.MANAGED_EXTERNAL_STORAGE);

        // Validate that the storage unit contains only 1 file
        assertHasOneStorageFile(storageUnitEntity);

        String s3BucketName = getStorageBucketName(storageUnitEntity.getStorage());
        String s3ObjectKey = storageUnitEntity.getStorageFiles().iterator().next().getPath();

        // Get the temporary credentials
        Credentials downloaderCredentials = getExternalDownloaderCredentials(storageUnitEntity.getStorage(), String.valueOf(businessObjectDataEntity.getId()),
            s3ObjectKey);

        // Generate a pre-signed URL
        Date expiration = downloaderCredentials.getExpiration();
        S3FileTransferRequestParamsDto s3BucketAccessParams = storageDaoHelper.getS3BucketAccessParams(storageUnitEntity.getStorage());
        // Enable S3SigV4 ONLY for this request
        // Signature V4 is ONLY required for KMS encrypted, pre-signed URL generation and SHOULD NOT be used for any other requests.
        s3BucketAccessParams.setSignerOverride(S3FileTransferRequestParamsDto.SIGNER_OVERRIDE_V4);
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

    /**
     * {@inheritDoc}
     */
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
        BusinessObjectDataEntity businessObjectDataEntity = herdDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Ensure the status of the business object data is "uploading" in order to extend credentials.
        if (!(businessObjectDataEntity.getStatus().getCode().equals(BusinessObjectDataStatusEntity.UPLOADING)))
        {
            throw new IllegalArgumentException(String.format(String
                .format("Business object data {%s} has a status of \"%s\" and must be \"%s\" to extend " + "credentials.",
                    herdHelper.businessObjectDataKeyToString(businessObjectDataKey), businessObjectDataEntity.getStatus().getCode(),
                    BusinessObjectDataStatusEntity.UPLOADING)));
        }

        // Get the S3 managed "loading dock" storage entity and make sure it exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE);

        String s3BucketName = getStorageBucketName(storageEntity);

        // Get the storage unit entity for this business object data in the S3 managed "loading dock" storage and make sure it exists.
        StorageUnitEntity storageUnitEntity = storageDaoHelper.getStorageUnitEntity(businessObjectDataEntity, StorageEntity.MANAGED_LOADING_DOCK_STORAGE);

        // Validate that the storage unit contains exactly one storage file.
        assertHasOneStorageFile(storageUnitEntity);

        // Get the storage file entity.
        StorageFileEntity storageFileEntity = storageUnitEntity.getStorageFiles().iterator().next();

        // Get the storage file path.
        String storageFilePath = storageFileEntity.getPath();

        String awsRoleArn = getStorageUploadRoleArn(storageEntity);

        Integer awsRoleDurationSeconds = getStorageUploadSessionDuration(storageEntity);

        String awsKmsKeyId = getStorageKmsKeyId(storageEntity);

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
                herdDaoHelper.businessObjectDataEntityAltKeyToString(storageUnitEntity.getBusinessObjectData())));
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
            getStorageDownloadSessionDuration(storageEntity), createDownloaderPolicy(getStorageBucketName(storageEntity), s3ObjectKey, getStorageKmsKeyId(
                storageEntity)));
    }

    /**
     * Asserts that the status of the given data is equal to the given expected value.
     *
     * @param expectedBusinessObjectDataStatusCode - the expected status
     * @param businessObjectDataEntity - the data entity
     *
     * @throws IllegalArgumentException when status does not equal
     */
    private void assertBusinessObjectDataStatusEquals(String expectedBusinessObjectDataStatusCode, BusinessObjectDataEntity businessObjectDataEntity)
    {
        String businessObjectDataStatusCode = businessObjectDataEntity.getStatus().getCode();
        Assert.isTrue(expectedBusinessObjectDataStatusCode.equals(businessObjectDataStatusCode), String.format(
            "Business object data status \"%s\" does not match the expected status \"%s\" for the business object data {%s}.", businessObjectDataStatusCode,
            expectedBusinessObjectDataStatusCode, herdDaoHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
    }

    /**
     * Gets the storage's bucket name. Throws if not defined.
     * 
     * @param storageEntity The storage entity
     * @return S3 bucket name
     */
    private String getStorageBucketName(StorageEntity storageEntity)
    {
        return storageDaoHelper.getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageEntity,
            true);
    }

    /**
     * Gets the storage's KMS key ID. Throws if not defined.
     * 
     * @param storageEntity The storage entity
     * @return KMS key ID
     */
    private String getStorageKmsKeyId(StorageEntity storageEntity)
    {
        return storageDaoHelper.getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID), storageEntity,
            true);
    }

    /**
     * Gets the storage's upload session duration in seconds. Defaults to the configured default value if not defined.
     * 
     * @param storageEntity The storage entity
     * @return Upload session duration in seconds
     */
    private Integer getStorageUploadSessionDuration(StorageEntity storageEntity)
    {
        return storageDaoHelper.getStorageAttributeIntegerValueByName(configurationHelper.getProperty(
            ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_SESSION_DURATION_SECS), storageEntity, configurationHelper.getProperty(
                ConfigurationValue.AWS_S3_DEFAULT_UPLOAD_SESSION_DURATION_SECS, Integer.class));
    }

    /**
     * Gets the storage's upload role ARN. Throws if not defined.
     * 
     * @param storageEntity The storage entity
     * @return Upload role ARN
     */
    private String getStorageUploadRoleArn(StorageEntity storageEntity)
    {
        return storageDaoHelper.getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN),
            storageEntity, true);
    }

    /**
     * Gets the storage's download session duration in seconds. Defaults to the configured default value if not defined.
     * 
     * @param storageEntity The storage entity
     * @return Download session duration in seconds
     */
    private Integer getStorageDownloadSessionDuration(StorageEntity storageEntity)
    {
        return storageDaoHelper.getStorageAttributeIntegerValueByName(configurationHelper.getProperty(
            ConfigurationValue.S3_ATTRIBUTE_NAME_DOWNLOAD_SESSION_DURATION_SECS), storageEntity, configurationHelper.getProperty(
                ConfigurationValue.AWS_S3_DEFAULT_DOWNLOAD_SESSION_DURATION_SECS, Integer.class));
    }

    /**
     * Gets the storage's download role ARN. Throws if not defined.
     * 
     * @param storageEntity The storage entity
     * @return Download role ARN
     */
    private String getStorageDownloadRoleArn(StorageEntity storageEntity)
    {
        return storageDaoHelper.getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_DOWNLOAD_ROLE_ARN),
            storageEntity, true);
    }
}
