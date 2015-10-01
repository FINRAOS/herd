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
package org.finra.dm.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.amazonaws.auth.policy.Action;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import org.finra.dm.core.DmDateUtils;
import org.finra.dm.core.helper.ConfigurationHelper;
import org.finra.dm.dao.S3Dao;
import org.finra.dm.dao.StsDao;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.dao.helper.AwsHelper;
import org.finra.dm.dao.helper.DmStringHelper;
import org.finra.dm.model.dto.AwsParamsDto;
import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.StorageAttributeEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.jpa.StorageFileEntity;
import org.finra.dm.model.jpa.StorageUnitEntity;
import org.finra.dm.model.api.xml.BusinessObjectData;
import org.finra.dm.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.DownloadSingleInitiationResponse;
import org.finra.dm.model.api.xml.UploadSingleCredentialExtensionResponse;
import org.finra.dm.model.api.xml.UploadSingleInitiationRequest;
import org.finra.dm.model.api.xml.UploadSingleInitiationResponse;
import org.finra.dm.service.UploadDownloadAsyncService;
import org.finra.dm.service.UploadDownloadHelperService;
import org.finra.dm.service.UploadDownloadService;
import org.finra.dm.service.helper.BusinessObjectDataHelper;
import org.finra.dm.service.helper.DmDaoHelper;
import org.finra.dm.service.helper.DmHelper;

/**
 * The upload download service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
public class UploadDownloadServiceImpl implements UploadDownloadService
{
    private static final Logger LOGGER = Logger.getLogger(UploadDownloadServiceImpl.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private DmHelper dmHelper;

    @Autowired
    private DmDaoHelper dmDaoHelper;

    @Autowired
    private StsDao stsDao;

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private DmStringHelper dmStringHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private UploadDownloadHelperService uploadDownloadHelperService;

    @Autowired
    private UploadDownloadAsyncService uploadDownloadAsyncService;

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
            dmDaoHelper.getBusinessObjectFormatEntity(uploadSingleInitiationRequest.getSourceBusinessObjectFormatKey());

        // Get the target business object format entity for the specified parameters and make sure it exists.
        BusinessObjectFormatEntity targetBusinessObjectFormatEntity =
            dmDaoHelper.getBusinessObjectFormatEntity(uploadSingleInitiationRequest.getTargetBusinessObjectFormatKey());

        // Get the S3 managed "loading dock" storage entity and make sure it exists.
        StorageEntity sourceStorageEntity = dmDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE);

        // Get S3 bucket name for the storage. Please note that since those values are required we pass a "true" flag.
        String s3BucketName = dmDaoHelper.getStorageAttributeValueByName(StorageAttributeEntity.ATTRIBUTE_BUCKET_NAME, sourceStorageEntity, true);

        // Get the S3 managed "external" storage entity and make sure it exists.
        StorageEntity targetStorageEntity = dmDaoHelper.getStorageEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE);

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
        String awsRoleArn = dmStringHelper.getRequiredConfigurationValue(ConfigurationValue.AWS_LOADING_DOCK_UPLOADER_ROLE_ARN);

        // Get expiration interval for the pre-signed URL to be generated.
        Integer awsRoleDurationSeconds = configurationHelper.getProperty(ConfigurationValue.AWS_LOADING_DOCK_UPLOADER_ROLE_DURATION_SECS, Integer.class);

        // Get decrypted AWS KMS Loading Dock Key ID value.
        String awsKmsKeyId = dmStringHelper.getRequiredConfigurationValue(ConfigurationValue.AWS_KMS_LOADING_DOCK_KEY_ID);

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
        response.setAwsSessionExpirationTime(DmDateUtils.getXMLGregorianCalendarValue(assumedSessionCredentials.getExpiration()));
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
        Policy policy = new Policy();
        List<Statement> statements = new ArrayList<>();
        {
            Statement statement = new Statement(Effect.Allow);
            statement.setActions(Arrays.<Action>asList(S3Actions.PutObject));
            statement.setResources(Arrays.asList(new Resource("arn:aws:s3:::" + s3BucketName + "/" + s3Key)));
            statements.add(statement);
        }
        {
            Statement statement = new Statement(Effect.Allow);
            statement.setActions(Arrays.<Action>asList(new KmsGenerateDataKeyAction(), new KmsDecryptAction()));
            statement.setResources(Arrays.asList(new Resource(awsKmsKeyId)));
            statements.add(statement);
        }
        policy.setStatements(statements);
        return policy;
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
        Policy policy = new Policy();
        List<Statement> statements = new ArrayList<>();
        {
            Statement statement = new Statement(Effect.Allow);
            statement.setActions(Arrays.<Action>asList(S3Actions.GetObject));
            statement.setResources(Arrays.asList(new Resource("arn:aws:s3:::" + s3BucketName + "/" + s3Key)));
            statements.add(statement);
        }
        {
            Statement statement = new Statement(Effect.Allow);
            statement.setActions(Arrays.<Action>asList(new KmsDecryptAction()));
            statement.setResources(Arrays.asList(new Resource(awsKmsKeyId)));
            statements.add(statement);
        }
        policy.setStatements(statements);
        return policy;
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
                dmDaoHelper.getStorageFileEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, objectKey).getStorageUnit().getBusinessObjectData();
            sourceBusinessObjectDataKey = dmDaoHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity);

            sourceOldStatus = sourceBusinessObjectDataEntity.getStatus().getCode();

            // Obtain the target business object data entities.
            BusinessObjectDataEntity targetBusinessObjectDataEntity =
                dmDaoHelper.getStorageFileEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE, objectKey).getStorageUnit().getBusinessObjectData();
            targetBusinessObjectDataKey = dmDaoHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity);

            targetOldStatus = targetBusinessObjectDataEntity.getStatus().getCode();

            // Verify that source business object data status is "UPLOADING".
            assertBusinessObjectDataStatusEquals(BusinessObjectDataStatusEntity.UPLOADING, sourceBusinessObjectDataEntity);

            // Verify that source business object data status is "UPLOADING".
            assertBusinessObjectDataStatusEquals(BusinessObjectDataStatusEntity.UPLOADING, targetBusinessObjectDataEntity);

            // Get the S3 managed "loading dock" storage entity and make sure it exists.
            StorageEntity s3ManagedLoadingDockStorageEntity = dmDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE);

            // Get bucket name for S3 managed "loading dock" storage. Please note that since this attribute value is required we pass a "true" flag.
            s3ManagedLoadingDockBucketName =
                dmDaoHelper.getStorageAttributeValueByName(StorageAttributeEntity.ATTRIBUTE_BUCKET_NAME, s3ManagedLoadingDockStorageEntity, true);

            // Get the storage unit entity for this business object data in the S3 managed "loading dock" storage and make sure it exists.
            StorageUnitEntity storageUnitEntity = dmDaoHelper.getStorageUnitEntity(sourceBusinessObjectDataEntity, StorageEntity.MANAGED_LOADING_DOCK_STORAGE);

            // Get the storage file entity.
            storageFileEntity = storageUnitEntity.getStorageFiles().iterator().next();

            AwsParamsDto awsParamsDto = awsHelper.getAwsParamsDto();
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
                S3FileTransferRequestParamsDto.builder().s3BucketName(s3ManagedLoadingDockBucketName).s3KeyPrefix(storageFileEntity.getPath())
                    .httpProxyHost(awsParamsDto.getHttpProxyHost()).httpProxyPort(awsParamsDto.getHttpProxyPort()).build();

            // Get the metadata for the S3 object.
            s3Dao.validateS3File(s3FileTransferRequestParamsDto, storageFileEntity.getFileSizeBytes());

            // Get AWS KMS External Key ID.
            String awsKmsExternalKeyId = dmStringHelper.getRequiredConfigurationValue(ConfigurationValue.AWS_KMS_EXTERNAL_KEY_ID);

            // Get the S3 managed "external" storage entity and make sure it exists.
            StorageEntity s3ManagedExternalStorageEntity = dmDaoHelper.getStorageEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE);

            // Get bucket name for the S3 managed "external" bucket. Please note that since this attribute value is required we pass a "true" flag.
            String s3ManagedExternalBucketName =
                dmDaoHelper.getStorageAttributeValueByName(StorageAttributeEntity.ATTRIBUTE_BUCKET_NAME, s3ManagedExternalStorageEntity, true);

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
                        dmHelper.businessObjectDataKeyToString(sourceBusinessObjectDataKey)), e);
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
                        dmHelper.businessObjectDataKeyToString(targetBusinessObjectDataKey)), e);
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
                        storageFileEntity.getPath(), dmHelper.businessObjectDataKeyToString(sourceBusinessObjectDataKey)), e);
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
            dmHelper.businessObjectDataKeyToString(completeUploadSingleMessageResult.getSourceBusinessObjectDataKey()),
            completeUploadSingleMessageResult.getSourceOldStatus(), completeUploadSingleMessageResult.getSourceNewStatus(),
            dmHelper.businessObjectDataKeyToString(completeUploadSingleMessageResult.getTargetBusinessObjectDataKey()),
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
        dmHelper.validateBusinessObjectFormatKey(request.getSourceBusinessObjectFormatKey(), true);

        // Validate and trim the target business object format key.
        dmHelper.validateBusinessObjectFormatKey(request.getTargetBusinessObjectFormatKey(), true);

        // Validate and trim the attributes.
        dmHelper.validateAttributes(request.getBusinessObjectDataAttributes());

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
        dmHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true, true);

        // Retrieve the persisted business objecty data
        BusinessObjectDataEntity businessObjectDataEntity = dmDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Make sure the status of the business object data is VALID
        assertBusinessObjectDataStatusEquals(BusinessObjectDataStatusEntity.VALID, businessObjectDataEntity);

        // Get the external storage registered against this data
        // Validate that the storage unit exists
        StorageUnitEntity storageUnitEntity = dmDaoHelper.getStorageUnitEntity(businessObjectDataEntity, StorageEntity.MANAGED_EXTERNAL_STORAGE);

        // Validate that the storage unit contains only 1 file
        assertHasOneStorageFile(storageUnitEntity);

        String s3BucketName = dmDaoHelper.getStorageAttributeValueByName(StorageAttributeEntity.ATTRIBUTE_BUCKET_NAME, storageUnitEntity.getStorage(), true);
        String s3ObjectKey = storageUnitEntity.getStorageFiles().iterator().next().getPath();

        // Get the temporary credentials
        Credentials downloaderCredentials = getExternalDownloaderCredentials(String.valueOf(businessObjectDataEntity.getId()), s3BucketName, s3ObjectKey);

        // Construct and return the response
        DownloadSingleInitiationResponse response = new DownloadSingleInitiationResponse();
        response.setBusinessObjectData(businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity));
        response.setAwsAccessKey(downloaderCredentials.getAccessKeyId());
        response.setAwsSecretKey(downloaderCredentials.getSecretAccessKey());
        response.setAwsSessionToken(downloaderCredentials.getSessionToken());
        response.setAwsSessionExpirationTime(DmDateUtils.getXMLGregorianCalendarValue(downloaderCredentials.getExpiration()));
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
        BusinessObjectDataEntity businessObjectDataEntity = dmDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Ensure the status of the business object data is "uploading" in order to extend credentials.
        if (!(businessObjectDataEntity.getStatus().getCode().equals(BusinessObjectDataStatusEntity.UPLOADING)))
        {
            throw new IllegalArgumentException(String.format(String
                .format("Business object data {%s} has a status of \"%s\" and must be \"%s\" to extend " + "credentials.",
                    dmHelper.businessObjectDataKeyToString(businessObjectDataKey), businessObjectDataEntity.getStatus().getCode(),
                    BusinessObjectDataStatusEntity.UPLOADING)));
        }

        // Get the S3 managed "loading dock" storage entity and make sure it exists.
        StorageEntity storageEntity = dmDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE);

        // Get S3 bucket name for the storage. Please note that since those values are required we pass a "true" flag.
        String s3BucketName = dmDaoHelper.getStorageAttributeValueByName(StorageAttributeEntity.ATTRIBUTE_BUCKET_NAME, storageEntity, true);

        // Get the storage unit entity for this business object data in the S3 managed "loading dock" storage and make sure it exists.
        StorageUnitEntity storageUnitEntity = dmDaoHelper.getStorageUnitEntity(businessObjectDataEntity, StorageEntity.MANAGED_LOADING_DOCK_STORAGE);

        // Validate that the storage unit contains exactly one storage file.
        assertHasOneStorageFile(storageUnitEntity);

        // Get the storage file entity.
        StorageFileEntity storageFileEntity = storageUnitEntity.getStorageFiles().iterator().next();

        // Get the storage file path.
        String storageFilePath = storageFileEntity.getPath();

        // Get decrypted AWS ARN of the role that is required to provide access to S3_MANAGED_LOADING_DOCK storage.
        String awsRoleArn = dmStringHelper.getRequiredConfigurationValue(ConfigurationValue.AWS_LOADING_DOCK_UPLOADER_ROLE_ARN);

        // Get expiration interval for the pre-signed URL to be generated.
        Integer awsRoleDurationSeconds = configurationHelper.getProperty(ConfigurationValue.AWS_LOADING_DOCK_UPLOADER_ROLE_DURATION_SECS, Integer.class);

        // Get decrypted AWS KMS Loading Dock Key ID value.
        String awsKmsKeyId = dmStringHelper.getRequiredConfigurationValue(ConfigurationValue.AWS_KMS_LOADING_DOCK_KEY_ID);

        // Get the temporary security credentials to access S3_MANAGED_STORAGE.
        Credentials assumedSessionCredentials = stsDao
            .getTemporarySecurityCredentials(awsHelper.getAwsParamsDto(), String.valueOf(businessObjectDataEntity.getId()), awsRoleArn, awsRoleDurationSeconds,
                createUploaderPolicy(s3BucketName, storageFilePath, awsKmsKeyId));

        // Create the response.
        UploadSingleCredentialExtensionResponse response = new UploadSingleCredentialExtensionResponse();
        response.setAwsAccessKey(assumedSessionCredentials.getAccessKeyId());
        response.setAwsSecretKey(assumedSessionCredentials.getSecretAccessKey());
        response.setAwsSessionToken(assumedSessionCredentials.getSessionToken());
        response.setAwsSessionExpirationTime(DmDateUtils.getXMLGregorianCalendarValue(assumedSessionCredentials.getExpiration()));

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
                dmDaoHelper.businessObjectDataEntityAltKeyToString(storageUnitEntity.getBusinessObjectData())));
    }

    /**
     * Gets a temporary session token that is only good for downloading the specified object key from the given bucket for a limited amount of time.
     *
     * @param sessionName the session name to use for the temporary credentials.
     * @param s3BucketName the S3 bucket name for the bucket that holds the upload data.
     * @param s3ObjectKey the S3 object key of the path to the data in the bucket.
     *
     * @return {@link Credentials} temporary session token
     */
    private Credentials getExternalDownloaderCredentials(String sessionName, String s3BucketName, String s3ObjectKey)
    {
        String downloaderRoleArn = dmStringHelper.getRequiredConfigurationValue(ConfigurationValue.AWS_EXTERNAL_DOWNLOADER_ROLE_ARN);
        int durationInSeconds = configurationHelper.getProperty(ConfigurationValue.AWS_EXTERNAL_DOWNLOADER_ROLE_DURATION_SECS, Integer.class);
        String awsKmsKeyId = dmStringHelper.getRequiredConfigurationValue(ConfigurationValue.AWS_KMS_EXTERNAL_KEY_ID);

        return stsDao.getTemporarySecurityCredentials(awsHelper.getAwsParamsDto(), sessionName, downloaderRoleArn, durationInSeconds,
            createDownloaderPolicy(s3BucketName, s3ObjectKey, awsKmsKeyId));
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
        Assert.isTrue(expectedBusinessObjectDataStatusCode.equals(businessObjectDataStatusCode), String
            .format("Business object data status \"%s\" does not match the expected status \"%s\" for the business object data {%s}.",
                businessObjectDataStatusCode, expectedBusinessObjectDataStatusCode,
                dmDaoHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
    }

    /**
     * A KMS "GenerateDataKey" action. A static named inner class was created as opposed to an anonymous inner class since it has no dependencies on it's
     * containing class and is therefore more efficient.
     */
    private static class KmsGenerateDataKeyAction implements Action
    {
        @Override
        public String getActionName()
        {
            return "kms:GenerateDataKey";
        }
    }

    /**
     * A KMS "Decrypt" action. A static named inner class was created as opposed to an anonymous inner class since it has no dependencies on it's containing
     * class and is therefore more efficient.
     */
    private static class KmsDecryptAction implements Action
    {
        @Override
        public String getActionName()
        {
            return "kms:Decrypt";
        }
    }
}
