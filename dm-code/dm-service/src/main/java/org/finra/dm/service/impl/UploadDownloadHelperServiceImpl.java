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

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.dm.dao.S3Dao;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.dto.AwsParamsDto;
import org.finra.dm.model.dto.S3FileCopyRequestParamsDto;
import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.service.UploadDownloadHelperService;
import org.finra.dm.service.helper.BusinessObjectDataHelper;
import org.finra.dm.service.helper.DmDaoHelper;
import org.finra.dm.service.helper.DmHelper;

/**
 * A helper service class for UploadDownloadService.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
public class UploadDownloadHelperServiceImpl implements UploadDownloadHelperService
{
    private static final Logger LOGGER = Logger.getLogger(UploadDownloadHelperServiceImpl.class);

    @Autowired
    private DmHelper dmHelper;

    @Autowired
    private DmDaoHelper dmDaoHelper;

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public String[] performFileMoveSync(BusinessObjectDataKey sourceBusinessObjectDataKey, BusinessObjectDataKey targetBusinessObjectDataKey,
        String sourceBucketName, String targetBucketName, String filePath, String kmsKeyId, AwsParamsDto awsParams)
    {
        return performFileMoveSyncImpl(sourceBusinessObjectDataKey, targetBusinessObjectDataKey, sourceBucketName, targetBucketName, filePath, kmsKeyId,
            awsParams);
    }

    /**
     * Implementation for the perform file move Sync.
     *
     * @param sourceBusinessObjectDataKey the source business object data key.
     * @param targetBusinessObjectDataKey the target business object data key.
     * @param sourceBucketName the source bucket name.
     * @param targetBucketName the target bucket name.
     * @param filePath the file path.
     * @param kmsKeyId the KMS Id.
     * @param awsParams the AWS parameters.
     *
     * @return an array of 2 strings: 1) the source status and 2) the target status.
     */
    protected String[] performFileMoveSyncImpl(BusinessObjectDataKey sourceBusinessObjectDataKey, BusinessObjectDataKey targetBusinessObjectDataKey,
        String sourceBucketName, String targetBucketName, String filePath, String kmsKeyId, AwsParamsDto awsParams)
    {
        S3FileCopyRequestParamsDto params = new S3FileCopyRequestParamsDto();
        params.setSourceBucketName(sourceBucketName);
        params.setTargetBucketName(targetBucketName);
        params.setS3KeyPrefix(filePath);
        params.setKmsKeyId(kmsKeyId);
        params.setHttpProxyHost(awsParams.getHttpProxyHost());
        params.setHttpProxyPort(awsParams.getHttpProxyPort());

        String targetStatus;
        String sourceStatus;

        try
        {
            // Copy the file from source S3 bucket to target bucket, and mark the status to VALID.
            s3Dao.copyFile(params);
            targetStatus = BusinessObjectDataStatusEntity.VALID;
        }
        catch (Exception e)
        {
            // Update the status of target BData to INVALID.
            targetStatus = BusinessObjectDataStatusEntity.INVALID;

            // Log the error.
            LOGGER.error(String.format("Failed to copy the upload single file: \"%s\" from source bucket name: \"%s\" to target bucket name: \"%s\" " +
                "for source business object data {%s} and target business object data {%s}.", filePath, sourceBucketName, targetBucketName,
                dmHelper.businessObjectDataKeyToString(sourceBusinessObjectDataKey), dmHelper.businessObjectDataKeyToString(targetBusinessObjectDataKey)), e);
        }

        try
        {
            // If status to update is "VALID", check again to ensure that status is still RE-ENCRYPTING. 
            // Otherwise leave it alone. 

            BusinessObjectDataEntity businessObjectDataEntity = dmDaoHelper.getBusinessObjectDataEntity(targetBusinessObjectDataKey);

            if (targetStatus.equalsIgnoreCase(BusinessObjectDataStatusEntity.VALID) &&
                !businessObjectDataEntity.getStatus().getCode().equalsIgnoreCase(BusinessObjectDataStatusEntity.RE_ENCRYPTING))
            {
                targetStatus = null;
            }

            // Update the status of target BData.
            if (targetStatus != null)
            {
                businessObjectDataHelper.updateBusinessObjectDataStatus(businessObjectDataEntity, targetStatus);
            }
        }
        catch (Exception e)
        {
            // Log the error if failed to update the business object data status.
            LOGGER.error(String.format(
                "Failed to update target business object data status to \"%s\" for the upload single file \"%s\" " + "for target business object data %s.",
                targetStatus, filePath, dmHelper.businessObjectDataKeyToString(targetBusinessObjectDataKey)), e);

            // Could not update target business object data status, so set the targetStatus to null to reflect that no change happened.
            targetStatus = null;
        }

        try
        {
            // Delete the source file from S3.
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
                S3FileTransferRequestParamsDto.builder().s3BucketName(sourceBucketName).s3KeyPrefix(filePath).httpProxyHost(awsParams.getHttpProxyHost())
                    .httpProxyPort(awsParams.getHttpProxyPort()).build();

            s3Dao.deleteFile(s3FileTransferRequestParamsDto);
        }
        catch (Exception e)
        {
            // Log the error if failed to delete the file from source S3 bucket.
            LOGGER.error(String
                .format("Failed to delete the upload single file: \"%s\" from source bucket name: \"%s\" " + "for source business object data %s.", filePath,
                    sourceBucketName, dmHelper.businessObjectDataKeyToString(sourceBusinessObjectDataKey)), e);
        }

        try
        {
            // Update the status of source BData to deleted.
            sourceStatus = BusinessObjectDataStatusEntity.DELETED;
            businessObjectDataHelper.updateBusinessObjectDataStatus(dmDaoHelper.getBusinessObjectDataEntity(sourceBusinessObjectDataKey), sourceStatus);
        }
        catch (Exception e)
        {
            // Log the error.
            LOGGER.error(String.format(
                "Failed to update source business object data status to \"DELETED\" for the upload single file \"%s\" " + "for source business object data %s.",
                filePath, dmHelper.businessObjectDataKeyToString(sourceBusinessObjectDataKey)), e);

            // Could not update source business object data status, so set the sourceStatus to null to reflect that no change happened.
            sourceStatus = null;
        }

        return new String[] {sourceStatus, targetStatus};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus)
    {
        updateBusinessObjectDataStatusImpl(businessObjectDataKey, businessObjectDataStatus);
    }

    /**
     * Implementation of the update business object data status.
     */
    protected void updateBusinessObjectDataStatusImpl(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus)
    {
        businessObjectDataHelper.updateBusinessObjectDataStatus(dmDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey), businessObjectDataStatus);
    }
}
