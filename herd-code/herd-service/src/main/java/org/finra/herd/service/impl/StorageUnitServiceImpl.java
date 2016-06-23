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

import java.util.UUID;

import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.StsDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.api.xml.StorageUnitDownloadCredential;
import org.finra.herd.model.api.xml.StorageUnitUploadCredential;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.service.StorageUnitService;
import org.finra.herd.service.helper.AwsPolicyBuilder;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.KmsActions;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageHelper;

@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class StorageUnitServiceImpl implements StorageUnitService
{
    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StsDao stsDao;

    @NamespacePermission(fields = "#businessObjectDataKey.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public S3KeyPrefixInformation getS3KeyPrefix(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey, String storageName,
        Boolean createNewVersion)
    {
        return getS3KeyPrefixImpl(businessObjectDataKey, businessObjectFormatPartitionKey, storageName, createNewVersion);
    }

    @NamespacePermission(fields = "#businessObjectDataKey?.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public StorageUnitUploadCredential getStorageUnitUploadCredential(BusinessObjectDataKey businessObjectDataKey, Boolean createNewVersion, String storageName)
    {
        StorageUnitUploadCredential businessObjectDataUploadCredential = new StorageUnitUploadCredential();
        businessObjectDataUploadCredential.setAwsCredential(getBusinessObjectDataS3Credential(businessObjectDataKey, createNewVersion, storageName, true));
        businessObjectDataUploadCredential.setAwsKmsKeyId(getStorageKmsKeyId(storageDaoHelper.getStorageEntity(storageName.trim())));
        return businessObjectDataUploadCredential;
    }

    @NamespacePermission(fields = "#businessObjectDataKey?.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public StorageUnitDownloadCredential getStorageUnitDownloadCredential(BusinessObjectDataKey businessObjectDataKey, String storageName)
    {
        StorageUnitDownloadCredential businessObjectDataDownloadCredential = new StorageUnitDownloadCredential();
        businessObjectDataDownloadCredential.setAwsCredential(getBusinessObjectDataS3Credential(businessObjectDataKey, null, storageName, false));
        return businessObjectDataDownloadCredential;
    }

    /**
     * Gets the S3 key prefix.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectFormatPartitionKey the business object format partition key
     * @param storageName the storage name
     * @param createNewVersion specifies if it is OK to return an S3 key prefix for a new business object data version that is not an initial version. This
     * parameter is ignored, when the business object data version is specified.
     *
     * @return the S3 key prefix
     */
    protected S3KeyPrefixInformation getS3KeyPrefixImpl(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey,
        String storageName, Boolean createNewVersion)
    {
        // Validate and trim the business object data key.
        businessObjectDataHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, false);

        // If specified, trim the partition key parameter.
        String businessObjectFormatPartitionKeyLocal = businessObjectFormatPartitionKey;
        if (businessObjectFormatPartitionKeyLocal != null)
        {
            businessObjectFormatPartitionKeyLocal = businessObjectFormatPartitionKeyLocal.trim();
        }

        // If specified, trim the storage name. Otherwise, default to the configuration option.
        String storageNameLocal = storageName;
        if (StringUtils.isNotBlank(storageNameLocal))
        {
            storageNameLocal = storageNameLocal.trim();
        }
        else
        {
            storageNameLocal = configurationHelper.getProperty(ConfigurationValue.S3_STORAGE_NAME_DEFAULT);
        }

        // Get the business object format for the specified parameters and make sure it exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(
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

        // Get and validate the storage along with the relative attributes.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageNameLocal);

        // If the business object data version is not specified, get the next business object data version value.
        if (businessObjectDataKey.getBusinessObjectDataVersion() == null)
        {
            // Get the latest data version for this business object data, if it exists.
            BusinessObjectDataEntity latestVersionBusinessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(
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
        String s3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(storageEntity, businessObjectFormatEntity, businessObjectDataKey);

        // Create and return the S3 key prefix.
        S3KeyPrefixInformation s3KeyPrefixInformation = new S3KeyPrefixInformation();
        s3KeyPrefixInformation.setS3KeyPrefix(s3KeyPrefix);
        return s3KeyPrefixInformation;
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
        String roleArn = storageHelper.getStorageAttributeValueByName(configurationHelper.getProperty(roleArnConfigurationValue), storageEntity, true);
        Integer durationSeconds = storageHelper
            .getStorageAttributeIntegerValueByName(configurationHelper.getProperty(sessionDurationConfigurationValue), storageEntity,
                configurationHelper.getProperty(defaultSessionDurationConfigurationValue, Integer.class));
        String bucketName = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageEntity, true);

        S3KeyPrefixInformation s3KeyPrefixInformation = getS3KeyPrefixImpl(businessObjectDataKey, null, storageName, createNewVersion);
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
            .getTemporarySecurityCredentials(awsHelper.getAwsParamsDto(), UUID.randomUUID().toString(), roleArn, durationSeconds, awsPolicyBuilder.build());

        AwsCredential awsCredential = new AwsCredential();
        awsCredential.setAwsAccessKey(credentials.getAccessKeyId());
        awsCredential.setAwsSecretKey(credentials.getSecretAccessKey());
        awsCredential.setAwsSessionToken(credentials.getSessionToken());
        awsCredential.setAwsSessionExpirationTime(HerdDateUtils.getXMLGregorianCalendarValue(credentials.getExpiration()));
        return awsCredential;
    }

    private String getStorageKmsKeyId(StorageEntity storageEntity)
    {
        return storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID), storageEntity, false, true);
    }
}
