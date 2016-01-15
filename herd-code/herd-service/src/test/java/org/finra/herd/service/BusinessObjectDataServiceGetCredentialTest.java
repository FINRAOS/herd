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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDataDownloadCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataUploadCredential;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.StoragePlatformEntity;

public class BusinessObjectDataServiceGetCredentialTest extends AbstractServiceTest
{
    @Test
    public void getBusinessObjectDataUploadCredentialWithBdataVersion()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE_CD);
        businessObjectDataKey.setBusinessObjectDefinitionName(BOD_NAME);
        businessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataKey.setBusinessObjectFormatVersion(FORMAT_VERSION);
        businessObjectDataKey.setPartitionValue(PARTITION_VALUE);
        businessObjectDataKey.setBusinessObjectDataVersion(DATA_VERSION);
        Boolean createNewVersion = false;
        String storageName = STORAGE_NAME;

        createBusinessObjectFormatEntity(businessObjectDataKey);

        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN), "testRole"));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucket"));
        createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        BusinessObjectDataUploadCredential businessObjectDataUploadCredential =
            businessObjectDataService.getBusinessObjectDataUploadCredential(businessObjectDataKey, createNewVersion, storageName);

        assertNotNull(businessObjectDataUploadCredential);
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsAccessKey());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSecretKey());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSessionToken());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSessionExpirationTime());
    }

    @Test
    public void getBusinessObjectDataUploadCredentialWithCreateNewVersion()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE_CD);
        businessObjectDataKey.setBusinessObjectDefinitionName(BOD_NAME);
        businessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataKey.setBusinessObjectFormatVersion(FORMAT_VERSION);
        businessObjectDataKey.setPartitionValue(PARTITION_VALUE);
        Boolean createNewVersion = true;
        String storageName = STORAGE_NAME;

        createBusinessObjectFormatEntity(businessObjectDataKey);

        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN), "testRole"));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucket"));
        createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        BusinessObjectDataUploadCredential businessObjectDataUploadCredential =
            businessObjectDataService.getBusinessObjectDataUploadCredential(businessObjectDataKey, createNewVersion, storageName);

        assertNotNull(businessObjectDataUploadCredential);
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsAccessKey());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSecretKey());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSessionToken());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSessionExpirationTime());
    }

    @Test
    public void getBusinessObjectDataUploadCredentialUseStorageAttributeDuration()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE_CD);
        businessObjectDataKey.setBusinessObjectDefinitionName(BOD_NAME);
        businessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataKey.setBusinessObjectFormatVersion(FORMAT_VERSION);
        businessObjectDataKey.setPartitionValue(PARTITION_VALUE);
        businessObjectDataKey.setBusinessObjectDataVersion(DATA_VERSION);
        Boolean createNewVersion = false;
        String storageName = STORAGE_NAME;

        createBusinessObjectFormatEntity(businessObjectDataKey);

        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN), "testRole"));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucket"));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_SESSION_DURATION_SECS), "12345"));
        createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        BusinessObjectDataUploadCredential businessObjectDataUploadCredential =
            businessObjectDataService.getBusinessObjectDataUploadCredential(businessObjectDataKey, createNewVersion, storageName);

        assertNotNull(businessObjectDataUploadCredential);
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsAccessKey());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSecretKey());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSessionToken());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSessionExpirationTime());
    }

    @Test
    public void getBusinessObjectDataUploadCredentialAssertOneOfBdataOrCreateNewVersionRequired()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE_CD);
        businessObjectDataKey.setBusinessObjectDefinitionName(BOD_NAME);
        businessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataKey.setBusinessObjectFormatVersion(FORMAT_VERSION);
        businessObjectDataKey.setPartitionValue(PARTITION_VALUE);
        businessObjectDataKey.setBusinessObjectDataVersion(null);
        Boolean createNewVersion = null;
        String storageName = STORAGE_NAME;

        createBusinessObjectFormatEntity(businessObjectDataKey);

        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN), "testRole"));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucket"));
        createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        try
        {
            businessObjectDataService.getBusinessObjectDataUploadCredential(businessObjectDataKey, createNewVersion, storageName);
            fail("Expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("One of businessObjectDataVersion or createNewVersion must be specified.", e.getMessage());
        }
    }

    @Test
    public void getBusinessObjectDataUploadCredentialAssertBDataVersionAndCreateNewVersionExclusive()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE_CD);
        businessObjectDataKey.setBusinessObjectDefinitionName(BOD_NAME);
        businessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataKey.setBusinessObjectFormatVersion(FORMAT_VERSION);
        businessObjectDataKey.setPartitionValue(PARTITION_VALUE);
        businessObjectDataKey.setBusinessObjectDataVersion(DATA_VERSION);
        Boolean createNewVersion = true;
        String storageName = STORAGE_NAME;

        createBusinessObjectFormatEntity(businessObjectDataKey);

        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN), "testRole"));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucket"));
        createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        try
        {
            businessObjectDataService.getBusinessObjectDataUploadCredential(businessObjectDataKey, createNewVersion, storageName);
            fail("Expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("createNewVersion must be false or unspecified when businessObjectDataVersion is specified.", e.getMessage());
        }
    }

    @Test
    public void getBusinessObjectDataDownloadCredential()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE_CD);
        businessObjectDataKey.setBusinessObjectDefinitionName(BOD_NAME);
        businessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataKey.setBusinessObjectFormatVersion(FORMAT_VERSION);
        businessObjectDataKey.setPartitionValue(PARTITION_VALUE);
        businessObjectDataKey.setBusinessObjectDataVersion(DATA_VERSION);
        String storageName = STORAGE_NAME;

        createBusinessObjectFormatEntity(businessObjectDataKey);

        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_DOWNLOAD_ROLE_ARN), "testRole"));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucket"));
        createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        BusinessObjectDataDownloadCredential businessObjectDataDownloadCredential =
            businessObjectDataService.getBusinessObjectDataDownloadCredential(businessObjectDataKey, storageName);

        assertNotNull(businessObjectDataDownloadCredential);
        assertNotNull(businessObjectDataDownloadCredential.getAwsCredential());
        assertNotNull(businessObjectDataDownloadCredential.getAwsCredential().getAwsAccessKey());
        assertNotNull(businessObjectDataDownloadCredential.getAwsCredential().getAwsSecretKey());
        assertNotNull(businessObjectDataDownloadCredential.getAwsCredential().getAwsSessionToken());
        assertNotNull(businessObjectDataDownloadCredential.getAwsCredential().getAwsSessionExpirationTime());
    }

    @Test
    public void getBusinessObjectDataUploadCredentialWithInvalidDurationStorageAttribute()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE_CD);
        businessObjectDataKey.setBusinessObjectDefinitionName(BOD_NAME);
        businessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataKey.setBusinessObjectFormatVersion(FORMAT_VERSION);
        businessObjectDataKey.setPartitionValue(PARTITION_VALUE);
        businessObjectDataKey.setBusinessObjectDataVersion(DATA_VERSION);
        Boolean createNewVersion = false;
        String storageName = STORAGE_NAME;

        createBusinessObjectFormatEntity(businessObjectDataKey);

        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN), "testRole"));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucket"));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_SESSION_DURATION_SECS), "notAValidInteger"));
        createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        try
        {
            businessObjectDataService.getBusinessObjectDataUploadCredential(businessObjectDataKey, createNewVersion, storageName);
            fail("Expected an IllegalStateException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Storage attribute \"upload.session.duration.secs\" must be a valid integer. Actual value is \"notAValidInteger\"", e.getMessage());
        }
    }

    @Test
    public void getBusinessObjectDataUploadCredentialWithKmsId()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE_CD);
        businessObjectDataKey.setBusinessObjectDefinitionName(BOD_NAME);
        businessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataKey.setBusinessObjectFormatVersion(FORMAT_VERSION);
        businessObjectDataKey.setPartitionValue(PARTITION_VALUE);
        Boolean createNewVersion = true;
        String storageName = STORAGE_NAME;

        createBusinessObjectFormatEntity(businessObjectDataKey);

        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN), "testRole"));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucket"));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID), "test"));
        createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        BusinessObjectDataUploadCredential businessObjectDataUploadCredential =
            businessObjectDataService.getBusinessObjectDataUploadCredential(businessObjectDataKey, createNewVersion, storageName);

        assertNotNull(businessObjectDataUploadCredential);
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsAccessKey());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSecretKey());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSessionToken());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSessionExpirationTime());
        assertEquals("test", businessObjectDataUploadCredential.getAwsKmsKeyId());
    }

    @Test
    public void getBusinessObjectDataUploadCredentialAssertStorageNameTrim()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE_CD);
        businessObjectDataKey.setBusinessObjectDefinitionName(BOD_NAME);
        businessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataKey.setBusinessObjectFormatVersion(FORMAT_VERSION);
        businessObjectDataKey.setPartitionValue(PARTITION_VALUE);
        Boolean createNewVersion = true;
        String storageName = STORAGE_NAME;

        createBusinessObjectFormatEntity(businessObjectDataKey);

        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN), "testRole"));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucket"));
        createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        BusinessObjectDataUploadCredential businessObjectDataUploadCredential =
            businessObjectDataService.getBusinessObjectDataUploadCredential(businessObjectDataKey, createNewVersion, BLANK_TEXT + storageName + BLANK_TEXT);

        assertNotNull(businessObjectDataUploadCredential);
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsAccessKey());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSecretKey());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSessionToken());
        assertNotNull(businessObjectDataUploadCredential.getAwsCredential().getAwsSessionExpirationTime());
    }

    private void createBusinessObjectFormatEntity(BusinessObjectDataKey businessObjectDataKey)
    {
        createBusinessObjectFormatEntity(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion(), null, true, PARTITION_KEY, null, null, null, null, null, null, null);
    }
}
