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
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageUnitDownloadCredential;
import org.finra.herd.model.api.xml.StorageUnitUploadCredential;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.StoragePlatformEntity;

public class StorageUnitServiceGetCredentialTest extends AbstractServiceTest
{
    @Test
    public void getStorageUnitUploadCredentialWithBdataVersion()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE);
        businessObjectDataKey.setBusinessObjectDefinitionName(BDEF_NAME);
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
        attributes.add(
            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), S3_KEY_PREFIX_VELOCITY_TEMPLATE));
        storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        StorageUnitUploadCredential storageUnitUploadCredential =
            storageUnitService.getStorageUnitUploadCredential(businessObjectDataKey, createNewVersion, storageName);

        assertNotNull(storageUnitUploadCredential);
        assertNotNull(storageUnitUploadCredential.getAwsCredential());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsAccessKey());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSecretKey());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSessionToken());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSessionExpirationTime());
    }

    @Test
    public void getStorageUnitUploadCredentialWithCreateNewVersion()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE);
        businessObjectDataKey.setBusinessObjectDefinitionName(BDEF_NAME);
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
        attributes.add(
            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), S3_KEY_PREFIX_VELOCITY_TEMPLATE));
        storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        StorageUnitUploadCredential storageUnitUploadCredential =
            storageUnitService.getStorageUnitUploadCredential(businessObjectDataKey, createNewVersion, storageName);

        assertNotNull(storageUnitUploadCredential);
        assertNotNull(storageUnitUploadCredential.getAwsCredential());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsAccessKey());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSecretKey());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSessionToken());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSessionExpirationTime());
    }

    @Test
    public void getStorageUnitUploadCredentialUseStorageAttributeDuration()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE);
        businessObjectDataKey.setBusinessObjectDefinitionName(BDEF_NAME);
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
        attributes.add(
            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), S3_KEY_PREFIX_VELOCITY_TEMPLATE));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_SESSION_DURATION_SECS), "12345"));
        storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        StorageUnitUploadCredential storageUnitUploadCredential =
            storageUnitService.getStorageUnitUploadCredential(businessObjectDataKey, createNewVersion, storageName);

        assertNotNull(storageUnitUploadCredential);
        assertNotNull(storageUnitUploadCredential.getAwsCredential());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsAccessKey());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSecretKey());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSessionToken());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSessionExpirationTime());
    }

    @Test
    public void getStorageUnitUploadCredentialAssertOneOfBdataOrCreateNewVersionRequired()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE);
        businessObjectDataKey.setBusinessObjectDefinitionName(BDEF_NAME);
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
        storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        try
        {
            storageUnitService.getStorageUnitUploadCredential(businessObjectDataKey, createNewVersion, storageName);
            fail("Expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("One of businessObjectDataVersion or createNewVersion must be specified.", e.getMessage());
        }
    }

    @Test
    public void getStorageUnitUploadCredentialAssertBDataVersionAndCreateNewVersionExclusive()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE);
        businessObjectDataKey.setBusinessObjectDefinitionName(BDEF_NAME);
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
        storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        try
        {
            storageUnitService.getStorageUnitUploadCredential(businessObjectDataKey, createNewVersion, storageName);
            fail("Expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("createNewVersion must be false or unspecified when businessObjectDataVersion is specified.", e.getMessage());
        }
    }

    @Test
    public void getStorageUnitDownloadCredential()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE);
        businessObjectDataKey.setBusinessObjectDefinitionName(BDEF_NAME);
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
        attributes.add(
            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), S3_KEY_PREFIX_VELOCITY_TEMPLATE));
        storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        StorageUnitDownloadCredential storageUnitDownloadCredential = storageUnitService.getStorageUnitDownloadCredential(businessObjectDataKey, storageName);

        assertNotNull(storageUnitDownloadCredential);
        assertNotNull(storageUnitDownloadCredential.getAwsCredential());
        assertNotNull(storageUnitDownloadCredential.getAwsCredential().getAwsAccessKey());
        assertNotNull(storageUnitDownloadCredential.getAwsCredential().getAwsSecretKey());
        assertNotNull(storageUnitDownloadCredential.getAwsCredential().getAwsSessionToken());
        assertNotNull(storageUnitDownloadCredential.getAwsCredential().getAwsSessionExpirationTime());
    }

    @Test
    public void getStorageUnitUploadCredentialWithInvalidDurationStorageAttribute()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE);
        businessObjectDataKey.setBusinessObjectDefinitionName(BDEF_NAME);
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
        storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        try
        {
            storageUnitService.getStorageUnitUploadCredential(businessObjectDataKey, createNewVersion, storageName);
            fail("Expected an IllegalStateException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Storage attribute \"upload.session.duration.secs\" must be a valid integer. Actual value is \"notAValidInteger\"", e.getMessage());
        }
    }

    @Test
    public void getStorageUnitUploadCredentialWithKmsId()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE);
        businessObjectDataKey.setBusinessObjectDefinitionName(BDEF_NAME);
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
        attributes.add(
            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), S3_KEY_PREFIX_VELOCITY_TEMPLATE));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID), "test"));
        storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        StorageUnitUploadCredential storageUnitUploadCredential =
            storageUnitService.getStorageUnitUploadCredential(businessObjectDataKey, createNewVersion, storageName);

        assertNotNull(storageUnitUploadCredential);
        assertNotNull(storageUnitUploadCredential.getAwsCredential());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsAccessKey());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSecretKey());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSessionToken());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSessionExpirationTime());
        assertEquals("test", storageUnitUploadCredential.getAwsKmsKeyId());
    }

    @Test
    public void getStorageUnitUploadCredentialAssertStorageNameTrim()
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(NAMESPACE);
        businessObjectDataKey.setBusinessObjectDefinitionName(BDEF_NAME);
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
        attributes.add(
            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), S3_KEY_PREFIX_VELOCITY_TEMPLATE));
        storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);

        StorageUnitUploadCredential storageUnitUploadCredential =
            storageUnitService.getStorageUnitUploadCredential(businessObjectDataKey, createNewVersion, BLANK_TEXT + storageName + BLANK_TEXT);

        assertNotNull(storageUnitUploadCredential);
        assertNotNull(storageUnitUploadCredential.getAwsCredential());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsAccessKey());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSecretKey());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSessionToken());
        assertNotNull(storageUnitUploadCredential.getAwsCredential().getAwsSessionExpirationTime());
    }

    private void createBusinessObjectFormatEntity(BusinessObjectDataKey businessObjectDataKey)
    {
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                businessObjectDataKey.getBusinessObjectFormatVersion(), null, null, true, PARTITION_KEY, null, null, null, null, null, null, null);
    }
}
