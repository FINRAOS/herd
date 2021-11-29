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
package org.finra.herd.tools.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.finra.herd.core.AbstractCoreTest;
import org.finra.herd.sdk.model.Attribute;
import org.finra.herd.sdk.model.AwsCredential;
import org.finra.herd.sdk.model.BusinessObjectDataKey;
import org.finra.herd.sdk.model.Storage;
import org.finra.herd.sdk.model.StorageDirectory;
import org.finra.herd.sdk.model.StorageFile;
import org.finra.herd.sdk.model.StorageUnit;
import org.finra.herd.sdk.model.StorageUnitStatusChangeEvent;

import org.joda.time.DateTime;
import org.junit.Test;

public class ToolsDtoHelperTest extends AbstractCoreTest
{
    @Test
    public void validateBusinessObjectDataKeyToString()
    {
        assertNull(ToolsDtoHelper.businessObjectDataKeyToString(null));
        assertEquals(
            "namespace: \"namespace\", businessObjectDefinitionName: \"bdef\", businessObjectFormatUsage: \"formatUsage\", businessObjectFormatFileType: \"fileType\", businessObjectFormatVersion: 0, businessObjectDataPartitionValue: \"pVal\", businessObjectDataSubPartitionValues: \"a,b\", businessObjectDataVersion: 1",
            ToolsDtoHelper.businessObjectDataKeyToString(
                createBusinessObjectDataKey("namespace", "bdef", "formatUsage", "fileType", 0, "pVal", Arrays.asList("a", "b"), 1)));
    }

    @Test
    public void validateConvertStorageUnit()
    {
        // empty
        StorageUnit sdkStorageUnit = new StorageUnit();
        org.finra.herd.model.api.xml.StorageUnit modelStorageUnit = ToolsDtoHelper.convertStorageUnit(sdkStorageUnit);
        assertNull(modelStorageUnit.getStorage().getName());

        // storage
        Storage storage = new Storage();
        storage.setName("testStorage");
        sdkStorageUnit.setStorage(storage);
        validateConvertStorageUnit(sdkStorageUnit);

        // status
        sdkStorageUnit.setStorageUnitStatus("VALID");
        validateConvertStorageUnit(sdkStorageUnit);

        // storageFile
        StorageFile storageFile = new StorageFile();
        storageFile.setFileSizeBytes(1L);
        storageFile.setFilePath("filepath");
        storageFile.setRowCount(1L);
        sdkStorageUnit.setStorageFiles(Collections.singletonList(storageFile));
        validateConvertStorageUnit(sdkStorageUnit);

        // storageDirectory
        StorageDirectory storageDirectory = new StorageDirectory();
        storageDirectory.setDirectoryPath("dirPath");
        sdkStorageUnit.setStorageDirectory(storageDirectory);
        validateConvertStorageUnit(sdkStorageUnit);

        // storageStatusHistory
        StorageUnitStatusChangeEvent statusChangeEvent = new StorageUnitStatusChangeEvent();
        statusChangeEvent.setStatus("VALID");
        statusChangeEvent.setUserId("userId");
        statusChangeEvent.setEventTime(DateTime.now());
        sdkStorageUnit.setStorageUnitStatusHistory(Collections.singletonList(statusChangeEvent));
        validateConvertStorageUnit(sdkStorageUnit);

        // Expire time
        sdkStorageUnit.setRestoreExpirationOn(DateTime.now());
        sdkStorageUnit.setStorageUnitStatusHistory(Collections.singletonList(statusChangeEvent));
        validateConvertStorageUnit(sdkStorageUnit);
    }

    private void validateConvertStorageUnit(StorageUnit sdkStorageUnit)
    {
        org.finra.herd.model.api.xml.StorageUnit modelStorageUnit = ToolsDtoHelper.convertStorageUnit(sdkStorageUnit);
        validateStorage(sdkStorageUnit.getStorage(), modelStorageUnit.getStorage());

        if (sdkStorageUnit.getStorageFiles() != null)
        {
            assertEquals(sdkStorageUnit.getStorageFiles().size(), modelStorageUnit.getStorageFiles().size());
            for (int i = 0; i < sdkStorageUnit.getStorageFiles().size(); i++)
            {
                StorageFile sdkStorageFile = sdkStorageUnit.getStorageFiles().get(i);
                org.finra.herd.model.api.xml.StorageFile modelStorageFile = modelStorageUnit.getStorageFiles().get(i);
                assertEquals(sdkStorageFile.getFileSizeBytes(), modelStorageFile.getFileSizeBytes());
                assertEquals(sdkStorageFile.getRowCount(), modelStorageFile.getRowCount());
                assertEquals(sdkStorageFile.getFilePath(), modelStorageFile.getFilePath());
            }
        }

        if (sdkStorageUnit.getStorageDirectory() != null)
        {
            assertEquals(sdkStorageUnit.getStorageDirectory().getDirectoryPath(), modelStorageUnit.getStorageDirectory().getDirectoryPath());
        }

        if (sdkStorageUnit.getStorageUnitStatusHistory() != null)
        {
            assertEquals(sdkStorageUnit.getStorageUnitStatusHistory().size(), modelStorageUnit.getStorageUnitStatusHistory().size());
            for (int i = 0; i < sdkStorageUnit.getStorageUnitStatusHistory().size(); i++)
            {
                StorageUnitStatusChangeEvent sdkStatusChangeEvent = sdkStorageUnit.getStorageUnitStatusHistory().get(i);
                org.finra.herd.model.api.xml.StorageUnitStatusChangeEvent modelStatusChangeEvent = modelStorageUnit.getStorageUnitStatusHistory().get(i);
                assertEquals(sdkStatusChangeEvent.getStatus(), modelStatusChangeEvent.getStatus());
                assertEquals(sdkStatusChangeEvent.getUserId(), modelStatusChangeEvent.getUserId());
                assertEquals(sdkStatusChangeEvent.getEventTime().toDate(), modelStatusChangeEvent.getEventTime().toGregorianCalendar().getTime());
            }
        }

        if (sdkStorageUnit.getRestoreExpirationOn() != null)
        {
            assertEquals(sdkStorageUnit.getRestoreExpirationOn().toDate(), modelStorageUnit.getRestoreExpirationOn().toGregorianCalendar().getTime());
        }
        else
        {
            assertNull(modelStorageUnit.getRestoreExpirationOn());
        }

        assertEquals(sdkStorageUnit.getStorageUnitStatus(), modelStorageUnit.getStorageUnitStatus());
        assertEquals(sdkStorageUnit.getStoragePolicyTransitionFailedAttempts(), modelStorageUnit.getStoragePolicyTransitionFailedAttempts());
    }

    @Test
    public void validateConvertStorage()
    {
        // validate null
        assertNull(ToolsDtoHelper.convertStorage(null).getName());

        // validate without attributes
        Storage sdkStorage = new Storage();
        sdkStorage.setName("testStorage");
        sdkStorage.setStoragePlatformName("testPlatform");
        org.finra.herd.model.api.xml.Storage modelStorage = ToolsDtoHelper.convertStorage(sdkStorage);
        validateStorage(sdkStorage, modelStorage);

        // validate with attributes
        Attribute attribute1 = new Attribute();
        attribute1.setName("name");
        attribute1.setValue("value");
        Attribute attribute2 = new Attribute();
        attribute2.setName("name");
        attribute2.setValue("value");
        sdkStorage.setAttributes(Arrays.asList(attribute1, attribute2));
        modelStorage = ToolsDtoHelper.convertStorage(sdkStorage);
        validateStorage(sdkStorage, modelStorage);
    }

    private void validateStorage(Storage sdkStorage, org.finra.herd.model.api.xml.Storage modelStorage)
    {
        if (sdkStorage != null)
        {
            assertEquals(sdkStorage.getName(), modelStorage.getName());
            assertEquals(sdkStorage.getStoragePlatformName(), modelStorage.getStoragePlatformName());

            if (sdkStorage.getAttributes() == null)
            {
                assertEquals(0, modelStorage.getAttributes().size());
            }
            else
            {
                assertEquals(sdkStorage.getAttributes().size(), modelStorage.getAttributes().size());
                for (int i = 0; i < modelStorage.getAttributes().size(); i++)
                {
                    assertEquals(sdkStorage.getAttributes().get(i).getName(), modelStorage.getAttributes().get(i).getName());
                    assertEquals(sdkStorage.getAttributes().get(i).getValue(), modelStorage.getAttributes().get(i).getValue());
                }

            }
        }
    }

    @Test
    public void validateConvertAwsCredential()
    {
        AwsCredential sdkAwsCredential = new AwsCredential();
        sdkAwsCredential.setAwsSecretKey("secretKey");
        sdkAwsCredential.setAwsAccessKey("accessKey");
        sdkAwsCredential.setAwsSessionToken("sessionToken");
        sdkAwsCredential.setAwsSessionExpirationTime(null);
        org.finra.herd.model.api.xml.AwsCredential awsCredential = ToolsDtoHelper.convertAwsCredential(sdkAwsCredential);
        assertEquals(sdkAwsCredential.getAwsAccessKey(), awsCredential.getAwsAccessKey());
        assertEquals(sdkAwsCredential.getAwsSecretKey(), awsCredential.getAwsSecretKey());
        assertEquals(sdkAwsCredential.getAwsSessionToken(), awsCredential.getAwsSessionToken());
        assertNull(awsCredential.getAwsSessionExpirationTime());

        sdkAwsCredential.setAwsSessionExpirationTime(DateTime.now());
        awsCredential = ToolsDtoHelper.convertAwsCredential(sdkAwsCredential);
        assertEquals(sdkAwsCredential.getAwsAccessKey(), awsCredential.getAwsAccessKey());
        assertEquals(sdkAwsCredential.getAwsSecretKey(), awsCredential.getAwsSecretKey());
        assertEquals(sdkAwsCredential.getAwsSessionToken(), awsCredential.getAwsSessionToken());
        assertEquals(sdkAwsCredential.getAwsSessionExpirationTime().toDate(), awsCredential.getAwsSessionExpirationTime().toGregorianCalendar().getTime());
    }

    private BusinessObjectDataKey createBusinessObjectDataKey(String namespace, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue, List<String> subPartitionValues,
        Integer businessObjectDataVersion)
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(namespace);
        businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        businessObjectDataKey.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectDataKey.setPartitionValue(partitionValue);
        businessObjectDataKey.setSubPartitionValues(subPartitionValues);
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataVersion);
        return businessObjectDataKey;
    }
}
