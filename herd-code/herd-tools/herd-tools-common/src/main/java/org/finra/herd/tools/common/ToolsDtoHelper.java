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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import org.finra.herd.core.HerdDateUtils;

import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.sdk.model.*;

import javax.xml.datatype.XMLGregorianCalendar;

import java.util.ArrayList;
import java.util.List;

public class ToolsDtoHelper
{

    public static String businessObjectDataKeyToString(BusinessObjectDataKey businessObjectDataKey)
    {
        if (businessObjectDataKey == null)
        {
            return null;
        }

        return String.format(
            "namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                "businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", businessObjectDataSubPartitionValues: \"%s\", " +
                "businessObjectDataVersion: %d", businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(),
            CollectionUtils.isEmpty(businessObjectDataKey.getSubPartitionValues()) ? "" : StringUtils.join(businessObjectDataKey.getSubPartitionValues(), ","),
            businessObjectDataKey.getBusinessObjectDataVersion());
    }

    public static org.finra.herd.model.api.xml.StorageUnit convertStorageUnit(StorageUnit storageUnit)
    {
        org.finra.herd.model.api.xml.Storage storage = convertStorage(storageUnit.getStorage());

        List<StorageFile> storageFileList = new ArrayList<>();
        if (storageUnit.getStorageFiles() != null)
        {
            for (org.finra.herd.sdk.model.StorageFile storageFile : storageUnit.getStorageFiles())
            {
                storageFileList.add(
                    new org.finra.herd.model.api.xml.StorageFile(storageFile.getFilePath(), storageFile.getFileSizeBytes(), storageFile.getRowCount()));
            }
        }
        org.finra.herd.model.api.xml.StorageDirectory storageDirectory = new org.finra.herd.model.api.xml.StorageDirectory();
        if (storageUnit.getStorageDirectory() != null)
        {
            storageDirectory.setDirectoryPath(storageUnit.getStorageDirectory().getDirectoryPath());
        }
        List<org.finra.herd.model.api.xml.StorageUnitStatusChangeEvent> storageUnitStatusChangeEvent = new ArrayList<>();
        if (storageUnit.getStorageUnitStatusHistory() != null)
        {
            for (StorageUnitStatusChangeEvent statusChangeEvent : storageUnit.getStorageUnitStatusHistory())
            {
                XMLGregorianCalendar eventTime = null;
                if (statusChangeEvent.getEventTime() != null)
                {
                    eventTime = HerdDateUtils.getXMLGregorianCalendarValue(statusChangeEvent.getEventTime().toDate());
                }
                storageUnitStatusChangeEvent.add(
                    new org.finra.herd.model.api.xml.StorageUnitStatusChangeEvent(statusChangeEvent.getStatus(), eventTime, statusChangeEvent.getUserId()));
            }
        }
        XMLGregorianCalendar restoreExpirationOn = null;
        if (storageUnit.getRestoreExpirationOn() != null)
        {
            restoreExpirationOn = HerdDateUtils.getXMLGregorianCalendarValue(storageUnit.getRestoreExpirationOn().toDate());
        }
        return new org.finra.herd.model.api.xml.StorageUnit(storage, storageDirectory, storageFileList, storageUnit.getStorageUnitStatus(),
            storageUnitStatusChangeEvent, storageUnit.getStoragePolicyTransitionFailedAttempts(), restoreExpirationOn);
    }

    public static org.finra.herd.model.api.xml.Storage convertStorage(Storage storage)
    {
        org.finra.herd.model.api.xml.Storage modelStorage = new org.finra.herd.model.api.xml.Storage();
        if (storage != null)
        {
            modelStorage.setName(storage.getName());
            modelStorage.setStoragePlatformName(storage.getStoragePlatformName());
            modelStorage.setAttributes(new ArrayList<>());
            if (storage.getAttributes() != null)
            {
                for (Attribute attribute : storage.getAttributes())
                {
                    org.finra.herd.model.api.xml.Attribute modelAttribute =
                        new org.finra.herd.model.api.xml.Attribute(attribute.getName(), attribute.getValue());
                    modelStorage.getAttributes().add(modelAttribute);
                }
            }
        }
        return modelStorage;
    }

    public static org.finra.herd.model.api.xml.AwsCredential convertAwsCredential(AwsCredential sdkAwsCredential)
    {
        org.finra.herd.model.api.xml.AwsCredential awsCredential =
            new org.finra.herd.model.api.xml.AwsCredential(sdkAwsCredential.getAwsAccessKey(), sdkAwsCredential.getAwsSecretKey(),
                sdkAwsCredential.getAwsSessionToken(), null);
        if (sdkAwsCredential.getAwsSessionExpirationTime() != null)
        {
            awsCredential.setAwsSessionExpirationTime(HerdDateUtils.getXMLGregorianCalendarValue(sdkAwsCredential.getAwsSessionExpirationTime().toDate()));
        }
        return awsCredential;
    }
}
