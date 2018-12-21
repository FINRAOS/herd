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
package org.finra.herd.service.helper;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectDataStorageFileKey;
import org.finra.herd.model.api.xml.DownloadBusinessObjectDataStorageFileSingleInitiationRequest;

/**
 * A helper class for upload download service related code.
 */
@Component
public class UploadDownloadHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;


    /**
     * Validates a download business object data storage file single initiation request. This method also trims the request parameters.
     *
     * @param downloadBusinessObjectDataStorageFileSingleInitiationRequest the download business object data storage file single initiation request
     */
    public void validateAndTrimDownloadBusinessObjectDataStorageFileSingleInitiationRequest(
        DownloadBusinessObjectDataStorageFileSingleInitiationRequest downloadBusinessObjectDataStorageFileSingleInitiationRequest)
    {
        Assert.notNull(downloadBusinessObjectDataStorageFileSingleInitiationRequest,
            "A download business object data storage file single initiation request must be specified.");
        validateAndTrimBusinessObjectDataStorageFileKey(
            downloadBusinessObjectDataStorageFileSingleInitiationRequest.getBusinessObjectDataStorageFileKey());
    }

    /**
     * Validates a business object data storage file key. This method also trims the key parameters.
     *
     * @param businessObjectDataStorageFileKey the business object data storage file key
     */
    public void validateAndTrimBusinessObjectDataStorageFileKey(BusinessObjectDataStorageFileKey businessObjectDataStorageFileKey)
    {
        Assert.notNull(businessObjectDataStorageFileKey, "A business object data storage file key must be specified.");
        businessObjectDataStorageFileKey
            .setNamespace(alternateKeyHelper.validateStringParameter("namespace", businessObjectDataStorageFileKey.getNamespace()));
        businessObjectDataStorageFileKey.setBusinessObjectDefinitionName(alternateKeyHelper
            .validateStringParameter("business object definition name", businessObjectDataStorageFileKey.getBusinessObjectDefinitionName()));
        businessObjectDataStorageFileKey.setBusinessObjectFormatUsage(alternateKeyHelper
            .validateStringParameter("business object format usage", businessObjectDataStorageFileKey.getBusinessObjectFormatUsage()));
        businessObjectDataStorageFileKey.setBusinessObjectFormatFileType(alternateKeyHelper
            .validateStringParameter("business object format file type", businessObjectDataStorageFileKey.getBusinessObjectFormatFileType()));

        Assert.notNull(businessObjectDataStorageFileKey.getBusinessObjectFormatVersion(), "A business object format version must be specified.");

        businessObjectDataStorageFileKey.setPartitionValue(alternateKeyHelper
            .validateStringParameter("partition value", businessObjectDataStorageFileKey.getPartitionValue()));

        List<String> subPartitionValues = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(businessObjectDataStorageFileKey.getSubPartitionValues()))
        {
            for (String subPartitionValue : businessObjectDataStorageFileKey.getSubPartitionValues())
            {
                subPartitionValues.add(alternateKeyHelper.validateStringParameter("sub partition value", subPartitionValue));
            }
        }
        businessObjectDataStorageFileKey.setSubPartitionValues(subPartitionValues);

        Assert.notNull(businessObjectDataStorageFileKey.getBusinessObjectDataVersion(), "A business object data version must be specified.");

        businessObjectDataStorageFileKey.setStorageName(alternateKeyHelper
            .validateStringParameter("storage name", businessObjectDataStorageFileKey.getStorageName()));

        Assert.hasText(businessObjectDataStorageFileKey.getFilePath(), "A file path must be specified.");

        businessObjectDataStorageFileKey.setFilePath(businessObjectDataStorageFileKey.getFilePath().trim());
    }
}
