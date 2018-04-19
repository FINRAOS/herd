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
package org.finra.herd.rest;

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageUnitDownloadCredential;
import org.finra.herd.model.api.xml.StorageUnitUploadCredential;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.StorageUnitService;
import org.finra.herd.ui.constants.UiConstants;

@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Storage Unit")
public class StorageUnitRestController extends HerdBaseController
{
    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private StorageUnitService storageUnitService;

    /**
     * Gets the AWS credential to upload to the specified storage unit. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param businessObjectDataVersion the business object data version
     * @param storageName the storage name
     * @param subPartitionValues the sub-partition values
     *
     * @return AWS credential
     */
    @RequestMapping(value = "/storageUnits/upload/credential/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}" +
        "/storageNames/{storageName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGE_UNIT_UPLOAD_CREDENTIAL_GET)
    public StorageUnitUploadCredential getStorageUnitUploadCredential(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion, @PathVariable("storageName") String storageName,
        @RequestParam(value = "subPartitionValues", required = false) String subPartitionValues)
    {
        return storageUnitService.getStorageUnitUploadCredential(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, herdStringHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValues),
                businessObjectDataVersion), null, storageName);
    }

    /**
     * Gets the AWS credential to download to the specified storage unit. <p>Requires READ permission on namespace</p>
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param businessObjectDataVersion the business object data version
     * @param storageName the storage name
     * @param subPartitionValues the sub-partition values
     *
     * @return AWS credential
     */
    @RequestMapping(value = "/storageUnits/download/credential/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}/businessObjectDataVersions/{businessObjectDataVersion}" +
        "/storageNames/{storageName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGE_UNIT_DOWNLOAD_CREDENTIAL_GET)
    public StorageUnitDownloadCredential getStorageUnitDownloadCredential(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion, @PathVariable("storageName") String storageName,
        @RequestParam(value = "subPartitionValues", required = false) String subPartitionValues)
    {
        return storageUnitService.getStorageUnitDownloadCredential(
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, herdStringHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValues),
                businessObjectDataVersion), storageName);
    }
}
