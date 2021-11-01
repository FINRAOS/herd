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
package org.finra.herd.tools.downloader;

import org.finra.herd.core.HerdStringUtils;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.api.xml.StorageUnitDownloadCredential;
import org.finra.herd.sdk.api.BusinessObjectDataApi;
import org.finra.herd.sdk.api.StorageUnitApi;
import org.finra.herd.sdk.invoker.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.finra.herd.model.dto.DataBridgeBaseManifestDto;
import org.finra.herd.model.dto.DownloaderInputManifestDto;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

import java.net.URISyntaxException;

/**
 * This class encapsulates web client functionality required to communicate with the herd registration service.
 */
@Component
public class DownloaderWebClient extends DataBridgeWebClient
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DownloaderWebClient.class);

    /**
     * Retrieves business object data from the herd registration server.
     *
     * @param manifest the downloader input manifest file information
     *
     * @throws ApiException if an Api exception was encountered
     */
    public BusinessObjectData getBusinessObjectData(DownloaderInputManifestDto manifest) throws ApiException, URISyntaxException {
        LOGGER.info("Retrieving business object data information from the registration server...");

        BusinessObjectDataApi businessObjectDataApi = new BusinessObjectDataApi(createApiClient(regServerAccessParamsDto));
        org.finra.herd.sdk.model.BusinessObjectData sdkResponse = businessObjectDataApi.businessObjectDataGetBusinessObjectData(manifest.getNamespace(), manifest.getBusinessObjectDefinitionName(), manifest.getBusinessObjectFormatUsage(),
                manifest.getBusinessObjectFormatFileType(),
                manifest.getPartitionKey(), manifest.getPartitionValue(), herdStringHelper.join(manifest.getSubPartitionValues(), "|", "\\"),
                HerdStringUtils.convertStringToInteger(manifest.getBusinessObjectFormatVersion(), null),
                HerdStringUtils.convertStringToInteger(manifest.getBusinessObjectDataVersion(), null),

                //todo: Double check below default value, should be null or default value??
                "VALID", false, false, false);

        LOGGER.info("Successfully retrieved business object data from the registration server.");
        return convertType(sdkResponse, BusinessObjectData.class);
    }

    /**
     * Retrieves S3 key prefix from the herd registration server.
     *
     * @param businessObjectData the business object data
     *
     * @return the S3 key prefix
     * @throws ApiException if an Api exception was encountered
     */
    public S3KeyPrefixInformation getS3KeyPrefix(BusinessObjectData businessObjectData) throws ApiException, URISyntaxException {
        DataBridgeBaseManifestDto dataBridgeBaseManifestDto = new DataBridgeBaseManifestDto();
        dataBridgeBaseManifestDto.setNamespace(businessObjectData.getNamespace());
        dataBridgeBaseManifestDto.setBusinessObjectDefinitionName(businessObjectData.getBusinessObjectDefinitionName());
        dataBridgeBaseManifestDto.setBusinessObjectFormatUsage(businessObjectData.getBusinessObjectFormatUsage());
        dataBridgeBaseManifestDto.setBusinessObjectFormatFileType(businessObjectData.getBusinessObjectFormatFileType());
        dataBridgeBaseManifestDto.setBusinessObjectFormatVersion(String.valueOf(businessObjectData.getBusinessObjectFormatVersion()));
        dataBridgeBaseManifestDto.setPartitionKey(businessObjectData.getPartitionKey());
        dataBridgeBaseManifestDto.setPartitionValue(businessObjectData.getPartitionValue());
        dataBridgeBaseManifestDto.setSubPartitionValues(businessObjectData.getSubPartitionValues());
        dataBridgeBaseManifestDto.setStorageName(businessObjectData.getStorageUnits().get(0).getStorage().getName());
        return super.getS3KeyPrefix(dataBridgeBaseManifestDto, businessObjectData.getVersion(), Boolean.FALSE);
    }

    /**
     * Gets the storage unit download credentials.
     *
     * @param manifest The manifest
     * @param storageName The storage name
     *
     * @return StorageUnitDownloadCredential
     * @throws ApiException if an Api exception was encountered
     */
    public StorageUnitDownloadCredential getStorageUnitDownloadCredential(DownloaderInputManifestDto manifest, String storageName) throws ApiException, URISyntaxException {
        StorageUnitApi storageUnitApi = new StorageUnitApi(createApiClient(regServerAccessParamsDto));
        LOGGER.info("Retrieving download credentials from registration server...");

        org.finra.herd.sdk.model.StorageUnitDownloadCredential sdkResponse = storageUnitApi.storageUnitGetStorageUnitDownloadCredential
                (manifest.getNamespace(), manifest.getBusinessObjectDefinitionName(), manifest.getBusinessObjectFormatUsage(), manifest.getBusinessObjectFormatFileType(),
                        HerdStringUtils.convertStringToInteger(manifest.getBusinessObjectFormatVersion(), null),
                        manifest.getPartitionValue(), HerdStringUtils.convertStringToInteger(manifest.getBusinessObjectDataVersion(), null),
                        storageName, herdStringHelper.join(manifest.getSubPartitionValues(), "|", "\\"));

        return convertType(sdkResponse, StorageUnitDownloadCredential.class);
        // TODO: ssl, host, port, certIgnore, hostnameVerifyIgnore, do we need all these? or we can remove them?
    }
}
