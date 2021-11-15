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
package org.finra.herd.tools.uploader;

import org.finra.herd.sdk.api.BusinessObjectDataApi;
import org.finra.herd.sdk.api.StorageUnitApi;
import org.finra.herd.sdk.invoker.ApiException;
import org.finra.herd.sdk.model.BusinessObjectDataKey;
import org.finra.herd.sdk.model.BusinessObjectDataVersions;
import org.finra.herd.sdk.model.StorageUnitUploadCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.tools.common.dto.DataBridgeBaseManifestDto;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

/**
 * This class encapsulates web client functionality required to communicate with the registration server.
 */
@Component
public class UploaderWebClient extends DataBridgeWebClient
{
    private static final Logger LOGGER = LoggerFactory.getLogger(UploaderWebClient.class);

    @Autowired
    private JsonHelper jsonHelper;

    /**
     * Gets the business object data upload credentials.
     *
     * @param manifest the manifest
     * @param storageName the storage name
     * @param businessObjectDataVersion the version of the business object data
     *
     * @return {@link StorageUnitUploadCredential}
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     * @throws ApiException if an Api exception was encountered
     */
    public StorageUnitUploadCredential getBusinessObjectDataUploadCredential(DataBridgeBaseManifestDto manifest, String storageName,
                                                                             Integer businessObjectDataVersion)
        throws ApiException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        StorageUnitApi storageUnitApi = new StorageUnitApi(createApiClient(regServerAccessParamsDto));

        return storageUnitApi.storageUnitGetStorageUnitUploadCredential(manifest.getNamespace(), manifest.getBusinessObjectDefinitionName(),
                manifest.getBusinessObjectFormatUsage(), manifest.getBusinessObjectFormatFileType(), Integer.valueOf(manifest.getBusinessObjectFormatVersion()),
                manifest.getPartitionValue(),  businessObjectDataVersion,  storageName, herdStringHelper.join(manifest.getSubPartitionValues(), "|", "\\"));
    }


    /**
     * Retrieves all versions for the specified business object data key.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return {@link org.finra.herd.model.api.xml.BusinessObjectDataVersions}
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     * @throws ApiException if an Api exception was encountered
     */
    public BusinessObjectDataVersions getBusinessObjectDataVersions(BusinessObjectDataKey businessObjectDataKey)
        throws ApiException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        LOGGER.info("Retrieving business object data versions from the registration server...");
        BusinessObjectDataApi businessObjectDataApi = new BusinessObjectDataApi(createApiClient(regServerAccessParamsDto));
        BusinessObjectDataVersions sdkResponse = businessObjectDataApi.businessObjectDataGetBusinessObjectDataVersions(
                businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                businessObjectDataKey.getPartitionValue(),
                herdStringHelper.join(businessObjectDataKey.getSubPartitionValues(), "|", "\\"),
                businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getBusinessObjectDataVersion()) ;

        LOGGER.info(String.format("Successfully retrieved %d already registered version(s) for the business object data. businessObjectDataKey=%s",
                sdkResponse.getBusinessObjectDataVersions().size(), jsonHelper.objectToJson(businessObjectDataKey)));
        return sdkResponse;
    }

    /**
     * Updates the business object data status. This method does not fail in case business object data status update is unsuccessful, but simply logs the
     * exception information as a warning.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataStatus the status of the business object data
     */
    public void updateBusinessObjectDataStatusIgnoreException(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus)
    {
        try
        {
            updateBusinessObjectDataStatus(businessObjectDataKey, businessObjectDataStatus);
        }
        catch (Exception e)
        {
            LOGGER.warn(e.getMessage(), e);
        }
    }

}
