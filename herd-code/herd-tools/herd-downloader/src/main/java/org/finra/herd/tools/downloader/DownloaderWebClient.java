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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.xml.bind.JAXBException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.api.xml.StorageUnitDownloadCredential;
import org.finra.herd.model.dto.DataBridgeBaseManifestDto;
import org.finra.herd.model.dto.DownloaderInputManifestDto;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

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
     * @return the business object data information
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    public BusinessObjectData getBusinessObjectData(DownloaderInputManifestDto manifest)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        LOGGER.info("Retrieving business object data information from the registration server...");

        StringBuilder uriPathBuilder = new StringBuilder(HERD_APP_REST_URI_PREFIX);
        uriPathBuilder.append("/businessObjectData");
        if (manifest.getNamespace() != null)
        {
            uriPathBuilder.append("/namespaces/").append(manifest.getNamespace());
        }
        uriPathBuilder.append("/businessObjectDefinitionNames/").append(manifest.getBusinessObjectDefinitionName());
        uriPathBuilder.append("/businessObjectFormatUsages/").append(manifest.getBusinessObjectFormatUsage());
        uriPathBuilder.append("/businessObjectFormatFileTypes/").append(manifest.getBusinessObjectFormatFileType());

        URIBuilder uriBuilder =
            new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost()).setPort(regServerAccessParamsDto.getRegServerPort())
                .setPath(uriPathBuilder.toString()).setParameter("partitionKey", manifest.getPartitionKey())
                .setParameter("partitionValue", manifest.getPartitionValue())
                .setParameter("businessObjectFormatVersion", manifest.getBusinessObjectFormatVersion())
                .setParameter("businessObjectDataVersion", manifest.getBusinessObjectDataVersion());

        if (manifest.getSubPartitionValues() != null)
        {
            uriBuilder.setParameter("subPartitionValues", herdStringHelper.join(manifest.getSubPartitionValues(), "|", "\\"));
        }

        URI uri = uriBuilder.build();

        CloseableHttpClient client = httpClientHelper
            .createHttpClient(regServerAccessParamsDto.isTrustSelfSignedCertificate(), regServerAccessParamsDto.isDisableHostnameVerification());
        HttpGet request = new HttpGet(uri);
        request.addHeader("Accepts", "application/xml");

        // If SSL is enabled, set the client authentication header.
        if (regServerAccessParamsDto.isUseSsl())
        {
            request.addHeader(getAuthorizationHeader());
        }

        LOGGER.info(String.format("    HTTP GET URI: %s", request.getURI().toString()));
        LOGGER.info(String.format("    HTTP GET Headers: %s", Arrays.toString(request.getAllHeaders())));

        BusinessObjectData businessObjectData =
            getBusinessObjectData(httpClientOperations.execute(client, request), "retrieve business object data from the registration server");

        LOGGER.info("Successfully retrieved business object data from the registration server.");

        return businessObjectData;
    }

    /**
     * Retrieves S3 key prefix from the herd registration server.
     *
     * @param businessObjectData the business object data
     *
     * @return the S3 key prefix
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    public S3KeyPrefixInformation getS3KeyPrefix(BusinessObjectData businessObjectData)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
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
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    public StorageUnitDownloadCredential getStorageUnitDownloadCredential(DownloaderInputManifestDto manifest, String storageName)
        throws URISyntaxException, IOException, JAXBException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        URIBuilder uriBuilder =
            new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost()).setPort(regServerAccessParamsDto.getRegServerPort())
                .setPath(String.join("/", HERD_APP_REST_URI_PREFIX, "storageUnits", "download", "credential", "namespaces", manifest.getNamespace(),
                    "businessObjectDefinitionNames", manifest.getBusinessObjectDefinitionName(), "businessObjectFormatUsages",
                    manifest.getBusinessObjectFormatUsage(), "businessObjectFormatFileTypes", manifest.getBusinessObjectFormatFileType(),
                    "businessObjectFormatVersions", manifest.getBusinessObjectFormatVersion(), "partitionValues", manifest.getPartitionValue(),
                    "businessObjectDataVersions", manifest.getBusinessObjectDataVersion(), "storageNames", storageName));
        if (manifest.getSubPartitionValues() != null)
        {
            uriBuilder.addParameter("subPartitionValues", herdStringHelper.join(manifest.getSubPartitionValues(), "|", "\\"));
        }
        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.addHeader("Accept", DEFAULT_ACCEPT);
        if (regServerAccessParamsDto.isUseSsl())
        {
            httpGet.addHeader(getAuthorizationHeader());
        }
        try (CloseableHttpClient httpClient = httpClientHelper
            .createHttpClient(regServerAccessParamsDto.isTrustSelfSignedCertificate(), regServerAccessParamsDto.isDisableHostnameVerification()))
        {
            LOGGER.info("Retrieving download credentials from registration server...");
            return getStorageUnitDownloadCredential(httpClientOperations.execute(httpClient, httpGet));
        }
    }

    /**
     * Gets the storage unit download credentials.
     *
     * @param response the HTTP response
     *
     * @return StorageUnitDownloadCredential
     */
    private StorageUnitDownloadCredential getStorageUnitDownloadCredential(CloseableHttpResponse response)
    {
        return (StorageUnitDownloadCredential) processXmlHttpResponse(response, "get storage unit download credential", StorageUnitDownloadCredential.class);
    }
}
