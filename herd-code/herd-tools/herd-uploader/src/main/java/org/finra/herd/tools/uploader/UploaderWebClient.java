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

import java.io.IOException;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataUploadCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataVersions;
import org.finra.herd.model.dto.DataBridgeBaseManifestDto;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

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
     * @param createNewVersion specifies to provide credentials fof the next business object data version
     *
     * @return {@link BusinessObjectDataUploadCredential}
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    public BusinessObjectDataUploadCredential getBusinessObjectDataUploadCredential(DataBridgeBaseManifestDto manifest, String storageName,
        Integer businessObjectDataVersion, Boolean createNewVersion)
        throws URISyntaxException, IOException, JAXBException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        URIBuilder uriBuilder =
            new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost()).setPort(regServerAccessParamsDto.getRegServerPort())
                .setPath(String.join("/", HERD_APP_REST_URI_PREFIX, "businessObjectData", "upload", "credential", "namespaces", manifest.getNamespace(),
                    "businessObjectDefinitionNames", manifest.getBusinessObjectDefinitionName(), "businessObjectFormatUsages",
                    manifest.getBusinessObjectFormatUsage(), "businessObjectFormatFileTypes", manifest.getBusinessObjectFormatFileType(),
                    "businessObjectFormatVersions", manifest.getBusinessObjectFormatVersion(), "partitionValues", manifest.getPartitionValue()))
                .setParameter("storageName", storageName);
        if (manifest.getSubPartitionValues() != null)
        {
            uriBuilder.setParameter("subPartitionValues", herdStringHelper.join(manifest.getSubPartitionValues(), "|", "\\"));
        }
        if (businessObjectDataVersion != null)
        {
            uriBuilder.setParameter("businessObjectDataVersion", businessObjectDataVersion.toString());
        }
        if (createNewVersion != null)
        {
            uriBuilder.setParameter("createNewVersion", createNewVersion.toString());
        }
        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.addHeader("Accepts", DEFAULT_ACCEPT);
        if (regServerAccessParamsDto.isUseSsl())
        {
            httpGet.addHeader(getAuthorizationHeader());
        }
        try (CloseableHttpClient httpClient = httpClientHelper
            .createHttpClient(regServerAccessParamsDto.isTrustSelfSignedCertificate(), regServerAccessParamsDto.isDisableHostnameVerification()))
        {
            LOGGER.info("Retrieving upload credentials from registration server...");
            return getBusinessObjectDataUploadCredential(httpClientOperations.execute(httpClient, httpGet));
        }
    }

    /**
     * Retrieves all versions for the specified business object data key.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return {@link org.finra.herd.model.api.xml.BusinessObjectDataVersions}
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    public BusinessObjectDataVersions getBusinessObjectDataVersions(BusinessObjectDataKey businessObjectDataKey)
        throws URISyntaxException, IOException, JAXBException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        LOGGER.info("Retrieving business object data versions from the registration server...");

        BusinessObjectDataVersions businessObjectDataVersions;
        try (CloseableHttpClient client = httpClientHelper
            .createHttpClient(regServerAccessParamsDto.isTrustSelfSignedCertificate(), regServerAccessParamsDto.isDisableHostnameVerification()))
        {
            StringBuilder uriPathBuilder = new StringBuilder(300);
            uriPathBuilder.append(HERD_APP_REST_URI_PREFIX);
            uriPathBuilder.append("/businessObjectData/namespaces/").append(businessObjectDataKey.getNamespace());
            uriPathBuilder.append("/businessObjectDefinitionNames/").append(businessObjectDataKey.getBusinessObjectDefinitionName());
            uriPathBuilder.append("/businessObjectFormatUsages/").append(businessObjectDataKey.getBusinessObjectFormatUsage());
            uriPathBuilder.append("/businessObjectFormatFileTypes/").append(businessObjectDataKey.getBusinessObjectFormatFileType());
            uriPathBuilder.append("/versions");

            URIBuilder uriBuilder = new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost())
                .setPort(regServerAccessParamsDto.getRegServerPort()).setPath(uriPathBuilder.toString())
                .setParameter("partitionValue", businessObjectDataKey.getPartitionValue());

            if (businessObjectDataKey.getSubPartitionValues() != null)
            {
                uriBuilder.setParameter("subPartitionValues", herdStringHelper.join(businessObjectDataKey.getSubPartitionValues(), "|", "\\"));
            }

            if (businessObjectDataKey.getBusinessObjectFormatVersion() != null)
            {
                uriBuilder.setParameter("businessObjectFormatVersion", businessObjectDataKey.getBusinessObjectFormatVersion().toString());
            }

            if (businessObjectDataKey.getBusinessObjectDataVersion() != null)
            {
                uriBuilder.setParameter("businessObjectDataVersion", businessObjectDataKey.getBusinessObjectDataVersion().toString());
            }

            HttpGet httpGet = new HttpGet(uriBuilder.build());
            httpGet.addHeader("Accepts", DEFAULT_ACCEPT);

            // If SSL is enabled, set the client authentication header.
            if (regServerAccessParamsDto.isUseSsl())
            {
                httpGet.addHeader(getAuthorizationHeader());
            }

            LOGGER.info(String.format("    HTTP GET URI: %s", httpGet.getURI().toString()));
            LOGGER.info(String.format("    HTTP GET Headers: %s", Arrays.toString(httpGet.getAllHeaders())));

            businessObjectDataVersions = getBusinessObjectDataVersions(httpClientOperations.execute(client, httpGet));
        }

        LOGGER.info(String.format("Successfully retrieved %d already registered version(s) for the business object data. businessObjectDataKey=%s",
            businessObjectDataVersions.getBusinessObjectDataVersions().size(), jsonHelper.objectToJson(businessObjectDataKey)));

        return businessObjectDataVersions;
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

    /**
     * Extracts BusinessObjectDataVersions object from the registration server HTTP response.
     *
     * @param httpResponse the response received from the supported options.
     *
     * @return the BusinessObjectDataVersions object extracted from the registration server response.
     */
    protected BusinessObjectDataVersions getBusinessObjectDataVersions(CloseableHttpResponse httpResponse)
    {
        return (BusinessObjectDataVersions) processXmlHttpResponse(httpResponse, "retrieve business object data versions from the registration server",
            BusinessObjectDataVersions.class);
    }

    /**
     * Gets the business object data upload credentials.
     *
     * @param response the HTTP response
     *
     * @return {@link BusinessObjectDataUploadCredential}
     */
    private BusinessObjectDataUploadCredential getBusinessObjectDataUploadCredential(CloseableHttpResponse response)
    {
        return (BusinessObjectDataUploadCredential) processXmlHttpResponse(response, "get business object data upload credential",
            BusinessObjectDataUploadCredential.class);
    }
}
