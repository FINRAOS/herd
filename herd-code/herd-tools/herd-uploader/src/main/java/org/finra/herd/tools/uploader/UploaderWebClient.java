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

import javax.xml.bind.JAXBException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataUploadCredential;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.dto.DataBridgeBaseManifestDto;
import org.finra.herd.model.dto.UploaderInputManifestDto;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

/**
 * This class encapsulates web client functionality required to communicate with the registration server.
 */
@Component
public class UploaderWebClient extends DataBridgeWebClient
{
    private static final Logger LOGGER = Logger.getLogger(UploaderWebClient.class);
    
    /**
     * Retrieves S3 key prefix from the registration server.
     *
     * @param manifest the uploader input manifest file information
     * @param createNewVersion if not set, only initial version of the business object data is allowed to be created
     *
     * @return the S3 key prefix
     */
    public S3KeyPrefixInformation getS3KeyPrefix(UploaderInputManifestDto manifest, Boolean createNewVersion)
        throws IOException, JAXBException, URISyntaxException
    {
        return getS3KeyPrefix(manifest, null, createNewVersion);
    }

    /**
     * Gets the business object data upload credentials.
     * 
     * @param manifest The manifest
     * @param storageName The storage name
     * @param createNewVersion Flag to create new version
     * @return {@link BusinessObjectDataUploadCredential}
     * @throws URISyntaxException When error occurs while URI creation
     * @throws IOException When error occurs communicating with server
     * @throws JAXBException When error occurs parsing the XML
     */
    public BusinessObjectDataUploadCredential getBusinessObjectDataUploadCredential(DataBridgeBaseManifestDto manifest, String storageName,
        Boolean createNewVersion) throws URISyntaxException, IOException, JAXBException
    {
        URIBuilder uriBuilder = new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost()).setPort(regServerAccessParamsDto
            .getRegServerPort()).setPath(String.join("/", HERD_APP_REST_URI_PREFIX, "businessObjectData", "upload", "credential", "namespaces", manifest
                .getNamespace(), "businessObjectDefinitionNames", manifest.getBusinessObjectDefinitionName(), "businessObjectFormatUsages", manifest
                    .getBusinessObjectFormatUsage(), "businessObjectFormatFileTypes", manifest.getBusinessObjectFormatFileType(),
                "businessObjectFormatVersions", manifest.getBusinessObjectFormatVersion(), "partitionValues", manifest.getPartitionValue())).setParameter(
                    "storageName", storageName);
        if (manifest.getSubPartitionValues() != null)
        {
            uriBuilder.setParameter("subPartitionValues", herdStringHelper.join(manifest.getSubPartitionValues(), "|", "\\"));
        }
        if (createNewVersion != null)
        {
            uriBuilder.setParameter("createNewVersion", createNewVersion.toString());
        }
        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.addHeader("Accepts", DEFAULT_ACCEPT);
        if (regServerAccessParamsDto.getUseSsl())
        {
            httpGet.addHeader(getAuthorizationHeader());
        }
        try (CloseableHttpClient httpClient = httpClientOperations.createHttpClient())
        {
            LOGGER.info("Retrieving upload credentials from registration server...");
            return getBusinessObjectDataUploadCredential(httpClientOperations.execute(httpClient, httpGet));
        }
    }

    /**
     * Gets the business object data upload credentials.
     * 
     * @param httpResponse HTTP response
     * @return {@link BusinessObjectDataUploadCredential}
     */
    private BusinessObjectDataUploadCredential getBusinessObjectDataUploadCredential(CloseableHttpResponse response)
    {
        return (BusinessObjectDataUploadCredential) processXmlHttpResponse(response, "get business object data upload credential",
            BusinessObjectDataUploadCredential.class);
    }
}
