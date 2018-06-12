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
package org.finra.herd.tools.retention.destroyer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.xml.bind.JAXBException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

@Component
public class RetentionExpirationDestroyerWebClient extends DataBridgeWebClient
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RetentionExpirationDestroyerWebClient.class);

    /**
     * Retrieves business object definition from the herd registration server.
     *
     * @param businessObjectDataKey the name of the business object data key
     *
     * @return the business object definition
     * @throws IOException if an I/O error was encountered
     * @throws JAXBException if a JAXB error was encountered
     * @throws URISyntaxException if an URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    public BusinessObjectData destroyBusinessObjectData(BusinessObjectDataKey businessObjectDataKey)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        String uriPath = HERD_APP_REST_URI_PREFIX + "/businessObjectData/destroy" + "/namespaces/" + businessObjectDataKey.getNamespace() +
            "/businessObjectDefinitionNames/" + businessObjectDataKey.getBusinessObjectDefinitionName() + "/businessObjectFormatUsages/" +
            businessObjectDataKey.getBusinessObjectFormatUsage() + "/businessObjectFormatFileTypes/" + businessObjectDataKey.getBusinessObjectFormatFileType() +
            "/businessObjectFormatVersions/" + businessObjectDataKey.getBusinessObjectFormatVersion() + "/partitionValues/" +
            businessObjectDataKey.getPartitionValue() + "/businessObjectDataVersions/" + businessObjectDataKey.getBusinessObjectDataVersion();

        URIBuilder uriBuilder =
            new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost()).setPort(regServerAccessParamsDto.getRegServerPort())
                .setPath(uriPath);

        if (CollectionUtils.isNotEmpty(businessObjectDataKey.getSubPartitionValues()))
        {
            uriBuilder.setParameter("subPartitionValues", herdStringHelper.join(businessObjectDataKey.getSubPartitionValues(), "|", "\\"));
        }

        URI uri = uriBuilder.build();

        try (CloseableHttpClient client = httpClientHelper
            .createHttpClient(regServerAccessParamsDto.isTrustSelfSignedCertificate(), regServerAccessParamsDto.isDisableHostnameVerification()))
        {
            HttpPost request = new HttpPost(uri);
            request.addHeader("Content-Type", DEFAULT_CONTENT_TYPE);
            request.addHeader("Accepts", DEFAULT_ACCEPT);

            // If SSL is enabled, set the client authentication header.
            if (regServerAccessParamsDto.isUseSsl())
            {
                request.addHeader(getAuthorizationHeader());
            }

            LOGGER.info(String.format("    HTTP POST URI: %s", request.getURI().toString()));
            LOGGER.info(String.format("    HTTP POST Headers: %s", Arrays.toString(request.getAllHeaders())));

            BusinessObjectData businessObjectData = destroyBusinessObjectData(httpClientOperations.execute(client, request));

            LOGGER.info("Successfully destroyed business object data from the registration server.");

            return businessObjectData;
        }
    }

    /**
     * Call business object Destroy to mark for destruction.
     *
     * @param httpResponse the response received from the supported options
     *
     * @return the BusinessObjectDefinition object extracted from the registration server response
     */
    private BusinessObjectData destroyBusinessObjectData(CloseableHttpResponse httpResponse)
    {
        return (BusinessObjectData) processXmlHttpResponse(httpResponse, "destroy business object data", BusinessObjectData.class);
    }
}
