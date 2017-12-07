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
package org.finra.herd.tools.retention.exporter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import javax.xml.bind.JAXBException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataKeys;
import org.finra.herd.model.dto.RetentionExpirationExporterInputManifestDto;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

/**
 * This class encapsulates web client functionality required to communicate with the registration server.
 */
@Component
public class ExporterWebClient extends DataBridgeWebClient
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExporterWebClient.class);

    /**
     * Retrieves business object data from the herd registration server.
     *
     * @param manifest the retention expiration exporter input manifest dto input manifest file information
     *
     * @return the business object data information
     * @throws JAXBException if a JAXB error was encountered.
     * @throws IOException if an I/O error was encountered.
     * @throws URISyntaxException if a URI syntax error was encountered.
     */
    public BusinessObjectDataKeys getBusinessObjectDataKeys(RetentionExpirationExporterInputManifestDto manifest)
        throws IOException, JAXBException, URISyntaxException
    {
        LOGGER.info("Retrieving business object data information from the registration server...");

        StringBuilder uriPathBuilder = new StringBuilder(HERD_APP_REST_URI_PREFIX);
        uriPathBuilder.append("/businessObjectData");
        if (manifest.getNamespace() != null)
        {
            uriPathBuilder.append("/namespaces/").append(manifest.getNamespace());
        }
        uriPathBuilder.append("/businessObjectDefinitionNames/").append(manifest.getBusinessObjectDefinitionName());

        URIBuilder uriBuilder =
            new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost()).setPort(regServerAccessParamsDto.getRegServerPort())
                .setPath(uriPathBuilder.toString());

        URI uri = uriBuilder.build();

        CloseableHttpClient client = httpClientOperations.createHttpClient();
        HttpGet request = new HttpGet(uri);
        request.addHeader("Accepts", "application/xml");

        // If SSL is enabled, set the client authentication header.
        if (regServerAccessParamsDto.isUseSsl())
        {
            request.addHeader(getAuthorizationHeader());
        }

        LOGGER.info(String.format("    HTTP GET URI: %s", request.getURI().toString()));
        LOGGER.info(String.format("    HTTP GET Headers: %s", Arrays.toString(request.getAllHeaders())));

        BusinessObjectDataKeys businessObjectDataKeys =
            getBusinessObjectDataKeys(httpClientOperations.execute(client, request), "retrieve business object data keys from the registration server");

        LOGGER.info("Successfully retrieved business object data keys from the registration server.");

        return businessObjectDataKeys;
    }

    /**
     * Extracts BusinessObjectDataKeys object from the registration server HTTP response.
     *
     * @param httpResponse the response received from the supported options.
     * @param actionDescription the description of the action being performed with the registration server (to be used in an error message).
     *
     * @return the BusinessObjectDataKeys object extracted from the registration server response.
     */
    private BusinessObjectDataKeys getBusinessObjectDataKeys(CloseableHttpResponse httpResponse, String actionDescription)
    {
        return (BusinessObjectDataKeys) processXmlHttpResponse(httpResponse, actionDescription, BusinessObjectDataKeys.class);
    }
}
