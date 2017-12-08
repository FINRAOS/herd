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
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchResult;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
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
    public BusinessObjectDataSearchResult searchBusinessObjectData(RetentionExpirationExporterInputManifestDto manifest)
        throws IOException, JAXBException, URISyntaxException
    {
        LOGGER.info("Retrieving business object data information from the registration server...");

        StringBuilder uriPathBuilder = new StringBuilder(HERD_APP_REST_URI_PREFIX);

        // Creating request for business object data search
        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(manifest.getNamespace());
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(manifest.getBusinessObjectDefinitionName());

        BusinessObjectDataSearchFilter businessObjectDataSearchFilter =
            new BusinessObjectDataSearchFilter(Arrays.asList((BusinessObjectDataSearchKey) Arrays.asList(businessObjectDataSearchKey)));
        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest(Arrays.asList(businessObjectDataSearchFilter));

        // Create a JAXB context and marshaller
        JAXBContext requestContext = JAXBContext.newInstance(BusinessObjectDataStorageFilesCreateRequest.class);
        Marshaller requestMarshaller = requestContext.createMarshaller();
        requestMarshaller.setProperty(Marshaller.JAXB_ENCODING, StandardCharsets.UTF_8.name());
        requestMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

        StringWriter sw = new StringWriter();
        requestMarshaller.marshal(request, sw);

        // Getting the result
        BusinessObjectDataSearchResult businessObjectDataSearchResult;
        try (CloseableHttpClient client = httpClientOperations.createHttpClient())
        {
            URI uri = new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost())
                .setPort(regServerAccessParamsDto.getRegServerPort()).setPath(HERD_APP_REST_URI_PREFIX + "/businessObjectData/search").build();
            HttpPost post = new HttpPost(uri);

            post.addHeader("Content-Type", DEFAULT_CONTENT_TYPE);
            post.addHeader("Accepts", DEFAULT_ACCEPT);

            // If SSL is enabled, set the client authentication header.
            if (regServerAccessParamsDto.isUseSsl())
            {
                post.addHeader(getAuthorizationHeader());
            }

            post.setEntity(new StringEntity(sw.toString()));

            LOGGER.info(String.format("    HTTP POST URI: %s", post.getURI().toString()));
            LOGGER.info(String.format("    HTTP POST Headers: %s", Arrays.toString(post.getAllHeaders())));
            LOGGER.info(String.format("    HTTP POST Entity Content:%n%s", sw.toString()));

            // searchBusinessObjectData() might return a null. That happens when the web client gets status code 200 back from
            // the service, but it fails to retrieve or deserialize the actual HTTP response.
            // Please note that processXmlHttpResponse() is responsible for logging the exception info as a warning.
            businessObjectDataSearchResult = searchBusinessObjectData(httpClientOperations.execute(client, post),
                "retrieve business object data search results from the registration server");
        }

        LOGGER.info("Successfully retrieved business object data search results from the registration server.");

        return businessObjectDataSearchResult;
    }

    /**
     * Retrieves business object definition from the herd registration server.
     *
     * @param manifest the retention expiration exporter input manifest dto input manifest file information
     *
     * @return the business object data information
     * @throws JAXBException if a JAXB error was encountered.
     * @throws IOException if an I/O error was encountered.
     * @throws URISyntaxException if a URI syntax error was encountered.
     */
    public BusinessObjectDefinition getBusinessObjectDefinition(RetentionExpirationExporterInputManifestDto manifest)
        throws IOException, JAXBException, URISyntaxException
    {
        LOGGER.info("Retrieving business object de information from the registration server...");

        StringBuilder uriPathBuilder = new StringBuilder(HERD_APP_REST_URI_PREFIX);
        uriPathBuilder.append("/businessObjectDefinitions");
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

        BusinessObjectDefinition businessObjectDefinition =
            this.getBusinessObjectDefinition(httpClientOperations.execute(client, request), "retrieve business object data keys from the registration server");

        LOGGER.info("Successfully retrieved business object data keys from the registration server.");

        return businessObjectDefinition;
    }


    /**
     * Extracts BusinessObjectDataSearchResult object from the registration server HTTP response.
     *
     * @param httpResponse the response received from the supported options.
     * @param actionDescription the description of the action being performed with the registration server (to be used in an error message).
     *
     * @return the BusinessObjectDataSearchResult object extracted from the registration server response.
     */
    protected BusinessObjectDataSearchResult searchBusinessObjectData(CloseableHttpResponse httpResponse, String actionDescription)
    {
        try
        {
            return (BusinessObjectDataSearchResult) processXmlHttpResponse(httpResponse, actionDescription, BusinessObjectDataSearchResult.class);
        }
        catch (Exception e)
        {
            if (httpResponse.getStatusLine().getStatusCode() == 200)
            {
                // We assume add files is a success when we get status code 200 back from the service.
                // Just return a null back, since processXmlHttpResponse() is responsible for logging the exception info.
                return null;
            }
            else
            {
                throw e;
            }
        }
    }

    /**
     * Extracts BusinessObjectDataKeys object from the registration server HTTP response.
     *
     * @param httpResponse the response received from the supported options.
     * @param actionDescription the description of the action being performed with the registration server (to be used in an error message).
     *
     * @return the BusinessObjectDataKeys object extracted from the registration server response.
     */
    private BusinessObjectDefinition getBusinessObjectDefinition(CloseableHttpResponse httpResponse, String actionDescription)
    {
        return (BusinessObjectDefinition) processXmlHttpResponse(httpResponse, actionDescription, BusinessObjectDefinition.class);
    }
}
