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
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
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

import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchResult;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

/**
 * This class encapsulates web client functionality required to communicate with the registration server.
 */
@Component
class RetentionExpirationExporterWebClient extends DataBridgeWebClient
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RetentionExpirationExporterWebClient.class);

    /**
     * Retrieves business object definition from the herd registration server.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     *
     * @return the business object definition
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    BusinessObjectDefinition getBusinessObjectDefinition(String namespace, String businessObjectDefinitionName)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        LOGGER.info("Retrieving business object definition information from the registration server...");

        String uriPathBuilder = HERD_APP_REST_URI_PREFIX + "/businessObjectDefinitions" + "/namespaces/" + namespace + "/businessObjectDefinitionNames/" +
            businessObjectDefinitionName;

        URIBuilder uriBuilder =
            new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost()).setPort(regServerAccessParamsDto.getRegServerPort())
                .setPath(uriPathBuilder);

        URI uri = uriBuilder.build();

        try (CloseableHttpClient client = httpClientHelper
            .createHttpClient(regServerAccessParamsDto.isTrustSelfSignedCertificate(), regServerAccessParamsDto.isDisableHostnameVerification()))
        {
            HttpGet request = new HttpGet(uri);
            request.addHeader("Accepts", DEFAULT_ACCEPT);

            // If SSL is enabled, set the client authentication header.
            if (regServerAccessParamsDto.isUseSsl())
            {
                request.addHeader(getAuthorizationHeader());
            }

            LOGGER.info(String.format("    HTTP GET URI: %s", request.getURI().toString()));
            LOGGER.info(String.format("    HTTP GET Headers: %s", Arrays.toString(request.getAllHeaders())));

            BusinessObjectDefinition businessObjectDefinition = getBusinessObjectDefinition(httpClientOperations.execute(client, request));

            LOGGER.info("Successfully retrieved business object definition from the registration server.");

            return businessObjectDefinition;
        }
    }

    /**
     * Retrieves business object definition from the herd registration server.
     *
     * @param businessObjectDataSearchRequest the business object definition search request
     * @param pageNum the page number for the result to contain
     *
     * @return the business object definition
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    BusinessObjectDataSearchResult searchBusinessObjectData(BusinessObjectDataSearchRequest businessObjectDataSearchRequest, Integer pageNum)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        LOGGER.info("Sending business object data search request to the registration server...");

        URIBuilder uriBuilder =
            new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost()).setPort(regServerAccessParamsDto.getRegServerPort())
                .setPath(HERD_APP_REST_URI_PREFIX + "/businessObjectData/search").setParameter("pageNum", pageNum.toString());

        URI uri = uriBuilder.build();

        // Create a JAXB context and marshaller
        JAXBContext requestContext = JAXBContext.newInstance(BusinessObjectDataSearchRequest.class);
        Marshaller requestMarshaller = requestContext.createMarshaller();
        requestMarshaller.setProperty(Marshaller.JAXB_ENCODING, StandardCharsets.UTF_8.name());
        requestMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

        StringWriter stringWriter = new StringWriter();
        requestMarshaller.marshal(businessObjectDataSearchRequest, stringWriter);

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

            request.setEntity(new StringEntity(stringWriter.toString()));

            LOGGER.info(String.format("    HTTP POST URI: %s", request.getURI().toString()));
            LOGGER.info(String.format("    HTTP POST Headers: %s", Arrays.toString(request.getAllHeaders())));
            LOGGER.info(String.format("    HTTP POST Entity Content:%n%s", stringWriter.toString()));

            BusinessObjectDataSearchResult businessObjectDataSearchResult = getBusinessObjectDataSearchResult(httpClientOperations.execute(client, request));

            LOGGER.info("Successfully received search business object data response from the registration server.");

            return businessObjectDataSearchResult;
        }
    }

    /**
     * Extracts BusinessObjectDataSearchResult from the registration server HTTP response.
     *
     * @param httpResponse the response received from the supported options
     *
     * @return the BusinessObjectDataSearchResult object extracted from the registration server response
     */
    private BusinessObjectDataSearchResult getBusinessObjectDataSearchResult(CloseableHttpResponse httpResponse)
    {
        return (BusinessObjectDataSearchResult) processXmlHttpResponse(httpResponse, "search business object data", BusinessObjectDataSearchResult.class);
    }

    /**
     * Extracts BusinessObjectDefinition object from the registration server HTTP response.
     *
     * @param httpResponse the response received from the supported options
     *
     * @return the BusinessObjectDefinition object extracted from the registration server response
     */
    private BusinessObjectDefinition getBusinessObjectDefinition(CloseableHttpResponse httpResponse)
    {
        return (BusinessObjectDefinition) processXmlHttpResponse(httpResponse, "retrieve business object definition from the registration server",
            BusinessObjectDefinition.class);
    }
}
