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
package org.finra.herd.tools.common.databridge;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.HttpClientOperations;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.HttpClientHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.herd.model.api.xml.ErrorInformation;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnitCreateRequest;
import org.finra.herd.model.dto.DataBridgeBaseManifestDto;
import org.finra.herd.model.dto.ManifestFile;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.UploaderInputManifestDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;

/**
 * A base class for the uploader and downloader web client.
 */
public abstract class DataBridgeWebClient
{
    protected static final String DEFAULT_ACCEPT = ContentType.APPLICATION_XML.withCharset(StandardCharsets.UTF_8).toString();

    protected static final String DEFAULT_CONTENT_TYPE = ContentType.APPLICATION_XML.withCharset(StandardCharsets.UTF_8).toString();

    protected static final String HERD_APP_REST_URI_PREFIX = "/herd-app/rest";

    private static final Logger LOGGER = LoggerFactory.getLogger(DataBridgeWebClient.class);

    @Autowired
    protected HerdStringHelper herdStringHelper;

    @Autowired
    protected HttpClientHelper httpClientHelper;

    @Autowired
    protected HttpClientOperations httpClientOperations;

    /**
     * The DTO for the parameters required to communicate with the registration server.
     */
    protected RegServerAccessParamsDto regServerAccessParamsDto;

    /**
     * Calls the registration server to add storage files to the business object data.
     *
     * @param businessObjectDataKey the business object data key
     * @param manifest the uploader input manifest file
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters to be used to retrieve local path and S3 key prefix values
     * @param storageName the storage name
     *
     * @return the business object data create storage files response turned by the registration server.
     * @throws IOException if an I/O error was encountered
     * @throws JAXBException if a JAXB error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    @SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "We will use the standard carriage return character.")
    public BusinessObjectDataStorageFilesCreateResponse addStorageFiles(BusinessObjectDataKey businessObjectDataKey, UploaderInputManifestDto manifest,
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, String storageName)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        LOGGER.info("Adding storage files to the business object data ...");

        BusinessObjectDataStorageFilesCreateRequest request = new BusinessObjectDataStorageFilesCreateRequest();
        request.setNamespace(businessObjectDataKey.getNamespace());
        request.setBusinessObjectDefinitionName(businessObjectDataKey.getBusinessObjectDefinitionName());
        request.setBusinessObjectFormatUsage(businessObjectDataKey.getBusinessObjectFormatUsage());
        request.setBusinessObjectFormatFileType(businessObjectDataKey.getBusinessObjectFormatFileType());
        request.setBusinessObjectFormatVersion(businessObjectDataKey.getBusinessObjectFormatVersion());
        request.setPartitionValue(businessObjectDataKey.getPartitionValue());
        request.setSubPartitionValues(businessObjectDataKey.getSubPartitionValues());
        request.setBusinessObjectDataVersion(businessObjectDataKey.getBusinessObjectDataVersion());
        request.setStorageName(storageName);

        List<StorageFile> storageFiles = new ArrayList<>();
        request.setStorageFiles(storageFiles);

        String localPath = s3FileTransferRequestParamsDto.getLocalPath();
        String s3KeyPrefix = s3FileTransferRequestParamsDto.getS3KeyPrefix();
        List<ManifestFile> localFiles = manifest.getManifestFiles();

        for (ManifestFile manifestFile : localFiles)
        {
            StorageFile storageFile = new StorageFile();
            storageFiles.add(storageFile);
            // Since the S3 key prefix represents a directory it is expected to contain a trailing '/' character.
            storageFile.setFilePath((s3KeyPrefix + manifestFile.getFileName()).replaceAll("\\\\", "/"));
            storageFile.setFileSizeBytes(Paths.get(localPath, manifestFile.getFileName()).toFile().length());
            storageFile.setRowCount(manifestFile.getRowCount());
        }

        // Create a JAXB context and marshaller
        JAXBContext requestContext = JAXBContext.newInstance(BusinessObjectDataStorageFilesCreateRequest.class);
        Marshaller requestMarshaller = requestContext.createMarshaller();
        requestMarshaller.setProperty(Marshaller.JAXB_ENCODING, StandardCharsets.UTF_8.name());
        requestMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

        StringWriter sw = new StringWriter();
        requestMarshaller.marshal(request, sw);

        BusinessObjectDataStorageFilesCreateResponse businessObjectDataStorageFilesCreateResponse;
        try (CloseableHttpClient client = httpClientHelper
            .createHttpClient(regServerAccessParamsDto.isTrustSelfSignedCertificate(), regServerAccessParamsDto.isDisableHostnameVerification()))
        {
            URI uri = new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost())
                .setPort(regServerAccessParamsDto.getRegServerPort()).setPath(HERD_APP_REST_URI_PREFIX + "/businessObjectDataStorageFiles").build();
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

            // getBusinessObjectDataStorageFilesCreateResponse() might return a null. That happens when the web client gets status code 200 back from
            // the service (add storage files is a success), but it fails to retrieve or deserialize the actual HTTP response.
            // Please note that processXmlHttpResponse() is responsible for logging the exception info as a warning.
            businessObjectDataStorageFilesCreateResponse = getBusinessObjectDataStorageFilesCreateResponse(httpClientOperations.execute(client, post));
        }

        LOGGER.info("Successfully added storage files to the registered business object data.");

        return businessObjectDataStorageFilesCreateResponse;
    }

    /**
     * Returns the Registration Server Access Parameters DTO.
     *
     * @return the DTO for the parameters required to communicate with the registration server
     */
    public RegServerAccessParamsDto getRegServerAccessParamsDto()
    {
        return regServerAccessParamsDto;
    }

    /**
     * Sets the Registration Server Access Parameters DTO.
     *
     * @param regServerAccessParamsDto the DTO for the parameters required to communicate with the registration server
     */
    public void setRegServerAccessParamsDto(RegServerAccessParamsDto regServerAccessParamsDto)
    {
        this.regServerAccessParamsDto = regServerAccessParamsDto;
    }

    /**
     * Gets storage information from the registration server.
     *
     * @param storageName the storage name
     *
     * @return the storage information
     * @throws IOException if an I/O error was encountered
     * @throws JAXBException if a JAXB error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    public Storage getStorage(String storageName)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        LOGGER.info(String.format("Retrieving storage information for \"%s\" storage name from the registration server...", storageName));

        final String URI_PATH = HERD_APP_REST_URI_PREFIX + "/storages/" + storageName;

        URIBuilder uriBuilder =
            new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost()).setPort(regServerAccessParamsDto.getRegServerPort())
                .setPath(URI_PATH);

        Storage storage;
        try (CloseableHttpClient client = httpClientHelper
            .createHttpClient(regServerAccessParamsDto.isTrustSelfSignedCertificate(), regServerAccessParamsDto.isDisableHostnameVerification()))
        {
            HttpGet request = new HttpGet(uriBuilder.build());
            request.addHeader("Accepts", DEFAULT_ACCEPT);

            // If SSL is enabled, set the client authentication header.
            if (regServerAccessParamsDto.isUseSsl())
            {
                request.addHeader(getAuthorizationHeader());
            }

            LOGGER.info(String.format("    HTTP GET URI: %s", request.getURI().toString()));
            LOGGER.info(String.format("    HTTP GET Headers: %s", Arrays.toString(request.getAllHeaders())));

            storage = getStorage(httpClientOperations.execute(client, request));
        }

        LOGGER.info("Successfully retrieved storage information from the registration server.");
        LOGGER.info("    Storage name: " + storage.getName());
        LOGGER.info("    Attributes: ");

        for (Attribute attribute : storage.getAttributes())
        {
            LOGGER.info(String.format("        \"%s\"=\"%s\"", attribute.getName(), attribute.getValue()));
        }

        return storage;
    }

    /**
     * Pre-registers business object data with the registration server.
     *
     * @param manifest the uploader input manifest file
     * @param storageName the storage name
     * @param createNewVersion if not set, only initial version of the business object data is allowed to be created
     *
     * @return the business object data returned by the registration server.
     * @throws IOException if an I/O error was encountered
     * @throws JAXBException if a JAXB error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    @SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "We will use the standard carriage return character.")
    public BusinessObjectData preRegisterBusinessObjectData(UploaderInputManifestDto manifest, String storageName, Boolean createNewVersion)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        LOGGER.info("Pre-registering business object data with the registration server...");

        BusinessObjectDataCreateRequest request = new BusinessObjectDataCreateRequest();
        request.setNamespace(manifest.getNamespace());
        request.setBusinessObjectDefinitionName(manifest.getBusinessObjectDefinitionName());
        request.setBusinessObjectFormatUsage(manifest.getBusinessObjectFormatUsage());
        request.setBusinessObjectFormatFileType(manifest.getBusinessObjectFormatFileType());
        request.setBusinessObjectFormatVersion(Integer.parseInt(manifest.getBusinessObjectFormatVersion()));
        request.setPartitionKey(manifest.getPartitionKey());
        request.setPartitionValue(manifest.getPartitionValue());
        request.setSubPartitionValues(manifest.getSubPartitionValues());
        request.setCreateNewVersion(createNewVersion);
        request.setStatus(BusinessObjectDataStatusEntity.UPLOADING);

        List<StorageUnitCreateRequest> storageUnits = new ArrayList<>();
        request.setStorageUnits(storageUnits);
        StorageUnitCreateRequest storageUnit = new StorageUnitCreateRequest();
        storageUnits.add(storageUnit);
        storageUnit.setStorageName(storageName);

        // Add business object data attributes, if any.
        if (manifest.getAttributes() != null)
        {
            List<Attribute> attributes = new ArrayList<>();
            request.setAttributes(attributes);

            for (Map.Entry<String, String> entry : manifest.getAttributes().entrySet())
            {
                Attribute attribute = new Attribute();
                attributes.add(attribute);
                attribute.setName(entry.getKey());
                attribute.setValue(entry.getValue());
            }
        }

        // Add business object data parents, if any.
        request.setBusinessObjectDataParents(manifest.getBusinessObjectDataParents());

        // Create a JAXB context and marshaller
        JAXBContext requestContext = JAXBContext.newInstance(BusinessObjectDataCreateRequest.class);
        Marshaller requestMarshaller = requestContext.createMarshaller();
        requestMarshaller.setProperty(Marshaller.JAXB_ENCODING, StandardCharsets.UTF_8.name());
        requestMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

        StringWriter sw = new StringWriter();
        requestMarshaller.marshal(request, sw);

        BusinessObjectData businessObjectData;
        try (CloseableHttpClient client = httpClientHelper
            .createHttpClient(regServerAccessParamsDto.isTrustSelfSignedCertificate(), regServerAccessParamsDto.isDisableHostnameVerification()))
        {
            URI uri = new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost())
                .setPort(regServerAccessParamsDto.getRegServerPort()).setPath(HERD_APP_REST_URI_PREFIX + "/businessObjectData").build();
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

            businessObjectData =
                getBusinessObjectData(httpClientOperations.execute(client, post), "register business object data with the registration server");
        }

        LOGGER.info(String
            .format("Successfully pre-registered business object data with the registration server. businessObjectDataId=%s", businessObjectData.getId()));

        return businessObjectData;
    }

    /**
     * Updates the business object data status.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataStatus the status of the business object data
     *
     * @return {@link org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateResponse}
     * @throws URISyntaxException if error occurs while URI creation
     * @throws IOException if error occurs communicating with server
     * @throws JAXBException if error occurs parsing the XML
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    public BusinessObjectDataStatusUpdateResponse updateBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus)
        throws URISyntaxException, IOException, JAXBException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        BusinessObjectDataStatusUpdateRequest request = new BusinessObjectDataStatusUpdateRequest();
        request.setStatus(businessObjectDataStatus);

        // Create a JAXB context and marshaller
        JAXBContext requestContext = JAXBContext.newInstance(BusinessObjectDataStatusUpdateRequest.class);
        Marshaller requestMarshaller = requestContext.createMarshaller();
        requestMarshaller.setProperty(Marshaller.JAXB_ENCODING, StandardCharsets.UTF_8.name());
        requestMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

        StringWriter sw = new StringWriter();
        requestMarshaller.marshal(request, sw);

        BusinessObjectDataStatusUpdateResponse businessObjectDataStatusUpdateResponse;
        try (CloseableHttpClient client = httpClientHelper
            .createHttpClient(regServerAccessParamsDto.isTrustSelfSignedCertificate(), regServerAccessParamsDto.isDisableHostnameVerification()))
        {

            StringBuilder uriPathBuilder = new StringBuilder(300);
            uriPathBuilder.append(HERD_APP_REST_URI_PREFIX + "/businessObjectDataStatus/namespaces/").append(businessObjectDataKey.getNamespace());
            uriPathBuilder.append("/businessObjectDefinitionNames/").append(businessObjectDataKey.getBusinessObjectDefinitionName());
            uriPathBuilder.append("/businessObjectFormatUsages/").append(businessObjectDataKey.getBusinessObjectFormatUsage());
            uriPathBuilder.append("/businessObjectFormatFileTypes/").append(businessObjectDataKey.getBusinessObjectFormatFileType());
            uriPathBuilder.append("/businessObjectFormatVersions/").append(businessObjectDataKey.getBusinessObjectFormatVersion());
            uriPathBuilder.append("/partitionValues/").append(businessObjectDataKey.getPartitionValue());
            for (int i = 0; i < org.apache.commons.collections4.CollectionUtils.size(businessObjectDataKey.getSubPartitionValues()) &&
                i < BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
            {
                uriPathBuilder.append("/subPartition").append(i + 1).append("Values/").append(businessObjectDataKey.getSubPartitionValues().get(i));
            }
            uriPathBuilder.append("/businessObjectDataVersions/").append(businessObjectDataKey.getBusinessObjectDataVersion());

            URIBuilder uriBuilder = new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost())
                .setPort(regServerAccessParamsDto.getRegServerPort()).setPath(uriPathBuilder.toString());

            HttpPut httpPut = new HttpPut(uriBuilder.build());
            httpPut.addHeader("Content-Type", DEFAULT_CONTENT_TYPE);
            httpPut.addHeader("Accepts", DEFAULT_ACCEPT);
            if (regServerAccessParamsDto.isUseSsl())
            {
                httpPut.addHeader(getAuthorizationHeader());
            }

            httpPut.setEntity(new StringEntity(sw.toString()));

            LOGGER.info(String.format("    HTTP POST URI: %s", httpPut.getURI().toString()));
            LOGGER.info(String.format("    HTTP POST Headers: %s", Arrays.toString(httpPut.getAllHeaders())));
            LOGGER.info(String.format("    HTTP POST Entity Content:%n%s", sw.toString()));

            businessObjectDataStatusUpdateResponse = getBusinessObjectDataStatusUpdateResponse(httpClientOperations.execute(client, httpPut));
        }

        LOGGER.info("Successfully updated status of the business object data.");

        return businessObjectDataStatusUpdateResponse;
    }

    /**
     * Returns an authorization header required for HTTPS client authentication with the registration server.
     *
     * @return the authorization header
     */
    protected BasicHeader getAuthorizationHeader()
    {
        String combined = regServerAccessParamsDto.getUsername() + ":" + regServerAccessParamsDto.getPassword();
        byte[] encodedBytes = Base64.encodeBase64(combined.getBytes(StandardCharsets.UTF_8));
        return new BasicHeader("Authorization", "Basic " + new String(encodedBytes, StandardCharsets.UTF_8));
    }

    /**
     * Extracts BusinessObjectData object from the registration server HTTP response.
     *
     * @param httpResponse the response received from the supported options.
     * @param actionDescription the description of the action being performed with the registration server (to be used in an error message).
     *
     * @return the BusinessObjectData object extracted from the registration server response.
     */
    protected BusinessObjectData getBusinessObjectData(CloseableHttpResponse httpResponse, String actionDescription)
    {
        return (BusinessObjectData) processXmlHttpResponse(httpResponse, actionDescription, BusinessObjectData.class);
    }

    /**
     * Extracts BusinessObjectDataStorageFilesCreateResponse object from the registration server HTTP response.
     *
     * @param httpResponse the response received from the supported options.
     *
     * @return the BusinessObjectData object extracted from the registration server response.
     */
    protected BusinessObjectDataStorageFilesCreateResponse getBusinessObjectDataStorageFilesCreateResponse(CloseableHttpResponse httpResponse)
    {
        try
        {
            return (BusinessObjectDataStorageFilesCreateResponse) processXmlHttpResponse(httpResponse, "add storage files",
                BusinessObjectDataStorageFilesCreateResponse.class);
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
     * Retrieves S3 key prefix from the registration server.
     *
     * @param manifest the manifest file information
     * @param businessObjectDataVersion the business object data version (optional)
     * @param createNewVersion if not set, only initial version of the business object data is allowed to be created.  This parameter is ignored, when the
     * business object data version is specified.
     *
     * @return the S3 key prefix
     * @throws IOException if an I/O error was encountered
     * @throws JAXBException if a JAXB error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    protected S3KeyPrefixInformation getS3KeyPrefix(DataBridgeBaseManifestDto manifest, Integer businessObjectDataVersion, Boolean createNewVersion)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        LOGGER.info("Retrieving S3 key prefix from the registration server...");

        StringBuilder uriPathBuilder = new StringBuilder(151);
        uriPathBuilder.append(HERD_APP_REST_URI_PREFIX + "/businessObjectData");
        // The namespace is optional. If not specified, do not add to the REST URI.
        if (StringUtils.isNotBlank(manifest.getNamespace()))
        {
            uriPathBuilder.append("/namespaces/").append(manifest.getNamespace());
        }
        uriPathBuilder.append("/businessObjectDefinitionNames/").append(manifest.getBusinessObjectDefinitionName());
        uriPathBuilder.append("/businessObjectFormatUsages/").append(manifest.getBusinessObjectFormatUsage());
        uriPathBuilder.append("/businessObjectFormatFileTypes/").append(manifest.getBusinessObjectFormatFileType());
        uriPathBuilder.append("/businessObjectFormatVersions/").append(manifest.getBusinessObjectFormatVersion());
        uriPathBuilder.append("/s3KeyPrefix");

        String uriPath = uriPathBuilder.toString();

        URIBuilder uriBuilder =
            new URIBuilder().setScheme(getUriScheme()).setHost(regServerAccessParamsDto.getRegServerHost()).setPort(regServerAccessParamsDto.getRegServerPort())
                .setPath(uriPath).setParameter("partitionKey", manifest.getPartitionKey()).setParameter("partitionValue", manifest.getPartitionValue())
                .setParameter("createNewVersion", createNewVersion.toString());

        if (!CollectionUtils.isEmpty(manifest.getSubPartitionValues()))
        {
            uriBuilder.setParameter("subPartitionValues", herdStringHelper.join(manifest.getSubPartitionValues(), "|", "\\"));
        }

        if (businessObjectDataVersion != null)
        {
            uriBuilder.setParameter("businessObjectDataVersion", businessObjectDataVersion.toString());
        }

        if (StringUtils.isNotBlank(manifest.getStorageName()))
        {
            uriBuilder.setParameter("storageName", manifest.getStorageName());
        }

        S3KeyPrefixInformation s3KeyPrefixInformation;
        try (CloseableHttpClient client = httpClientHelper
            .createHttpClient(regServerAccessParamsDto.isTrustSelfSignedCertificate(), regServerAccessParamsDto.isDisableHostnameVerification()))
        {
            HttpGet request = new HttpGet(uriBuilder.build());
            request.addHeader("Accepts", DEFAULT_ACCEPT);

            // If SSL is enabled, set the client authentication header.
            if (regServerAccessParamsDto.isUseSsl())
            {
                request.addHeader(getAuthorizationHeader());
            }

            LOGGER.info(String.format("    HTTP GET URI: %s", request.getURI().toString()));
            LOGGER.info(String.format("    HTTP GET Headers: %s", Arrays.toString(request.getAllHeaders())));

            s3KeyPrefixInformation = getS3KeyPrefixInformation(httpClientOperations.execute(client, request));
        }

        LOGGER.info("Successfully retrieved S3 key prefix from the registration server.");
        LOGGER.info("    S3 key prefix: " + s3KeyPrefixInformation.getS3KeyPrefix());

        return s3KeyPrefixInformation;
    }

    /**
     * Returns an URI scheme.
     */
    protected String getUriScheme()
    {
        return regServerAccessParamsDto.isUseSsl() ? "https" : "http";
    }

    /**
     * Extracts an instance of the specified object class from the registration server response.
     *
     * @param response the HTTP response received from the registration server.
     * @param actionDescription the description of the action being performed with the registration server (to be used in an error message).
     * @param responseClass the class of the object expected to be returned by the registration server.
     *
     * @return the BusinessObjectData object extracted from the registration server response.
     */
    protected Object processXmlHttpResponse(CloseableHttpResponse response, String actionDescription, Class<?>... responseClass)
    {
        StatusLine responseStatusLine = response.getStatusLine();
        Object responseObject = null;
        String xmlResponse = "";
        HttpErrorResponseException errorException = null;

        try
        {
            if (responseStatusLine.getStatusCode() == 200)
            {
                // Request is successfully handled by the Server.
                xmlResponse = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8.name());
                InputStream inputStream = new ByteArrayInputStream(xmlResponse.getBytes(StandardCharsets.UTF_8));

                // Un-marshall the response to the specified object class.
                JAXBContext responseContext = JAXBContext.newInstance(responseClass);
                Unmarshaller responseUnmarshaller = responseContext.createUnmarshaller();
                responseObject = responseUnmarshaller.unmarshal(inputStream);
            }
            else
            {
                // Handle erroneous HTTP response.
                xmlResponse = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8.name());
                InputStream inputStream = new ByteArrayInputStream(xmlResponse.getBytes(StandardCharsets.UTF_8));

                // Un-marshall response to the ErrorInformation object.
                JAXBContext responseContext = JAXBContext.newInstance(ErrorInformation.class);
                Unmarshaller responseUnmarshaller = responseContext.createUnmarshaller();
                ErrorInformation errorInfo = (ErrorInformation) responseUnmarshaller.unmarshal(inputStream);

                errorException = new HttpErrorResponseException("Failed to " + actionDescription, errorInfo.getStatusCode(), errorInfo.getStatusDescription(),
                    errorInfo.getMessage());
            }
        }
        catch (IOException | JAXBException e)
        {
            LOGGER.warn("Failed to get or process HTTP response from the registration server.", e);
            LOGGER.warn(String.format("    HTTP Response Status: %s", responseStatusLine));
            LOGGER.warn(String.format("    HTTP Response: %s", xmlResponse));
            errorException =
                new HttpErrorResponseException("Failed to " + actionDescription, responseStatusLine.getStatusCode(), responseStatusLine.getReasonPhrase(),
                    xmlResponse);
        }
        finally
        {
            try
            {
                response.close();
            }
            catch (Exception ex)
            {
                LOGGER.warn("Unable to close HTTP response.", ex);
            }
        }

        // If we populated a response exception, then throw it to the caller.
        if (errorException != null)
        {
            throw errorException;
        }

        // Return the response.
        return responseObject;
    }

    /**
     * Gets the business object data status update response.
     *
     * @param response the HTTP response
     *
     * @return {@link BusinessObjectDataStatusUpdateResponse}
     */
    private BusinessObjectDataStatusUpdateResponse getBusinessObjectDataStatusUpdateResponse(CloseableHttpResponse response)
    {
        return (BusinessObjectDataStatusUpdateResponse) processXmlHttpResponse(response, "update business object data status",
            BusinessObjectDataStatusUpdateResponse.class);
    }

    /**
     * Extracts S3KeyPrefixInformation object from the registration server HTTP response.
     *
     * @param httpResponse the response received from the supported options.
     *
     * @return the S3KeyPrefixInformation object extracted from the registration server response.
     */
    private S3KeyPrefixInformation getS3KeyPrefixInformation(CloseableHttpResponse httpResponse)
    {
        return (S3KeyPrefixInformation) processXmlHttpResponse(httpResponse, "retrieve S3 key prefix from the registration server",
            S3KeyPrefixInformation.class);
    }

    /**
     * Extracts Storage object from the registration server HTTP response.
     *
     * @param httpResponse the response received from the supported options
     *
     * @return the Storage object extracted from the registration server response
     */
    private Storage getStorage(CloseableHttpResponse httpResponse)
    {
        return (Storage) processXmlHttpResponse(httpResponse, "retrieve storage information from the registration server", Storage.class);
    }
}
