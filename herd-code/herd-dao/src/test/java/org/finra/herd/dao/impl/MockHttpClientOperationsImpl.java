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
package org.finra.herd.dao.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.HttpClientOperations;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchResult;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataUploadCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataVersion;
import org.finra.herd.model.api.xml.BusinessObjectDataVersions;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitDownloadCredential;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * Mock implementation of HTTP client operations.
 */
public class MockHttpClientOperationsImpl implements HttpClientOperations
{
    public static final String HOSTNAME_LATEST_BDATA_VERSION_EXISTS = "testLatestBdataVersionExists";

    public static final String HOSTNAME_LATEST_BDATA_VERSION_EXISTS_IN_UPLOADING_STATE = "testLatestBdataVersionExistsInUploadingState";

    public static final String HOSTNAME_RESPOND_WITH_STATUS_CODE_200_AND_INVALID_CONTENT = "testRespondWithStatusCode200AndInvalidContent";

    public static final String HOSTNAME_THROW_IO_EXCEPTION_DURING_ADD_STORAGE_FILES = "testThrowIoExceptionDuringAddStorageFiles";

    public static final String HOSTNAME_THROW_IO_EXCEPTION_DURING_GET_STORAGE = "testThrowIoExceptionDuringGetStorage";

    public static final String HOSTNAME_THROW_IO_EXCEPTION_DURING_REGISTER_BDATA = "testThrowIoExceptionDuringRegisterBdata";

    public static final String HOSTNAME_THROW_IO_EXCEPTION_DURING_UPDATE_BDATA_STATUS = "testThrowIoExceptionDuringUpdateBdataStatus";

    public static final String HOSTNAME_THROW_IO_EXCEPTION = "testThrowIoException";

    private static final Logger LOGGER = LoggerFactory.getLogger(MockHttpClientOperationsImpl.class);

    @Autowired
    protected ConfigurationHelper configurationHelper;

    @Autowired
    private XmlHelper xmlHelper;

    @Override
    public CloseableHttpResponse execute(CloseableHttpClient httpClient, HttpUriRequest request) throws IOException, JAXBException
    {
        LOGGER.debug("request = " + request);

        ProtocolVersion protocolVersion = new ProtocolVersion("http", 1, 1);
        StatusLine statusLine = new BasicStatusLine(protocolVersion, HttpStatus.SC_OK, "Success");
        MockCloseableHttpResponse response = new MockCloseableHttpResponse(statusLine, false);

        // Find out which API's are being called and build an appropriate response.
        URI uri = request.getURI();
        if (request instanceof HttpGet)
        {
            if (uri.getPath().startsWith("/herd-app/rest/businessObjectData/"))
            {
                if (uri.getPath().endsWith("s3KeyPrefix"))
                {
                    buildGetS3KeyPrefixResponse(response, uri);
                }
                else if (uri.getPath().endsWith("versions"))
                {
                    buildGetBusinessObjectDataVersionsResponse(response, uri);
                }
                else if (uri.getPath().startsWith("/herd-app/rest/businessObjectData/upload/credential"))
                {
                    getBusinessObjectDataUploadCredentialResponse(response, uri);
                }
                else
                {
                    buildGetBusinessObjectDataResponse(response, uri);
                }
            }
            else if (uri.getPath().startsWith("/herd-app/rest/businessObjectDefinitions/"))
            {
                checkHostname(request, HOSTNAME_THROW_IO_EXCEPTION);
                buildGetBusinessObjectDefinitionResponse(response, uri);
            }
            else if (uri.getPath().startsWith("/herd-app/rest/storages/"))
            {
                checkHostname(request, HOSTNAME_THROW_IO_EXCEPTION_DURING_GET_STORAGE);
                buildGetStorageResponse(response, uri);
            }
            else if (uri.getPath().startsWith("/herd-app/rest/storageUnits/download/credential"))
            {
                getStorageUnitDownloadCredentialResponse(response, uri);
            }
        }
        else if (request instanceof HttpPost)
        {
            if (uri.getPath().startsWith("/herd-app/rest/businessObjectDataStorageFiles"))
            {
                checkHostname(request, HOSTNAME_THROW_IO_EXCEPTION_DURING_ADD_STORAGE_FILES);
                buildPostBusinessObjectDataStorageFilesResponse(response, uri);
            }
            else if (uri.getPath().equals("/herd-app/rest/businessObjectData"))
            {
                checkHostname(request, HOSTNAME_THROW_IO_EXCEPTION_DURING_REGISTER_BDATA);
                buildPostBusinessObjectDataResponse(response, uri);
            }
            else if (uri.getPath().equals("/herd-app/rest/businessObjectData/search"))
            {
                checkHostname(request, HOSTNAME_THROW_IO_EXCEPTION);
                buildSearchBusinessObjectDataResponse(response, uri);
            }
            else if (uri.getPath().startsWith("/herd-app/rest/businessObjectData/destroy"))
            {
                checkHostname(request, HOSTNAME_THROW_IO_EXCEPTION);
                buildPostBusinessObjectDataResponse(response, uri);
            }
        }
        else if (request instanceof HttpPut)
        {
            if (uri.getPath().startsWith("/herd-app/rest/businessObjectDataStatus/"))
            {
                checkHostname(request, HOSTNAME_THROW_IO_EXCEPTION_DURING_UPDATE_BDATA_STATUS);
                buildPutBusinessObjectDataStatusResponse(response, uri);
            }
        }

        // If requested, set response content to an invalid XML.
        if (HOSTNAME_RESPOND_WITH_STATUS_CODE_200_AND_INVALID_CONTENT.equals(request.getURI().getHost()))
        {
            response.setEntity(new StringEntity("invalid xml"));
        }

        LOGGER.debug("response = " + response);
        return response;
    }

    /**
     * Builds a business object data response.
     *
     * @param response the response.
     * @param uri the URI of the incoming request.
     *
     * @throws JAXBException if a JAXB error occurred.
     */
    private void buildGetBusinessObjectDataResponse(MockCloseableHttpResponse response, URI uri) throws JAXBException
    {
        Pattern pattern = Pattern.compile(
            "/herd-app/rest/businessObjectData/namespaces/(.*)/businessObjectDefinitionNames/(.*)/businessObjectFormatUsages/(.*)" +
                "/businessObjectFormatFileTypes/(.*).*");
        Matcher matcher = pattern.matcher(uri.getPath());
        if (matcher.find())
        {
            BusinessObjectData businessObjectData = new BusinessObjectData();
            businessObjectData.setNamespace(matcher.group(1));
            businessObjectData.setBusinessObjectDefinitionName(matcher.group(2));
            businessObjectData.setBusinessObjectFormatUsage(matcher.group(3));
            businessObjectData.setBusinessObjectFormatFileType(matcher.group(4));
            businessObjectData.setPartitionValue("2014-01-31");
            businessObjectData.setPartitionKey("PROCESS_DATE");
            businessObjectData.setAttributes(new ArrayList<Attribute>());
            businessObjectData.setBusinessObjectFormatVersion(0);
            businessObjectData.setLatestVersion(true);
            businessObjectData.setStatus(BusinessObjectDataStatusEntity.VALID);

            List<StorageUnit> storageUnits = new ArrayList<>();
            businessObjectData.setStorageUnits(storageUnits);

            StorageUnit storageUnit = new StorageUnit();
            storageUnits.add(storageUnit);

            storageUnit.setStorage(getNewStorage(StorageEntity.MANAGED_STORAGE));

            List<StorageFile> storageFiles = new ArrayList<>();
            storageUnit.setStorageFiles(storageFiles);
            storageUnit.setStorageUnitStatus(StorageUnitStatusEntity.ENABLED);

            List<String> localFiles = Arrays.asList("foo1.dat", "Foo2.dat", "FOO3.DAT", "folder/foo3.dat", "folder/foo2.dat", "folder/foo1.dat");
            for (String filename : localFiles)
            {
                StorageFile storageFile = new StorageFile();
                storageFiles.add(storageFile);
                storageFile.setFilePath(businessObjectData.getNamespace().toLowerCase().replace('_', '-') + "/exchange-a/" +
                    businessObjectData.getBusinessObjectFormatUsage().toLowerCase().replace('_', '-') + "/" +
                    businessObjectData.getBusinessObjectFormatFileType().toLowerCase().replace('_', '-') + "/" +
                    businessObjectData.getBusinessObjectDefinitionName().toLowerCase().replace('_', '-') + "/frmt-v" +
                    businessObjectData.getBusinessObjectFormatVersion() + "/data-v" + businessObjectData.getVersion() + "/" +
                    businessObjectData.getPartitionKey().toLowerCase().replace('_', '-') + "=" + businessObjectData.getPartitionValue() + "/" + filename);
                storageFile.setFileSizeBytes(1024L);
                storageFile.setRowCount(10L);
            }

            businessObjectData.setSubPartitionValues(new ArrayList<String>());
            businessObjectData.setId(1234);
            businessObjectData.setVersion(0);

            response.setEntity(getHttpEntity(businessObjectData));
        }
    }

    /**
     * Builds a business object definition response.
     *
     * @param response the response
     * @param uri the URI of the incoming request
     *
     * @throws JAXBException if a JAXB error occurred
     */
    private void buildGetBusinessObjectDefinitionResponse(MockCloseableHttpResponse response, URI uri) throws JAXBException
    {
        Pattern pattern = Pattern.compile("/herd-app/rest/businessObjectDefinitions/namespaces/(.*)/businessObjectDefinitionNames/(.*)");
        Matcher matcher = pattern.matcher(uri.getPath());
        if (matcher.find())
        {
            BusinessObjectDefinition businessObjectDefinition = new BusinessObjectDefinition();
            businessObjectDefinition.setNamespace(matcher.group(1));
            businessObjectDefinition.setBusinessObjectDefinitionName(matcher.group(2));
            businessObjectDefinition.setDisplayName("testBusinessObjectDefinitionDisplayName");
            response.setEntity(getHttpEntity(businessObjectDefinition));
        }
    }

    /**
     * Builds a business object definition response.
     *
     * @param response the response
     * @param uri the URI of the incoming request
     *
     * @throws JAXBException if a JAXB error occurred
     */
    private void buildSearchBusinessObjectDataResponse(MockCloseableHttpResponse response, URI uri) throws JAXBException
    {
        BusinessObjectDataSearchResult businessObjectDataSearchResult = new BusinessObjectDataSearchResult();

        // Build the response based on the pageNum.
        if (uri.getRawQuery().equals("pageNum=1"))
        {
            List<BusinessObjectData> businessObjectDataElements = new ArrayList<>();

            // Add business object data with sub-partitions.
            BusinessObjectData businessObjectDataWithSubPartitions = new BusinessObjectData();
            businessObjectDataWithSubPartitions.setNamespace("testNamespace");
            businessObjectDataWithSubPartitions.setBusinessObjectDefinitionName("testBusinessObjectDefinitionName");
            businessObjectDataWithSubPartitions.setBusinessObjectFormatUsage("testBusinessObjectFormatUsage");
            businessObjectDataWithSubPartitions.setBusinessObjectFormatFileType("testBusinessObjectFormatFileType");
            businessObjectDataWithSubPartitions.setBusinessObjectFormatVersion(9);
            businessObjectDataWithSubPartitions.setPartitionValue("primaryPartitionValue");
            businessObjectDataWithSubPartitions
                .setSubPartitionValues(Arrays.asList("subPartitionValue1", "subPartitionValue2", "subPartitionValue3", "subPartitionValue4"));
            businessObjectDataWithSubPartitions.setVersion(5);
            businessObjectDataElements.add(businessObjectDataWithSubPartitions);

            // Add business object data without sub-partitions.
            BusinessObjectData businessObjectDataWithoutSubPartitions = new BusinessObjectData();
            businessObjectDataWithoutSubPartitions.setNamespace("testNamespace");
            businessObjectDataWithoutSubPartitions.setBusinessObjectDefinitionName("testBusinessObjectDefinitionName");
            businessObjectDataWithoutSubPartitions.setBusinessObjectFormatUsage("testBusinessObjectFormatUsage");
            businessObjectDataWithoutSubPartitions.setBusinessObjectFormatFileType("testBusinessObjectFormatFileType");
            businessObjectDataWithoutSubPartitions.setBusinessObjectFormatVersion(9);
            businessObjectDataWithoutSubPartitions.setPartitionValue("primaryPartitionValue");
            businessObjectDataWithoutSubPartitions.setSubPartitionValues(null);
            businessObjectDataWithoutSubPartitions.setVersion(5);
            businessObjectDataElements.add(businessObjectDataWithoutSubPartitions);

            businessObjectDataSearchResult.setBusinessObjectDataElements(businessObjectDataElements);
        }

        response.setEntity(getHttpEntity(businessObjectDataSearchResult));
    }

    /**
     * Builds a business object data get versions response.
     *
     * @param response the response.
     * @param uri the URI of the incoming request.
     *
     * @throws JAXBException if a JAXB error occurred.
     */
    private void buildGetBusinessObjectDataVersionsResponse(MockCloseableHttpResponse response, URI uri) throws JAXBException
    {
        Pattern pattern = Pattern.compile("/herd-app/rest/businessObjectData(/namespaces/(?<namespace>.*?))?" +
            "/businessObjectDefinitionNames/(?<businessObjectDefinitionName>.*?)/businessObjectFormatUsages/(?<businessObjectFormatUsage>.*?)" +
            "/businessObjectFormatFileTypes/(?<businessObjectFormatFileType>.*?)" + "/versions");

        Matcher matcher = pattern.matcher(uri.getPath());

        if (matcher.find())
        {
            BusinessObjectDataVersions businessObjectDataVersions = new BusinessObjectDataVersions();

            if (HOSTNAME_LATEST_BDATA_VERSION_EXISTS.equals(uri.getHost()) || HOSTNAME_LATEST_BDATA_VERSION_EXISTS_IN_UPLOADING_STATE.equals(uri.getHost()))
            {
                BusinessObjectDataVersion businessObjectDataVersion = new BusinessObjectDataVersion();
                businessObjectDataVersions.getBusinessObjectDataVersions().add(businessObjectDataVersion);

                businessObjectDataVersion.setBusinessObjectDataKey(
                    new BusinessObjectDataKey(getGroup(matcher, "namespace"), getGroup(matcher, "businessObjectDefinitionName"),
                        getGroup(matcher, "businessObjectFormatUsage"), getGroup(matcher, "businessObjectFormatFileType"), 0, "2014-01-31", null, 0));

                businessObjectDataVersion.setStatus(
                    HOSTNAME_LATEST_BDATA_VERSION_EXISTS_IN_UPLOADING_STATE.equals(uri.getHost()) ? BusinessObjectDataStatusEntity.UPLOADING :
                        BusinessObjectDataStatusEntity.VALID);
            }

            response.setEntity(getHttpEntity(businessObjectDataVersions));
        }
    }

    /**
     * Builds a Get S3 Key Prefix response.
     *
     * @param response the response.
     * @param uri the URI of the incoming request.
     *
     * @throws JAXBException if a JAXB error occurred.
     */
    private void buildGetS3KeyPrefixResponse(MockCloseableHttpResponse response, URI uri) throws JAXBException
    {
        Pattern pattern = Pattern.compile("/herd-app/rest/businessObjectData(/namespaces/(?<namespace>.*?))?" +
            "/businessObjectDefinitionNames/(?<businessObjectDefinitionName>.*?)/businessObjectFormatUsages/(?<businessObjectFormatUsage>.*?)" +
            "/businessObjectFormatFileTypes/(?<businessObjectFormatFileType>.*?)/businessObjectFormatVersions/(?<businessObjectFormatVersion>.*?)" +
            "/s3KeyPrefix");
        Matcher matcher = pattern.matcher(uri.getPath());
        if (matcher.find())
        {
            S3KeyPrefixInformation s3KeyPrefixInformation = new S3KeyPrefixInformation();
            String namespace = getGroup(matcher, "namespace");
            namespace = namespace == null ? "testNamespace" : namespace;
            String businessObjectFormatUsage = getGroup(matcher, "businessObjectFormatUsage");
            String businessObjectFormatType = getGroup(matcher, "businessObjectFormatFileType");
            String businessObjectDefinitionName = getGroup(matcher, "businessObjectDefinitionName");
            String businessObjectFormatVersion = getGroup(matcher, "businessObjectFormatVersion");
            s3KeyPrefixInformation.setS3KeyPrefix(
                namespace.toLowerCase().replace('_', '-') + "/exchange-a/" + businessObjectFormatUsage.toLowerCase().replace('_', '-') + "/" +
                    businessObjectFormatType.toLowerCase().replace('_', '-') + "/" + businessObjectDefinitionName.toLowerCase().replace('_', '-') + "/frmt-v" +
                    businessObjectFormatVersion + "/data-v0/process-date=2014-01-31");

            response.setEntity(getHttpEntity(s3KeyPrefixInformation));
        }
    }

    /**
     * Builds a Get Storage response.
     *
     * @param response the response.
     * @param uri the URI of the incoming request.
     *
     * @throws JAXBException if a JAXB error occurred.
     */
    private void buildGetStorageResponse(MockCloseableHttpResponse response, URI uri) throws JAXBException
    {
        Pattern pattern = Pattern.compile("/herd-app/rest/storages/(.*)");
        Matcher matcher = pattern.matcher(uri.getPath());
        if (matcher.find())
        {
            Storage storage = getNewStorage(matcher.group(1));
            response.setEntity(getHttpEntity(storage));
        }
    }

    /**
     * Builds a business object data create response.
     *
     * @param response the response.
     * @param uri the URI of the incoming request.
     *
     * @throws JAXBException if a JAXB error occurred.
     */
    private void buildPostBusinessObjectDataResponse(MockCloseableHttpResponse response, URI uri) throws JAXBException
    {
        BusinessObjectData businessObjectData = new BusinessObjectData();
        List<StorageUnit> storageUnits = new ArrayList<>();
        businessObjectData.setStorageUnits(storageUnits);
        StorageUnit storageUnit = new StorageUnit();
        storageUnit.setStorageDirectory(new StorageDirectory("app-a/exchange-a/prc/txt/new-orders/frmt-v0/data-v0/process-date=2014-01-31"));
        storageUnits.add(storageUnit);
        response.setEntity(getHttpEntity(businessObjectData));
    }

    /**
     * Builds a business object data storage files create response.
     *
     * @param response the response.
     * @param uri the URI of the incoming request.
     *
     * @throws JAXBException if a JAXB error occurred.
     */
    private void buildPostBusinessObjectDataStorageFilesResponse(MockCloseableHttpResponse response, URI uri) throws JAXBException
    {
        BusinessObjectDataStorageFilesCreateResponse businessObjectDataStorageFilesCreateResponse = new BusinessObjectDataStorageFilesCreateResponse();
        response.setEntity(getHttpEntity(businessObjectDataStorageFilesCreateResponse));
    }

    /**
     * Builds a business object data status update response.
     *
     * @param response the response.
     * @param uri the URI of the incoming request.
     *
     * @throws JAXBException if a JAXB error occurred.
     */
    private void buildPutBusinessObjectDataStatusResponse(MockCloseableHttpResponse response, URI uri) throws JAXBException
    {
        BusinessObjectDataStatusUpdateResponse businessObjectDataStatusUpdateResponse = new BusinessObjectDataStatusUpdateResponse();
        response.setEntity(getHttpEntity(businessObjectDataStatusUpdateResponse));
    }

    /**
     * Check the hostname to see if we should throw an exception.
     *
     * @param request the HTTP request.
     * @param hostnameToThrowException the hostname that will cause an exception to be thrown.
     *
     * @throws IOException if the hostname suggests that we should thrown this exception.
     */
    private void checkHostname(HttpUriRequest request, String hostnameToThrowException) throws IOException
    {
        // We don't have mocking for HttpPost operations yet (e.g. business object data registration) - just exception throwing as needed.
        String hostname = request.getURI().getHost();
        if (hostname != null)
        {
            if (hostname.contains(hostnameToThrowException))
            {
                throw new IOException(hostnameToThrowException);
            }
        }
    }

    private void getBusinessObjectDataUploadCredentialResponse(MockCloseableHttpResponse response, URI uri) throws UnsupportedCharsetException, JAXBException
    {
        BusinessObjectDataUploadCredential businessObjectDataUploadCredential = new BusinessObjectDataUploadCredential();
        AwsCredential awsCredential = new AwsCredential();
        awsCredential.setAwsAccessKey(uri.toString());
        businessObjectDataUploadCredential.setAwsCredential(awsCredential);

        response.setEntity(getHttpEntity(businessObjectDataUploadCredential));
    }

    private String getGroup(Matcher matcher, String groupName)
    {
        try
        {
            return matcher.group(groupName);
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            return null;
        }
    }

    private HttpEntity getHttpEntity(Object content) throws UnsupportedCharsetException, JAXBException
    {
        String xml = xmlHelper.objectToXml(content);
        LOGGER.debug("xml = " + xml);
        ContentType contentType = ContentType.APPLICATION_XML.withCharset(StandardCharsets.UTF_8);
        return new StringEntity(xml, contentType);
    }

    /**
     * Gets a new storage object with the specified information.
     *
     * @param storageName the storage name.
     *
     * @return the newly created storage.
     */
    private Storage getNewStorage(String storageName)
    {
        Storage storage = new Storage();
        storage.setName(storageName);
        storage.setStoragePlatformName(StoragePlatformEntity.S3);

        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucket"));
        /*
         * Set the KMS key attribute if the storage name contains ignore case the word "KMS" (ex. S3_MANAGED_KMS)
         */
        if (storageName.toLowerCase().contains("kms"))
        {
            attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID), "testKmsKeyId"));
        }
        storage.setAttributes(attributes);

        return storage;
    }

    private void getStorageUnitDownloadCredentialResponse(MockCloseableHttpResponse response, URI uri) throws UnsupportedCharsetException, JAXBException
    {
        StorageUnitDownloadCredential storageUnitDownloadCredential = new StorageUnitDownloadCredential();
        AwsCredential awsCredential = new AwsCredential();
        awsCredential.setAwsAccessKey(uri.toString());
        storageUnitDownloadCredential.setAwsCredential(awsCredential);

        response.setEntity(getHttpEntity(storageUnitDownloadCredential));
    }
}
