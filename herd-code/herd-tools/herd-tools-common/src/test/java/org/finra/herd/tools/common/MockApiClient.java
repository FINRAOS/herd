
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
package org.finra.herd.tools.common;

import com.google.gson.Gson;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.GenericType;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.sdk.invoker.ApiClient;
import org.finra.herd.sdk.invoker.ApiException;
import org.finra.herd.sdk.invoker.Pair;
import org.finra.herd.sdk.model.*;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.xml.bind.JAXBException;

import java.io.*;
import java.nio.charset.UnsupportedCharsetException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Mock implementation of HTTP client operations.
 */
public class MockApiClient extends ApiClient
{
    public static final String HOSTNAME_LATEST_BDATA_VERSION_EXISTS = "testLatestBdataVersionExists";

    public static final String HOSTNAME_LATEST_BDATA_VERSION_EXISTS_IN_UPLOADING_STATE = "testLatestBdataVersionExistsInUploadingState";

    public static final String HOSTNAME_RESPOND_WITH_STATUS_CODE_200_AND_INVALID_CONTENT = "testRespondWithStatusCode200AndInvalidContent";

    public static final String HOSTNAME_THROW_IO_EXCEPTION_DURING_ADD_STORAGE_FILES = "testThrowIoExceptionDuringAddStorageFiles";

    public static final String HOSTNAME_THROW_IO_EXCEPTION_DURING_GET_STORAGE = "testThrowIoExceptionDuringGetStorage";

    public static final String HOSTNAME_THROW_IO_EXCEPTION_DURING_REGISTER_BDATA = "testThrowIoExceptionDuringRegisterBdata";

    public static final String HOSTNAME_THROW_IO_EXCEPTION_DURING_UPDATE_BDATA_STATUS = "testThrowIoExceptionDuringUpdateBdataStatus";

    public static final String HOSTNAME_THROW_IO_EXCEPTION = "testThrowIoException";

    private static final Logger LOGGER = LoggerFactory.getLogger(org.finra.herd.dao.impl.MockHttpClientOperationsImpl.class);

    @Autowired
    protected ConfigurationHelper configurationHelper;

    public <T> T invokeAPI(String path, String method, List<Pair> queryParams, List<Pair> collectionQueryParams, Object body, Map<String, String> headerParams,
        Map<String, Object> formParams, String accept, String contentType, String[] authNames, GenericType<T> returnType) throws ApiException
    {
        LOGGER.debug("path = " + path);

        // If requested, set response content to an invalid XML.
        if (this.getBasePath().contains(HOSTNAME_RESPOND_WITH_STATUS_CODE_200_AND_INVALID_CONTENT))
        {
            //response.setEntityInputStream(toInputStream("invalid xml"));
            throw new ApiException(400, "invalid xml");
        }

        // Find out which API's are being called and build an appropriate response.
        if (method.equals("GET"))
        {
            if (path.startsWith("/businessObjectData/"))
            {
                if (path.endsWith("s3KeyPrefix"))
                {
                    return (T) buildGetS3KeyPrefix(path);
                }
                else if (path.endsWith("versions"))
                {
                    return (T) buildGetBusinessObjectDataVersions(path);
                }
                else
                {
                    return (T) buildGetBusinessObjectData(path);
                }
            }
            else if (path.startsWith("/businessObjectDefinitions/"))
            {
                checkHostname(HOSTNAME_THROW_IO_EXCEPTION);
                return (T) buildGetBusinessObjectDefinition(path);
            }
            else if (path.startsWith("/storages/"))
            {
                checkHostname(HOSTNAME_THROW_IO_EXCEPTION_DURING_GET_STORAGE);
                return (T) buildGetStorage(path);
            }
            else if (path.startsWith("/storageUnits/upload/credential"))
            {
                return (T) getStorageUnitUploadCredential(path, queryParams);
            }
            else if (path.startsWith("/storageUnits/download/credential"))
            {
                return (T) getStorageUnitDownloadCredential(path, queryParams);
            }
        }
        else if (method.equals("POST"))
        {
            if (path.startsWith("/businessObjectDataStorageFiles"))
            {
                checkHostname(HOSTNAME_THROW_IO_EXCEPTION_DURING_ADD_STORAGE_FILES);
                return (T) buildPostBusinessObjectDataStorageFiles(path);
            }
            else if (path.equals("/businessObjectData"))
            {
                checkHostname(HOSTNAME_THROW_IO_EXCEPTION_DURING_REGISTER_BDATA);
                return (T) buildPostBusinessObjectData(body);
            }
            else if (path.equals("/businessObjectData/search"))
            {
                checkHostname(HOSTNAME_THROW_IO_EXCEPTION);
                return (T) buildSearchBusinessObjectData(queryParams);
            }
            else if (path.startsWith("/businessObjectData/destroy"))
            {
                checkHostname(HOSTNAME_THROW_IO_EXCEPTION);
                return (T) buildDestroyBusinessObjectData();
            }
        }
        else if (method.equals("PUT"))
        {
            if (path.startsWith("/businessObjectDataStatus/"))
            {
                checkHostname(HOSTNAME_THROW_IO_EXCEPTION_DURING_UPDATE_BDATA_STATUS);
                return (T) buildPutBusinessObjectDataStatus(path);
            }
        }
        else
        {
            throw new ApiException(500, "unknown method type " + method);
        }
        return null;
    }

    /**
     * Builds a business object data response.
     *
     * @param path the path of the incoming request.
     */
    private BusinessObjectData buildGetBusinessObjectData(String path)
    {
        Pattern pattern = Pattern.compile(
            "/businessObjectData/namespaces/(.*)/businessObjectDefinitionNames/(.*)/businessObjectFormatUsages/(.*)" + "/businessObjectFormatFileTypes/(.*).*");
        Matcher matcher = pattern.matcher(path);
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
            businessObjectData.setVersion(0);
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
            businessObjectData.setId(1234L);

            return businessObjectData;
        }
        return null;
    }

    /**
     * Builds a business object definition response.
     *
     * @param path the path of the incoming request
     * @throws JAXBException if a JAXB error occurred
     */
    private BusinessObjectDefinition buildGetBusinessObjectDefinition(String path)
    {
        Pattern pattern = Pattern.compile("/businessObjectDefinitions/namespaces/(.*)/businessObjectDefinitionNames/(.*)");
        Matcher matcher = pattern.matcher(path);
        if (matcher.find())
        {
            BusinessObjectDefinition businessObjectDefinition = new BusinessObjectDefinition();
            businessObjectDefinition.setNamespace(matcher.group(1));
            businessObjectDefinition.setBusinessObjectDefinitionName(matcher.group(2));
            businessObjectDefinition.setDisplayName("testBusinessObjectDefinitionDisplayName");
            return businessObjectDefinition;
        }
        return null;
    }

    /**
     * Builds a business object definition response.
     *
     * @param queryParams the query params of the incoming request
     */
    private BusinessObjectDataSearchResult buildSearchBusinessObjectData(List<Pair> queryParams)
    {
        BusinessObjectDataSearchResult businessObjectDataSearchResult = new BusinessObjectDataSearchResult();

        // Build the response based on the pageNum.
        if (queryParams.stream().anyMatch(pair -> pair.getName().equals("pageNum") && pair.getValue().equals("1")))

        //if (uri.getRawQuery().equals("pageNum=1"))
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
            businessObjectDataWithSubPartitions.setSubPartitionValues(
                Arrays.asList("subPartitionValue1", "subPartitionValue2", "subPartitionValue3", "subPartitionValue4"));
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

        return businessObjectDataSearchResult;
    }

    /**
     * Builds a business object data get versions response.
     *
     * @param path the path of the incoming request.
     */
    private BusinessObjectDataVersions buildGetBusinessObjectDataVersions(String path) throws ApiException
    {
        Pattern pattern = Pattern.compile("/businessObjectData(/namespaces/(?<namespace>.*?))?" +
            "/businessObjectDefinitionNames/(?<businessObjectDefinitionName>.*?)/businessObjectFormatUsages/(?<businessObjectFormatUsage>.*?)" +
            "/businessObjectFormatFileTypes/(?<businessObjectFormatFileType>.*?)" + "/versions");

        Matcher matcher = pattern.matcher(path);
        BusinessObjectDataVersions businessObjectDataVersions = new BusinessObjectDataVersions();
        businessObjectDataVersions.setBusinessObjectDataVersions(new ArrayList<>());
        if (matcher.find())
        {
            if (this.getBasePath().contains(HOSTNAME_LATEST_BDATA_VERSION_EXISTS) ||
                this.getBasePath().contains(HOSTNAME_LATEST_BDATA_VERSION_EXISTS_IN_UPLOADING_STATE))
            {
                BusinessObjectDataVersion businessObjectDataVersion = new BusinessObjectDataVersion();
                businessObjectDataVersions.getBusinessObjectDataVersions().add(businessObjectDataVersion);

                businessObjectDataVersion.setBusinessObjectDataKey(
                    createBusinessObjectDataKey(getGroup(matcher, "namespace"), getGroup(matcher, "businessObjectDefinitionName"),
                        getGroup(matcher, "businessObjectFormatUsage"), getGroup(matcher, "businessObjectFormatFileType"), 0, "2014-01-31", null, 0));

                businessObjectDataVersion.setStatus(
                    this.getBasePath().contains(HOSTNAME_LATEST_BDATA_VERSION_EXISTS_IN_UPLOADING_STATE) ? BusinessObjectDataStatusEntity.UPLOADING :
                        BusinessObjectDataStatusEntity.VALID);
            }

            return businessObjectDataVersions;
        }
        return null;
    }

    /**
     * Builds a Get S3 Key Prefix response.
     *
     * @param path the path of the incoming request.
     * @throws JAXBException if a JAXB error occurred.
     */
    private S3KeyPrefixInformation buildGetS3KeyPrefix(String path)
    {
        Pattern pattern = Pattern.compile("/businessObjectData(/namespaces/(?<namespace>.*?))?" +
            "/businessObjectDefinitionNames/(?<businessObjectDefinitionName>.*?)/businessObjectFormatUsages/(?<businessObjectFormatUsage>.*?)" +
            "/businessObjectFormatFileTypes/(?<businessObjectFormatFileType>.*?)/businessObjectFormatVersions/(?<businessObjectFormatVersion>.*?)" +
            "/s3KeyPrefix");
        Matcher matcher = pattern.matcher(path);
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

            return s3KeyPrefixInformation;
        }
        return null;
    }

    /**
     * Builds a Get Storage response.
     *
     * @param path the path of the incoming request.
     */
    private Storage buildGetStorage(String path)
    {
        Pattern pattern = Pattern.compile("/storages/(.*)");
        Matcher matcher = pattern.matcher(path);
        if (matcher.find())
        {
            return getNewStorage(matcher.group(1));
        }
        return null;
    }

    private <T> T convertType(Object sdkObject, Class targetClass)
    {
        Gson gson = new Gson();
        String tmp = gson.toJson(sdkObject);
        return (T) gson.fromJson(tmp, targetClass);
    }

    private BusinessObjectData buildDestroyBusinessObjectData()
    {
        BusinessObjectData businessObjectData = new BusinessObjectData();
        List<StorageUnit> storageUnits = new ArrayList<>();
        businessObjectData.setStorageUnits(storageUnits);
        StorageUnit storageUnit = new StorageUnit();
        StorageDirectory storageDirectory = new StorageDirectory();
        storageDirectory.setDirectoryPath("app-a/exchange-a/prc/txt/new-orders/frmt-v0/data-v0/process-date=2014-01-31");
        storageUnit.setStorageDirectory(storageDirectory);
        storageUnits.add(storageUnit);
        return businessObjectData;
    }

    private BusinessObjectData buildPostBusinessObjectData(Object body)
    {
        BusinessObjectDataCreateRequest request = convertType(body, BusinessObjectDataCreateRequest.class);
        BusinessObjectData businessObjectData = new BusinessObjectData();
        List<StorageUnit> storageUnits = new ArrayList<>();
        businessObjectData.setStorageUnits(storageUnits);
        businessObjectData.setNamespace(request.getNamespace());
        businessObjectData.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName());
        businessObjectData.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage());
        businessObjectData.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType());
        businessObjectData.setBusinessObjectFormatVersion(request.getBusinessObjectFormatVersion());
        businessObjectData.setPartitionKey(request.getPartitionKey());
        businessObjectData.setPartitionValue(request.getPartitionValue());
        businessObjectData.setSubPartitionValues(request.getSubPartitionValues());
        businessObjectData.setVersion(0);
        StorageUnit storageUnit = new StorageUnit();
        StorageDirectory storageDirectory = new StorageDirectory();
        storageDirectory.setDirectoryPath("app-a/exchange-a/prc/txt/new-orders/frmt-v0/data-v0/process-date=2014-01-31");
        storageUnit.setStorageDirectory(storageDirectory);
        storageUnits.add(storageUnit);
        return businessObjectData;
    }

    /**
     * Builds a business object data storage files create response.
     *
     * @param path the path of the incoming request.
     * @throws JAXBException if a JAXB error occurred.
     */
    private BusinessObjectDataStorageFilesCreateResponse buildPostBusinessObjectDataStorageFiles(String path)
    {
        return new BusinessObjectDataStorageFilesCreateResponse();
    }

    /**
     * Builds a business object data status update response.
     *
     * @param path the path of the incoming request.
     * @throws JAXBException if a JAXB error occurred.
     */
    private BusinessObjectDataStatusUpdateResponse buildPutBusinessObjectDataStatus(String path)
    {
        return new BusinessObjectDataStatusUpdateResponse();
    }

    /**
     * Check the hostname to see if we should throw an exception.
     *
     * @param hostnameToThrowException the hostname that will cause an exception to be thrown.
     * @throws IOException if the hostname suggests that we should thrown this exception.
     */
    private void checkHostname(String hostnameToThrowException)
    {
        // We don't have mocking for HttpPost operations yet (e.g. business object data registration) - just exception throwing as needed.
        String basePath = this.getBasePath();
        if (basePath != null)
        {
            if (basePath.contains(hostnameToThrowException))
            {
                throw new ClientHandlerException(hostnameToThrowException);
            }
        }
    }

    private StorageUnitUploadCredential getStorageUnitUploadCredential(String path, List<Pair> queryParams) throws UnsupportedCharsetException
    {
        StorageUnitUploadCredential storageUnitUploadCredential = new StorageUnitUploadCredential();
        AwsCredential awsCredential = new AwsCredential();
        awsCredential.setAwsAccessKey(this.getBasePath() + path + getSubPartitionValues(queryParams));
        storageUnitUploadCredential.setAwsCredential(awsCredential);

        return storageUnitUploadCredential;
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

    /**
     * Gets a new storage object with the specified information.
     *
     * @param storageName the storage name.
     * @return the newly created storage.
     */
    private Storage getNewStorage(String storageName)
    {
        Storage storage = new Storage();
        storage.setName(storageName);
        storage.setStoragePlatformName(StoragePlatformEntity.S3);

        List<Attribute> attributes = new ArrayList<>();
        attributes.add(createAttribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucket"));
        /*
         * Set the KMS key attribute if the storage name contains ignore case the word "KMS" (ex. S3_MANAGED_KMS)
         */
        if (storageName.toLowerCase().contains("kms"))
        {

            attributes.add(createAttribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID), "testKmsKeyId"));
        }
        storage.setAttributes(attributes);

        return storage;
    }

    private StorageUnitDownloadCredential getStorageUnitDownloadCredential(String path, List<Pair> queryParams) throws UnsupportedCharsetException
    {
        StorageUnitDownloadCredential storageUnitDownloadCredential = new StorageUnitDownloadCredential();
        AwsCredential awsCredential = new AwsCredential();
        awsCredential.setAwsAccessKey(this.getBasePath() + path + getSubPartitionValues(queryParams));
        storageUnitDownloadCredential.setAwsCredential(awsCredential);

        return storageUnitDownloadCredential;
    }

    private String getSubPartitionValues(List<Pair> queryParams)
    {
        if (queryParams == null || queryParams.size() == 0)
        {
            return "";
        }
        else
        {
            Optional<Pair> optional = queryParams.stream().filter(pair -> pair.getName().equals("subPartitionValues")).findFirst();
            return optional.map(pair -> "?" + pair.getName() + "=" + this.escapeString(pair.getValue())).orElse("");
        }
    }

    private Attribute createAttribute(String name, String value)
    {
        Attribute attribute = new Attribute();
        attribute.setName(name);
        attribute.setValue(value);
        return attribute;
    }

    private BusinessObjectDataKey createBusinessObjectDataKey(String namespace, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue, List<String> subPartitionValues,
        Integer businessObjectDataVersion)
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(namespace);
        businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        businessObjectDataKey.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectDataKey.setPartitionValue(partitionValue);
        businessObjectDataKey.setSubPartitionValues(subPartitionValues);
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataVersion);

        return businessObjectDataKey;
    }
}

