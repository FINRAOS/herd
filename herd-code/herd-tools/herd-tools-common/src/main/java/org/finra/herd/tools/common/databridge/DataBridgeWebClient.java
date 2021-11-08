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

import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.finra.herd.sdk.api.BusinessObjectDataApi;
import org.finra.herd.sdk.api.BusinessObjectDataStatusApi;
import org.finra.herd.sdk.api.BusinessObjectDataStorageFileApi;
import org.finra.herd.sdk.api.StorageApi;
import org.finra.herd.sdk.invoker.ApiClient;
import org.finra.herd.sdk.invoker.ApiException;
import org.finra.herd.sdk.model.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.tools.common.dto.DataBridgeBaseManifestDto;
import org.finra.herd.tools.common.dto.ManifestFile;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.tools.common.dto.UploaderInputManifestDto;
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
    protected ApiClient apiClient;

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
     * @throws ApiException if an Api exception was encountered
     */
    @SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "We will use the standard carriage return character.")
    public BusinessObjectDataStorageFilesCreateResponse addStorageFiles(org.finra.herd.sdk.model.BusinessObjectDataKey businessObjectDataKey, UploaderInputManifestDto manifest,
                                                                        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, String storageName)
            throws ApiException, URISyntaxException {
        LOGGER.info("Adding storage files to the business object data ...");
        BusinessObjectDataStorageFileApi businessObjectDataStorageFileApi = new BusinessObjectDataStorageFileApi(createApiClient(regServerAccessParamsDto));

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

        BusinessObjectDataStorageFilesCreateResponse sdkBusinessObjectDataStorageFilesCreateResponse = businessObjectDataStorageFileApi.businessObjectDataStorageFileCreateBusinessObjectDataStorageFiles(request);

        LOGGER.info("Successfully added storage files to the registered business object data.");

        return sdkBusinessObjectDataStorageFilesCreateResponse;
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
     * @throws ApiException if an Api exception was encountered
     */
    public Storage getStorage(String storageName) throws ApiException, URISyntaxException {
        LOGGER.info(String.format("Retrieving storage information for \"%s\" storage name from the registration server...", storageName));
        StorageApi storageApi = new StorageApi(createApiClient(regServerAccessParamsDto));
        Storage sdkStorage = storageApi.storageGetStorage(storageName);

        LOGGER.info("Successfully retrieved storage information from the registration server.");
        LOGGER.info("    Storage name: " + sdkStorage.getName());
        LOGGER.info("    Attributes: ");

        for (Attribute attribute : sdkStorage.getAttributes())
        {
            LOGGER.info(String.format("        \"%s\"=\"%s\"", attribute.getName(), attribute.getValue()));
        }

        return sdkStorage;
    }

    /**
     * Pre-registers business object data with the registration server.
     *
     * @param manifest the uploader input manifest file
     * @param storageName the storage name
     * @param createNewVersion if not set, only initial version of the business object data is allowed to be created
     *
     * @return the business object data returned by the registration server.
     * @throws ApiException if an Api exception was encountered
     */
    @SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "We will use the standard carriage return character.")
    public BusinessObjectData preRegisterBusinessObjectData(UploaderInputManifestDto manifest, String storageName, Boolean createNewVersion)
            throws ApiException, URISyntaxException {
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
        List<BusinessObjectDataKey> businessObjectDataParents = manifest.getBusinessObjectDataParents();
        request.setBusinessObjectDataParents(businessObjectDataParents);

        BusinessObjectDataApi businessObjectDataApi = new BusinessObjectDataApi(createApiClient(regServerAccessParamsDto));
        BusinessObjectData businessObjectData = businessObjectDataApi.businessObjectDataCreateBusinessObjectData(request);
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
     * @throws ApiException if an Api exception was encountered
     */
    public BusinessObjectDataStatusUpdateResponse updateBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus)
            throws ApiException, URISyntaxException {
        BusinessObjectDataStatusUpdateRequest request = new BusinessObjectDataStatusUpdateRequest();
        request.setStatus(businessObjectDataStatus);

        BusinessObjectDataStatusApi businessObjectDataStatusApi = new BusinessObjectDataStatusApi(createApiClient(regServerAccessParamsDto));
        BusinessObjectDataStatusUpdateResponse sdkResponse;
        int subPartitions = Math.min(org.apache.commons.collections4.CollectionUtils.size(businessObjectDataKey.getSubPartitionValues()),
                BusinessObjectDataEntity.MAX_SUBPARTITIONS);
        switch (subPartitions) {
            case 1:
                sdkResponse = businessObjectDataStatusApi.businessObjectDataStatusUpdateBusinessObjectDataStatus1(
                        businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                        businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                        businessObjectDataKey.getBusinessObjectFormatVersion(),
                        businessObjectDataKey.getPartitionValue(),
                        businessObjectDataKey.getSubPartitionValues().get(0),
                        businessObjectDataKey.getBusinessObjectDataVersion(), request) ;
                break;
            case 2:
                sdkResponse = businessObjectDataStatusApi.businessObjectDataStatusUpdateBusinessObjectDataStatus2(
                        businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                        businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                        businessObjectDataKey.getBusinessObjectFormatVersion(),
                        businessObjectDataKey.getPartitionValue(),
                        businessObjectDataKey.getSubPartitionValues().get(0), businessObjectDataKey.getSubPartitionValues().get(1),
                        businessObjectDataKey.getBusinessObjectDataVersion(), request) ;
                break;
            case 3:
                sdkResponse = businessObjectDataStatusApi.businessObjectDataStatusUpdateBusinessObjectDataStatus3(
                        businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                        businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                        businessObjectDataKey.getBusinessObjectFormatVersion(),
                        businessObjectDataKey.getPartitionValue(),
                        businessObjectDataKey.getSubPartitionValues().get(0), businessObjectDataKey.getSubPartitionValues().get(1), businessObjectDataKey.getSubPartitionValues().get(2),
                        businessObjectDataKey.getBusinessObjectDataVersion(), request) ;
                break;

            case 4:
                sdkResponse = businessObjectDataStatusApi.businessObjectDataStatusUpdateBusinessObjectDataStatus4(
                        businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                        businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                        businessObjectDataKey.getBusinessObjectFormatVersion(),
                        businessObjectDataKey.getPartitionValue(),
                        businessObjectDataKey.getSubPartitionValues().get(0), businessObjectDataKey.getSubPartitionValues().get(1),businessObjectDataKey.getSubPartitionValues().get(2),businessObjectDataKey.getSubPartitionValues().get(3),
                        businessObjectDataKey.getBusinessObjectDataVersion(), request) ;
                break;
            default:
                sdkResponse = businessObjectDataStatusApi.businessObjectDataStatusUpdateBusinessObjectDataStatus(
                        businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                        businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                        businessObjectDataKey.getBusinessObjectFormatVersion(),
                        businessObjectDataKey.getPartitionValue(),
                        businessObjectDataKey.getBusinessObjectDataVersion(), request) ;
        }
        LOGGER.info("Successfully updated status of the business object data.");
        return sdkResponse;
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
     * @throws ApiException if an Api exception was encountered
     */
    protected S3KeyPrefixInformation getS3KeyPrefix(DataBridgeBaseManifestDto manifest, Integer businessObjectDataVersion, Boolean createNewVersion) throws ApiException, URISyntaxException {
        LOGGER.info("Retrieving S3 key prefix from the registration server...");

        BusinessObjectDataApi businessObjectDataApi = new BusinessObjectDataApi(createApiClient(regServerAccessParamsDto));

        S3KeyPrefixInformation sdkResponse = businessObjectDataApi.businessObjectDataGetS3KeyPrefix(manifest.getNamespace(), manifest.getBusinessObjectDefinitionName(), manifest.getBusinessObjectFormatUsage(),
                manifest.getBusinessObjectFormatFileType(), Integer.valueOf(manifest.getBusinessObjectFormatVersion()),
                manifest.getPartitionKey(), manifest.getPartitionValue(), herdStringHelper.join(manifest.getSubPartitionValues(), "|", "\\"),
        businessObjectDataVersion, manifest.getStorageName(), createNewVersion);

        LOGGER.info("Successfully retrieved S3 key prefix from the registration server.");
        LOGGER.info("    S3 key prefix: " + sdkResponse.getS3KeyPrefix());

        return sdkResponse;
    }

    public ApiClient createApiClient(RegServerAccessParamsDto regServerAccessParamsDto/*, Boolean trustSelfSignedCertificate, Boolean disableHostnameVerification*/) throws URISyntaxException {
        String protocol = regServerAccessParamsDto.isUseSsl() ? "https" : "http";
        String basPath = new URIBuilder().setScheme(protocol).setHost(regServerAccessParamsDto.getRegServerHost())
                .setPort(regServerAccessParamsDto.getRegServerPort()).build().toString();

        apiClient.setBasePath(basPath + HERD_APP_REST_URI_PREFIX);
        if (regServerAccessParamsDto.isUseSsl()){
            apiClient.setUsername(regServerAccessParamsDto.getUsername());
            apiClient.setPassword(regServerAccessParamsDto.getPassword());
        }
        apiClient.selectHeaderAccept(new String[] {DEFAULT_ACCEPT});
        apiClient.selectHeaderContentType(new String[]{DEFAULT_CONTENT_TYPE});
        return apiClient;
    }

    public BusinessObjectDataKey getBusinessObjectDataKey(BusinessObjectData businessObjectData)
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();

        businessObjectDataKey.setNamespace(businessObjectData.getNamespace());
        businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectData.getBusinessObjectDefinitionName());
        businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectData.getBusinessObjectFormatUsage());
        businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectData.getBusinessObjectFormatFileType());
        businessObjectDataKey.setBusinessObjectFormatVersion(businessObjectData.getBusinessObjectFormatVersion());
        businessObjectDataKey.setPartitionValue(businessObjectData.getPartitionValue());
        businessObjectDataKey.setSubPartitionValues(businessObjectData.getSubPartitionValues());
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectData.getVersion());

        return businessObjectDataKey;
    }
}
