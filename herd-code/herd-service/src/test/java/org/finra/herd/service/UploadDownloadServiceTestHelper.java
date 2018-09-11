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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.core.helper.LoggingHelper;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.BusinessObjectDataStatusDao;
import org.finra.herd.dao.BusinessObjectFormatDaoTestHelper;
import org.finra.herd.dao.impl.MockStsOperationsImpl;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.DownloadSingleInitiationResponse;
import org.finra.herd.model.api.xml.File;
import org.finra.herd.model.api.xml.UploadSingleInitiationRequest;
import org.finra.herd.model.api.xml.UploadSingleInitiationResponse;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.impl.UploadDownloadHelperServiceImpl;

@Component
public class UploadDownloadServiceTestHelper
{
    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectDataServiceTestHelper businessObjectDataServiceTestHelper;

    @Autowired
    private BusinessObjectDataStatusDao businessObjectDataStatusDao;

    @Autowired
    private BusinessObjectDefinitionServiceTestHelper businessObjectDefinitionServiceTestHelper;

    @Autowired
    private BusinessObjectFormatDaoTestHelper businessObjectFormatDaoTestHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private LoggingHelper loggingHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private UploadDownloadService uploadDownloadService;

    /**
     * Create and persist database entities required for upload download testing.
     */
    public void createDatabaseEntitiesForUploadDownloadTesting()
    {
        createDatabaseEntitiesForUploadDownloadTesting(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
            AbstractServiceTest.FORMAT_FILE_TYPE_CODE, AbstractServiceTest.FORMAT_VERSION);
        createDatabaseEntitiesForUploadDownloadTesting(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME_2, AbstractServiceTest.FORMAT_USAGE_CODE_2,
            AbstractServiceTest.FORMAT_FILE_TYPE_CODE_2, AbstractServiceTest.FORMAT_VERSION_2);
    }

    /**
     * Create and persist database entities required for upload download testing.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format file type
     * @param businessObjectFormatFileType the business object format file type
     */
    public void createDatabaseEntitiesForUploadDownloadTesting(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion)
    {
        // Create a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, AbstractServiceTest.FORMAT_DESCRIPTION, AbstractServiceTest.FORMAT_DOCUMENT_SCHEMA, AbstractServiceTest.LATEST_VERSION_FLAG_SET,
                AbstractServiceTest.PARTITION_KEY);
    }

    /**
     * Creates a upload single initiation request.
     *
     * @return the newly created upload single initiation request
     */
    public UploadSingleInitiationRequest createUploadSingleInitiationRequest()
    {
        return createUploadSingleInitiationRequest(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
            AbstractServiceTest.FORMAT_FILE_TYPE_CODE, AbstractServiceTest.FORMAT_VERSION, AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME_2,
            AbstractServiceTest.FORMAT_USAGE_CODE_2, AbstractServiceTest.FORMAT_FILE_TYPE_CODE_2, AbstractServiceTest.FORMAT_VERSION_2,
            AbstractServiceTest.FILE_NAME);
    }

    /**
     * Creates a upload single initiation request.
     *
     * @param sourceNamespaceCode the source namespace code
     * @param sourceBusinessObjectDefinitionName the source business object definition name
     * @param sourceBusinessObjectFormatUsage the source business object usage
     * @param sourceBusinessObjectFormatFileType the source business object format file type
     * @param sourceBusinessObjectFormatVersion the source business object format version
     * @param targetNamespaceCode the target namespace code
     * @param targetBusinessObjectDefinitionName the target business object definition name
     * @param targetBusinessObjectFormatUsage the target business object usage
     * @param targetBusinessObjectFormatFileType the target business object format file type
     * @param targetBusinessObjectFormatVersion the target business object format version
     *
     * @return the newly created upload single initiation request
     */
    public UploadSingleInitiationRequest createUploadSingleInitiationRequest(String sourceNamespaceCode, String sourceBusinessObjectDefinitionName,
        String sourceBusinessObjectFormatUsage, String sourceBusinessObjectFormatFileType, Integer sourceBusinessObjectFormatVersion,
        String targetNamespaceCode, String targetBusinessObjectDefinitionName, String targetBusinessObjectFormatUsage,
        String targetBusinessObjectFormatFileType, Integer targetBusinessObjectFormatVersion)
    {
        return createUploadSingleInitiationRequest(sourceNamespaceCode, sourceBusinessObjectDefinitionName, sourceBusinessObjectFormatUsage,
            sourceBusinessObjectFormatFileType, sourceBusinessObjectFormatVersion, targetNamespaceCode, targetBusinessObjectDefinitionName,
            targetBusinessObjectFormatUsage, targetBusinessObjectFormatFileType, targetBusinessObjectFormatVersion, AbstractServiceTest.FILE_NAME);
    }

    /**
     * Creates a upload single initiation request.
     *
     * @param sourceNamespaceCode the source namespace code
     * @param sourceBusinessObjectDefinitionName the source business object definition name
     * @param sourceBusinessObjectFormatUsage the source business object usage
     * @param sourceBusinessObjectFormatFileType the source business object format file type
     * @param sourceBusinessObjectFormatVersion the source business object format version
     * @param targetNamespaceCode the target namespace code
     * @param targetBusinessObjectDefinitionName the target business object definition name
     * @param targetBusinessObjectFormatUsage the target business object usage
     * @param targetBusinessObjectFormatFileType the target business object format file type
     * @param targetBusinessObjectFormatVersion the target business object format version
     * @param fileName the file name
     *
     * @return the newly created upload single initiation request
     */
    public UploadSingleInitiationRequest createUploadSingleInitiationRequest(String sourceNamespaceCode, String sourceBusinessObjectDefinitionName,
        String sourceBusinessObjectFormatUsage, String sourceBusinessObjectFormatFileType, Integer sourceBusinessObjectFormatVersion,
        String targetNamespaceCode, String targetBusinessObjectDefinitionName, String targetBusinessObjectFormatUsage,
        String targetBusinessObjectFormatFileType, Integer targetBusinessObjectFormatVersion, String fileName)
    {
        UploadSingleInitiationRequest request = new UploadSingleInitiationRequest();

        request.setSourceBusinessObjectFormatKey(
            new BusinessObjectFormatKey(sourceNamespaceCode, sourceBusinessObjectDefinitionName, sourceBusinessObjectFormatUsage,
                sourceBusinessObjectFormatFileType, sourceBusinessObjectFormatVersion));
        request.setTargetBusinessObjectFormatKey(
            new BusinessObjectFormatKey(targetNamespaceCode, targetBusinessObjectDefinitionName, targetBusinessObjectFormatUsage,
                targetBusinessObjectFormatFileType, targetBusinessObjectFormatVersion));
        request.setBusinessObjectDataAttributes(businessObjectDefinitionServiceTestHelper.getNewAttributes());
        request.setFile(new File(fileName, AbstractServiceTest.FILE_SIZE_1_KB));

        return request;
    }

    /**
     * Creates the appropriate business object data entries for an upload.
     *
     * @param businessObjectDataStatusCode the target business object data status.
     *
     * @return the upload single initiation response created during the upload flow.
     */
    public UploadSingleInitiationResponse createUploadedFileData(String businessObjectDataStatusCode)
    {
        loggingHelper.setLogLevel(UploadDownloadHelperServiceImpl.class, LogLevel.OFF);

        // Create source and target business object formats database entities which are required to initiate an upload.
        createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(createUploadSingleInitiationRequest());

        // Complete the upload.
        uploadDownloadService.performCompleteUploadSingleMessage(
            resultUploadSingleInitiationResponse.getSourceBusinessObjectData().getStorageUnits().get(0).getStorageFiles().get(0).getFilePath());

        // Update the target business object data status to valid. Normally this would happen as part of the completion request, but since the status update
        // happens asynchronously, this will not happen within a unit test context which is why we are setting it explicitly.
        businessObjectDataDao.getBusinessObjectDataByAltKey(
            businessObjectDataHelper.getBusinessObjectDataKey(resultUploadSingleInitiationResponse.getTargetBusinessObjectData()))
            .setStatus(businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(businessObjectDataStatusCode));
        resultUploadSingleInitiationResponse.getTargetBusinessObjectData().setStatus(businessObjectDataStatusCode);

        // Return the initiate upload single response.
        return resultUploadSingleInitiationResponse;
    }

    /**
     * Validates a download single initiation response as compared to the upload initiation response.
     *
     * @param uploadSingleInitiationResponse the upload single initiation response.
     * @param downloadSingleInitiationResponse the download single initiation response.
     */
    public void validateDownloadSingleInitiationResponse(UploadSingleInitiationResponse uploadSingleInitiationResponse,
        DownloadSingleInitiationResponse downloadSingleInitiationResponse)
    {
        BusinessObjectData targetBusinessObjectData = uploadSingleInitiationResponse.getTargetBusinessObjectData();

        validateDownloadSingleInitiationResponse(targetBusinessObjectData.getNamespace(), targetBusinessObjectData.getBusinessObjectDefinitionName(),
            targetBusinessObjectData.getBusinessObjectFormatUsage(), targetBusinessObjectData.getBusinessObjectFormatFileType(),
            targetBusinessObjectData.getBusinessObjectFormatVersion(), targetBusinessObjectData.getAttributes(),
            targetBusinessObjectData.getStorageUnits().get(0).getStorageFiles().get(0).getFileSizeBytes(), downloadSingleInitiationResponse);
    }

    public void validateDownloadSingleInitiationResponse(String expectedNamespaceCode, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        List<Attribute> expectedAttributes, Long expectedFileSizeBytes, DownloadSingleInitiationResponse actualDownloadSingleInitiationResponse)
    {
        assertNotNull(actualDownloadSingleInitiationResponse);

        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(expectedNamespaceCode, expectedBusinessObjectDefinitionName, expectedBusinessObjectFormatUsage,
                expectedBusinessObjectFormatFileType, expectedBusinessObjectFormatVersion, BusinessObjectDataStatusEntity.VALID, expectedAttributes,
                StorageEntity.MANAGED_EXTERNAL_STORAGE, AbstractServiceTest.FILE_NAME, expectedFileSizeBytes,
                actualDownloadSingleInitiationResponse.getBusinessObjectData());

        assertNotNull("aws access key", actualDownloadSingleInitiationResponse.getAwsAccessKey());
        assertNotNull("aws secret key", actualDownloadSingleInitiationResponse.getAwsSecretKey());
        assertNotNull("aws session token", actualDownloadSingleInitiationResponse.getAwsSessionToken());
        assertNotNull("pre-signed URL", actualDownloadSingleInitiationResponse.getPreSignedUrl());
    }

    /**
     * Validates upload single initiation response contents against specified parameters.
     *
     * @param expectedSourceNamespaceCode the expected source namespace code
     * @param expectedSourceBusinessObjectDefinitionName the expected source business object definition name
     * @param expectedSourceBusinessObjectFormatUsage the expected source business object format usage
     * @param expectedSourceBusinessObjectFormatFileType the expected source business object format file type
     * @param expectedSourceBusinessObjectFormatVersion the expected source business object format version
     * @param expectedTargetNamespaceCode the expected target namespace code
     * @param expectedTargetBusinessObjectDefinitionName the expected target business object definition name
     * @param expectedTargetBusinessObjectFormatUsage the expected target business object format usage
     * @param expectedTargetBusinessObjectFormatFileType the expected target business object format file type
     * @param expectedTargetBusinessObjectFormatVersion the expected target business object format version
     * @param expectedAttributes the expected business object data attributes
     * @param expectedFileName the expected file name
     * @param expectedFileSizeBytes the expected file size in bytes
     * @param expectedTargetStorageName The expected target storage name. Optional. Defaults to configured {@link
     * org.finra.herd.model.dto.ConfigurationValue#S3_EXTERNAL_STORAGE_NAME_DEFAULT}
     * @param actualUploadSingleInitiationResponse the upload single initiation response to be validated
     */
    public void validateUploadSingleInitiationResponse(String expectedSourceNamespaceCode, String expectedSourceBusinessObjectDefinitionName,
        String expectedSourceBusinessObjectFormatUsage, String expectedSourceBusinessObjectFormatFileType, Integer expectedSourceBusinessObjectFormatVersion,
        String expectedTargetNamespaceCode, String expectedTargetBusinessObjectDefinitionName, String expectedTargetBusinessObjectFormatUsage,
        String expectedTargetBusinessObjectFormatFileType, Integer expectedTargetBusinessObjectFormatVersion, List<Attribute> expectedAttributes,
        String expectedFileName, Long expectedFileSizeBytes, String expectedTargetStorageName,
        UploadSingleInitiationResponse actualUploadSingleInitiationResponse)
    {
        if (expectedTargetStorageName == null)
        {
            expectedTargetStorageName = configurationHelper.getProperty(ConfigurationValue.S3_EXTERNAL_STORAGE_NAME_DEFAULT);
        }

        assertNotNull(actualUploadSingleInitiationResponse);

        // Validate source business object data.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(expectedSourceNamespaceCode, expectedSourceBusinessObjectDefinitionName, expectedSourceBusinessObjectFormatUsage,
                expectedSourceBusinessObjectFormatFileType, expectedSourceBusinessObjectFormatVersion, BusinessObjectDataStatusEntity.UPLOADING,
                expectedAttributes, StorageEntity.MANAGED_LOADING_DOCK_STORAGE, expectedFileName, expectedFileSizeBytes,
                actualUploadSingleInitiationResponse.getSourceBusinessObjectData());

        // Validate target business object data.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(expectedTargetNamespaceCode, expectedTargetBusinessObjectDefinitionName, expectedTargetBusinessObjectFormatUsage,
                expectedTargetBusinessObjectFormatFileType, expectedTargetBusinessObjectFormatVersion, BusinessObjectDataStatusEntity.UPLOADING,
                expectedAttributes, expectedTargetStorageName, expectedFileName, expectedFileSizeBytes,
                actualUploadSingleInitiationResponse.getTargetBusinessObjectData());

        // Validate the file element.
        assertNotNull(actualUploadSingleInitiationResponse.getFile());
        assertEquals(expectedFileName, actualUploadSingleInitiationResponse.getFile().getFileName());
        assertEquals(expectedFileSizeBytes, actualUploadSingleInitiationResponse.getFile().getFileSizeBytes());

        // Validate the source uuid element.
        assertEquals(actualUploadSingleInitiationResponse.getSourceBusinessObjectData().getPartitionValue(), actualUploadSingleInitiationResponse.getUuid());

        // Validate the target uuid element.
        assertEquals(actualUploadSingleInitiationResponse.getTargetBusinessObjectData().getPartitionValue(), actualUploadSingleInitiationResponse.getUuid());

        // Validate temporary security credentials.
        assertEquals(MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_ACCESS_KEY, actualUploadSingleInitiationResponse.getAwsAccessKey());
        assertEquals(MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SECRET_KEY, actualUploadSingleInitiationResponse.getAwsSecretKey());
        assertEquals(MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SESSION_TOKEN, actualUploadSingleInitiationResponse.getAwsSessionToken());

        assertEquals(expectedTargetStorageName, actualUploadSingleInitiationResponse.getTargetStorageName());

        // Validate KMS Key ID.
        assertEquals(storageHelper.getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID),
            storageDaoHelper.getStorageEntity(expectedTargetStorageName), true), actualUploadSingleInitiationResponse.getAwsKmsKeyId());
    }
}
