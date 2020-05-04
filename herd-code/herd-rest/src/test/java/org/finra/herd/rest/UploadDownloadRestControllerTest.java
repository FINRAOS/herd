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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFileKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSampleDataFileKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.DownloadBusinessObjectDataStorageFileSingleInitiationRequest;
import org.finra.herd.model.api.xml.DownloadBusinessObjectDataStorageFileSingleInitiationResponse;
import org.finra.herd.model.api.xml.DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest;
import org.finra.herd.model.api.xml.DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationResponse;
import org.finra.herd.model.api.xml.DownloadSingleInitiationResponse;
import org.finra.herd.model.api.xml.File;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.UploadBusinessObjectDefinitionSampleDataFileInitiationRequest;
import org.finra.herd.model.api.xml.UploadBusinessObjectDefinitionSampleDataFileInitiationResponse;
import org.finra.herd.model.api.xml.UploadSingleCredentialExtensionResponse;
import org.finra.herd.model.api.xml.UploadSingleInitiationRequest;
import org.finra.herd.model.api.xml.UploadSingleInitiationResponse;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.UploadDownloadService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;

/**
 * This class tests various functionality within the upload download REST controller.
 */
public class UploadDownloadRestControllerTest extends AbstractRestTest
{
    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private NotificationEventService notificationEventService;

    @InjectMocks
    private UploadDownloadRestController uploadDownloadRestController;

    @Mock
    private UploadDownloadService uploadDownloadService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testExtendUploadSingleCredentials()
    {
        // Create a response.
        UploadSingleCredentialExtensionResponse response =
            new UploadSingleCredentialExtensionResponse(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN,
                AWS_ASSUMED_ROLE_SESSION_EXPIRATION_TIME);

        // Mock the external calls.
        when(uploadDownloadService
            .extendUploadSingleCredentials(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION))
            .thenReturn(response);

        // Call the method under test.
        UploadSingleCredentialExtensionResponse result = uploadDownloadRestController
            .extendUploadSingleCredentials(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION);

        // Verify the external calls.
        verify(uploadDownloadService)
            .extendUploadSingleCredentials(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(response, result);
    }

    @Test
    public void testInitiateDownloadSingle()
    {
        // Create a business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(ID);

        // Create a response.
        DownloadSingleInitiationResponse response =
            new DownloadSingleInitiationResponse(businessObjectData, AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN,
                AWS_ASSUMED_ROLE_SESSION_EXPIRATION_TIME, AWS_PRE_SIGNED_URL);

        // Mock the external calls.
        when(uploadDownloadService
            .initiateDownloadSingle(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION))
            .thenReturn(response);

        // Call the method under test.
        DownloadSingleInitiationResponse result = uploadDownloadRestController
            .initiateDownloadSingle(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION);

        // Verify the external calls.
        verify(uploadDownloadService)
            .initiateDownloadSingle(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(response, result);
    }

    @Test
    public void testInitiateDownloadSingleSampleFile()
    {
        // Create a business object definition sample data file key.
        BusinessObjectDefinitionSampleDataFileKey businessObjectDefinitionSampleDataFileKey =
            new BusinessObjectDefinitionSampleDataFileKey(BDEF_NAMESPACE, BDEF_NAME, DIRECTORY_PATH, FILE_NAME);

        // Create a request.
        DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest request =
            new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest(businessObjectDefinitionSampleDataFileKey);

        // Create a response.
        DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationResponse response =
            new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationResponse(businessObjectDefinitionSampleDataFileKey, S3_BUCKET_NAME,
                AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, NO_AWS_KMS_KEY_ID, AWS_ASSUMED_ROLE_SESSION_EXPIRATION_TIME,
                AWS_PRE_SIGNED_URL);

        // Mock the external calls.
        when(uploadDownloadService.initiateDownloadSingleSampleFile(request)).thenReturn(response);

        // Call the method under test.
        DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationResponse result = uploadDownloadRestController.initiateDownloadSingleSampleFile(request);

        // Verify the external calls.
        verify(uploadDownloadService).initiateDownloadSingleSampleFile(request);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(response, result);
    }

    @Test
    public void testInitiateUploadSampleFile()
    {
        // Create a business object data key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create a request.
        UploadBusinessObjectDefinitionSampleDataFileInitiationRequest request =
            new UploadBusinessObjectDefinitionSampleDataFileInitiationRequest(businessObjectDefinitionKey);

        // Create a response.
        UploadBusinessObjectDefinitionSampleDataFileInitiationResponse response =
            new UploadBusinessObjectDefinitionSampleDataFileInitiationResponse(businessObjectDefinitionKey, S3_BUCKET_NAME, S3_ENDPOINT, S3_KEY_PREFIX,
                AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, NO_AWS_KMS_KEY_ID, AWS_ASSUMED_ROLE_SESSION_EXPIRATION_TIME);

        // Mock the external calls.
        when(uploadDownloadService.initiateUploadSampleFile(request)).thenReturn(response);

        // Call the method under test.
        UploadBusinessObjectDefinitionSampleDataFileInitiationResponse result = uploadDownloadRestController.initiateUploadSampleFile(request);

        // Verify the external calls.
        verify(uploadDownloadService).initiateUploadSampleFile(request);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(response, result);
    }

    @Test
    public void testInitiateUploadSingle()
    {
        // Create business object format keys.
        BusinessObjectFormatKey sourceBusinessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);
        BusinessObjectFormatKey targetBusinessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2);

        // Create a file object.
        File file = new File(LOCAL_FILE, FILE_SIZE);

        // Create a request.
        UploadSingleInitiationRequest request = new UploadSingleInitiationRequest(sourceBusinessObjectFormatKey, targetBusinessObjectFormatKey,
            Lists.newArrayList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1)), file, STORAGE_NAME);

        // Create business object data keys.
        BusinessObjectDataKey sourceBusinessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);
        BusinessObjectDataKey targetBusinessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, PARTITION_VALUE_2,
                SUBPARTITION_VALUES_2, DATA_VERSION_2);

        // Create a business object data objects.
        BusinessObjectData sourceBusinessObjectData = new BusinessObjectData();
        sourceBusinessObjectData.setId(ID);
        sourceBusinessObjectData.setStatus(BDATA_STATUS);
        sourceBusinessObjectData.setStorageUnits(Lists.newArrayList(
            new StorageUnit(new Storage(STORAGE_NAME, STORAGE_PLATFORM_CODE, NO_ATTRIBUTES), NO_STORAGE_DIRECTORY, NO_STORAGE_FILES, STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS_HISTORY, NO_STORAGE_POLICY_TRANSITION_FAILED_ATTEMPTS, NO_RESTORE_EXPIRATION_ON)));
        BusinessObjectData targetBusinessObjectData = new BusinessObjectData();
        targetBusinessObjectData.setId(ID_2);
        targetBusinessObjectData.setStatus(BDATA_STATUS_2);
        targetBusinessObjectData.setStorageUnits(Lists.newArrayList(
            new StorageUnit(new Storage(STORAGE_NAME_2, STORAGE_PLATFORM_CODE, NO_ATTRIBUTES), NO_STORAGE_DIRECTORY, NO_STORAGE_FILES, STORAGE_UNIT_STATUS_2,
                NO_STORAGE_UNIT_STATUS_HISTORY, NO_STORAGE_POLICY_TRANSITION_FAILED_ATTEMPTS, NO_RESTORE_EXPIRATION_ON)));

        // Create a response.
        UploadSingleInitiationResponse response =
            new UploadSingleInitiationResponse(sourceBusinessObjectData, targetBusinessObjectData, file, UUID_VALUE, AWS_ASSUMED_ROLE_ACCESS_KEY,
                AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, AWS_ASSUMED_ROLE_SESSION_EXPIRATION_TIME, AWS_KMS_KEY_ID, STORAGE_NAME);

        // Mock the external calls.
        when(uploadDownloadService.initiateUploadSingle(request)).thenReturn(response);
        when(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectData)).thenReturn(sourceBusinessObjectDataKey);
        when(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectData)).thenReturn(targetBusinessObjectDataKey);

        // Call the method under test.
        UploadSingleInitiationResponse result = uploadDownloadRestController.initiateUploadSingle(request);

        // Verify the external calls.
        verify(uploadDownloadService).initiateUploadSingle(request);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(sourceBusinessObjectData);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(targetBusinessObjectData);
        verify(notificationEventService)
            .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN, sourceBusinessObjectDataKey,
                BDATA_STATUS, null);
        verify(notificationEventService)
            .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG, sourceBusinessObjectDataKey,
                BDATA_STATUS, null);
        verify(notificationEventService)
            .processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG, sourceBusinessObjectDataKey,
                STORAGE_NAME, STORAGE_UNIT_STATUS, null);
        verify(notificationEventService)
            .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN, targetBusinessObjectDataKey,
                BDATA_STATUS_2, null);
        verify(notificationEventService)
            .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG, targetBusinessObjectDataKey,
                BDATA_STATUS_2, null);
        verify(notificationEventService)
            .processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG, targetBusinessObjectDataKey,
                STORAGE_NAME_2, STORAGE_UNIT_STATUS_2, null);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(response, result);
    }

    @Test
    public void testInitiateDownloadSingleBusinessObjectDataStorageFile()
    {
        // Create a business object data storage file key.
        BusinessObjectDataStorageFileKey businessObjectDataStorageFileKey =
            new BusinessObjectDataStorageFileKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME, DIRECTORY_PATH + FILE_NAME);

        // Create a request.
        DownloadBusinessObjectDataStorageFileSingleInitiationRequest downloadBusinessObjectDataStorageFileSingleInitiationRequest =
            new DownloadBusinessObjectDataStorageFileSingleInitiationRequest(businessObjectDataStorageFileKey);

        // Create a response.
        DownloadBusinessObjectDataStorageFileSingleInitiationResponse downloadBusinessObjectDataStorageFileSingleInitiationResponse =
            new DownloadBusinessObjectDataStorageFileSingleInitiationResponse(businessObjectDataStorageFileKey, S3_BUCKET_NAME, AWS_ASSUMED_ROLE_ACCESS_KEY,
                AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, AWS_ASSUMED_ROLE_SESSION_EXPIRATION_TIME, AWS_PRE_SIGNED_URL);

        // Mock the external calls.
        when(uploadDownloadService.initiateDownloadSingleBusinessObjectDataStorageFile(downloadBusinessObjectDataStorageFileSingleInitiationRequest))
            .thenReturn(downloadBusinessObjectDataStorageFileSingleInitiationResponse);

        // Call the method under test.
        DownloadBusinessObjectDataStorageFileSingleInitiationResponse result =
            uploadDownloadRestController.initiateDownloadSingleBusinessObjectDataStorageFile(downloadBusinessObjectDataStorageFileSingleInitiationRequest);

        // Verify the external calls.
        verify(uploadDownloadService).initiateDownloadSingleBusinessObjectDataStorageFile(downloadBusinessObjectDataStorageFileSingleInitiationRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals("Download business object data storage file single initiation response does not equal result",
            downloadBusinessObjectDataStorageFileSingleInitiationResponse, result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataHelper, notificationEventService, uploadDownloadService);
    }
}
