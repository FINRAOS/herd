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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributesUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailability;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdl;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDownloadCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKeys;
import org.finra.herd.model.api.xml.BusinessObjectDataRetentionInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataRetryStoragePolicyTransitionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchResult;
import org.finra.herd.model.api.xml.BusinessObjectDataUploadCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataVersion;
import org.finra.herd.model.api.xml.BusinessObjectDataVersions;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.api.xml.StorageUnitDownloadCredential;
import org.finra.herd.model.api.xml.StorageUnitUploadCredential;
import org.finra.herd.model.dto.BusinessObjectDataSearchResultPagingInfoDto;
import org.finra.herd.service.BusinessObjectDataService;
import org.finra.herd.service.StorageUnitService;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;

/**
 * This class tests functionality within the business object data REST controller.
 */
public class BusinessObjectDataRestControllerTest extends AbstractRestTest
{
    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @InjectMocks
    private BusinessObjectDataRestController businessObjectDataRestController;

    @Mock
    private BusinessObjectDataService businessObjectDataService;

    @Mock
    private HerdStringHelper herdStringHelper;

    @Mock
    private StorageUnitService storageUnitService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCheckBusinessObjectDataAvailability()
    {
        // Create a business object data availability request.
        BusinessObjectDataAvailabilityRequest businessObjectDataAvailabilityRequest = new BusinessObjectDataAvailabilityRequest();

        // Create a business object data availability response.
        BusinessObjectDataAvailability businessObjectDataAvailability = new BusinessObjectDataAvailability();

        // Mock the external calls.
        when(businessObjectDataService.checkBusinessObjectDataAvailability(businessObjectDataAvailabilityRequest)).thenReturn(businessObjectDataAvailability);

        // Call the method under test.
        BusinessObjectDataAvailability result = businessObjectDataRestController.checkBusinessObjectDataAvailability(businessObjectDataAvailabilityRequest);

        // Verify the external calls.
        verify(businessObjectDataService).checkBusinessObjectDataAvailability(businessObjectDataAvailabilityRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAvailability, result);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityCollection()
    {
        // Create a business object data availability collection request.
        BusinessObjectDataAvailabilityCollectionRequest businessObjectDataAvailabilityCollectionRequest = new BusinessObjectDataAvailabilityCollectionRequest();

        // Create a business object data availability collection response.
        BusinessObjectDataAvailabilityCollectionResponse businessObjectDataAvailabilityCollectionResponse =
            new BusinessObjectDataAvailabilityCollectionResponse();

        // Mock the external calls.
        when(businessObjectDataService.checkBusinessObjectDataAvailabilityCollection(businessObjectDataAvailabilityCollectionRequest))
            .thenReturn(businessObjectDataAvailabilityCollectionResponse);

        // Call the method under test.
        BusinessObjectDataAvailabilityCollectionResponse result =
            businessObjectDataRestController.checkBusinessObjectDataAvailabilityCollection(businessObjectDataAvailabilityCollectionRequest);

        // Verify the external calls.
        verify(businessObjectDataService).checkBusinessObjectDataAvailabilityCollection(businessObjectDataAvailabilityCollectionRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAvailabilityCollectionResponse, result);
    }

    @Test
    public void testCreateBusinessObjectData()
    {
        // Create a business object data create request.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = new BusinessObjectDataCreateRequest();

        // Create a business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(ID);

        // Mock the external calls.
        when(businessObjectDataService.createBusinessObjectData(businessObjectDataCreateRequest)).thenReturn(businessObjectData);

        // Call the method under test.
        BusinessObjectData result = businessObjectDataRestController.createBusinessObjectData(businessObjectDataCreateRequest);

        // Verify the external calls.
        verify(businessObjectDataService).createBusinessObjectData(businessObjectDataCreateRequest);
        verify(businessObjectDataDaoHelper).triggerNotificationsForCreateBusinessObjectData(businessObjectData);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectData, result);
    }

    @Test
    public void testDeleteBusinessObjectData()
    {
        // Create a list of business object data keys with all possible number of sub-partition values.
        List<BusinessObjectDataKey> businessObjectDataKeys = new ArrayList<>();
        for (int subPartitionValuesCount = 0; subPartitionValuesCount <= SUBPARTITION_VALUES.size(); subPartitionValuesCount++)
        {
            businessObjectDataKeys.add(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES.subList(0, subPartitionValuesCount), DATA_VERSION));
        }

        // Create a list of business object data instances one per business object data key.
        List<BusinessObjectData> businessObjectDataList = new ArrayList<>();
        for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys)
        {
            BusinessObjectData businessObjectData = new BusinessObjectData();
            businessObjectData.setNamespace(businessObjectDataKey.getNamespace());
            businessObjectData.setBusinessObjectDefinitionName(businessObjectDataKey.getBusinessObjectDefinitionName());
            businessObjectData.setBusinessObjectFormatUsage(businessObjectDataKey.getBusinessObjectFormatUsage());
            businessObjectData.setBusinessObjectFormatFileType(businessObjectDataKey.getBusinessObjectFormatFileType());
            businessObjectData.setBusinessObjectFormatVersion(businessObjectDataKey.getBusinessObjectFormatVersion());
            businessObjectData.setPartitionValue(businessObjectDataKey.getPartitionValue());
            businessObjectData.setSubPartitionValues(businessObjectDataKey.getSubPartitionValues());
            businessObjectData.setVersion(businessObjectDataKey.getBusinessObjectDataVersion());
            businessObjectDataList.add(businessObjectData);
        }

        // Mock the external calls.
        for (int subPartitionValuesCount = 0; subPartitionValuesCount <= SUBPARTITION_VALUES.size(); subPartitionValuesCount++)
        {
            when(businessObjectDataService.deleteBusinessObjectData(businessObjectDataKeys.get(subPartitionValuesCount), DELETE_FILES))
                .thenReturn(businessObjectDataList.get(subPartitionValuesCount));
        }

        // Call the methods under test and validate the results.
        assertEquals(businessObjectDataList.get(0), businessObjectDataRestController
            .deleteBusinessObjectData(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION,
                DELETE_FILES));
        assertEquals(businessObjectDataList.get(1), businessObjectDataRestController
            .deleteBusinessObjectData(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), DATA_VERSION, DELETE_FILES));
        assertEquals(businessObjectDataList.get(2), businessObjectDataRestController
            .deleteBusinessObjectData(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), DATA_VERSION, DELETE_FILES));
        assertEquals(businessObjectDataList.get(3), businessObjectDataRestController
            .deleteBusinessObjectData(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), DATA_VERSION, DELETE_FILES));
        assertEquals(businessObjectDataList.get(4), businessObjectDataRestController
            .deleteBusinessObjectData(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION, DELETE_FILES));

        // Verify the external calls.
        for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys)
        {
            verify(businessObjectDataService).deleteBusinessObjectData(businessObjectDataKey, DELETE_FILES);
        }
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDestroyBusinessObjectData()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a delimited list of sub-partition values.
        String delimitedSubPartitionValues = String.join("|", SUBPARTITION_VALUES);

        // Create a business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(ID);

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues)).thenReturn(SUBPARTITION_VALUES);
        when(businessObjectDataService.destroyBusinessObjectData(businessObjectDataKey)).thenReturn(businessObjectData);

        // Call the method under test.
        BusinessObjectData result = businessObjectDataRestController
            .destroyBusinessObjectData(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION,
                delimitedSubPartitionValues);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues);
        verify(businessObjectDataService).destroyBusinessObjectData(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectData, result);
    }

    @Test
    public void testGenerateBusinessObjectDataDdl()
    {
        // Create a business object data ddl request.
        BusinessObjectDataDdlRequest businessObjectDataDdlRequest = new BusinessObjectDataDdlRequest();

        // Create a business object data ddl response.
        BusinessObjectDataDdl businessObjectDataDdl = new BusinessObjectDataDdl();

        // Mock the external calls.
        when(businessObjectDataService.generateBusinessObjectDataDdl(businessObjectDataDdlRequest)).thenReturn(businessObjectDataDdl);

        // Call the method under test.
        BusinessObjectDataDdl result = businessObjectDataRestController.generateBusinessObjectDataDdl(businessObjectDataDdlRequest);

        // Verify the external calls.
        verify(businessObjectDataService).generateBusinessObjectDataDdl(businessObjectDataDdlRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataDdl, result);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlCollection()
    {
        // Create a business object data ddl collection request.
        BusinessObjectDataDdlCollectionRequest businessObjectDataDdlCollectionRequest = new BusinessObjectDataDdlCollectionRequest();

        // Create a business object data ddl collection response.
        BusinessObjectDataDdlCollectionResponse businessObjectDataDdlCollectionResponse = new BusinessObjectDataDdlCollectionResponse();

        // Mock the external calls.
        when(businessObjectDataService.generateBusinessObjectDataDdlCollection(businessObjectDataDdlCollectionRequest))
            .thenReturn(businessObjectDataDdlCollectionResponse);

        // Call the method under test.
        BusinessObjectDataDdlCollectionResponse result =
            businessObjectDataRestController.generateBusinessObjectDataDdlCollection(businessObjectDataDdlCollectionRequest);

        // Verify the external calls.
        verify(businessObjectDataService).generateBusinessObjectDataDdlCollection(businessObjectDataDdlCollectionRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataDdlCollectionResponse, result);
    }

    @Test
    public void testGetAllBusinessObjectDataByBusinessObjectDefinition()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create a list of business object data keys.
        BusinessObjectDataKeys businessObjectDataKeys = new BusinessObjectDataKeys(Arrays.asList(new BusinessObjectDataKey()));

        // Mock the external calls.
        when(businessObjectDataService.getAllBusinessObjectDataByBusinessObjectDefinition(businessObjectDefinitionKey)).thenReturn(businessObjectDataKeys);

        // Call the method under test.
        BusinessObjectDataKeys result = businessObjectDataRestController.getAllBusinessObjectDataByBusinessObjectDefinition(BDEF_NAMESPACE, BDEF_NAME);

        // Verify the external calls.
        verify(businessObjectDataService).getAllBusinessObjectDataByBusinessObjectDefinition(businessObjectDefinitionKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataKeys, result);
    }

    @Test
    public void testGetAllBusinessObjectDataByBusinessObjectFormat()
    {
        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Create a list of business object data keys.
        BusinessObjectDataKeys businessObjectDataKeys = new BusinessObjectDataKeys(Arrays.asList(new BusinessObjectDataKey()));

        // Mock the external calls.
        when(businessObjectDataService.getAllBusinessObjectDataByBusinessObjectFormat(businessObjectFormatKey)).thenReturn(businessObjectDataKeys);

        // Call the method under test.
        BusinessObjectDataKeys result = businessObjectDataRestController
            .getAllBusinessObjectDataByBusinessObjectFormat(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Verify the external calls.
        verify(businessObjectDataService).getAllBusinessObjectDataByBusinessObjectFormat(businessObjectFormatKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataKeys, result);
    }

    @Test
    public void testGetBusinessObjectData()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a delimited list of sub-partition values.
        String delimitedSubPartitionValues = String.join("|", SUBPARTITION_VALUES);

        // Create a business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(ID);

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues)).thenReturn(SUBPARTITION_VALUES);
        when(businessObjectDataService.getBusinessObjectData(businessObjectDataKey, PARTITION_KEY, BDATA_STATUS, INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY,
            INCLUDE_STORAGE_UNIT_STATUS_HISTORY)).thenReturn(businessObjectData);

        // Call the method under test.
        BusinessObjectData result = businessObjectDataRestController
            .getBusinessObjectData(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, PARTITION_VALUE,
                delimitedSubPartitionValues, FORMAT_VERSION, DATA_VERSION, BDATA_STATUS, INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY,
                INCLUDE_STORAGE_UNIT_STATUS_HISTORY);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues);
        verify(businessObjectDataService).getBusinessObjectData(businessObjectDataKey, PARTITION_KEY, BDATA_STATUS, INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY,
            INCLUDE_STORAGE_UNIT_STATUS_HISTORY);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectData, result);
    }

    @Test
    public void testGetBusinessObjectDataDownloadCredential()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a delimited list of sub-partition values.
        String delimitedSubPartitionValues = String.join("|", SUBPARTITION_VALUES);

        // Create an AWS credential.
        AwsCredential awsCredential = new AwsCredential(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN,
            AWS_ASSUMED_ROLE_SESSION_EXPIRATION_TIME);

        // Create a storage unit download credential.
        StorageUnitDownloadCredential storageUnitDownloadCredential = new StorageUnitDownloadCredential(awsCredential);

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues)).thenReturn(SUBPARTITION_VALUES);
        when(storageUnitService.getStorageUnitDownloadCredential(businessObjectDataKey, STORAGE_NAME)).thenReturn(storageUnitDownloadCredential);

        // Call the method under test.
        BusinessObjectDataDownloadCredential result = businessObjectDataRestController
            .getBusinessObjectDataDownloadCredential(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                DATA_VERSION, STORAGE_NAME, delimitedSubPartitionValues);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues);
        verify(storageUnitService).getStorageUnitDownloadCredential(businessObjectDataKey, STORAGE_NAME);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new BusinessObjectDataDownloadCredential(awsCredential), result);
    }

    @Test
    public void testGetBusinessObjectDataUploadCredential()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a delimited list of sub-partition values.
        String delimitedSubPartitionValues = String.join("|", SUBPARTITION_VALUES);

        // Create an AWS credential.
        AwsCredential awsCredential = new AwsCredential(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN,
            AWS_ASSUMED_ROLE_SESSION_EXPIRATION_TIME);

        // Create a storage unit download credential.
        StorageUnitUploadCredential storageUnitUploadCredential = new StorageUnitUploadCredential(awsCredential, AWS_KMS_KEY_ID);

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues)).thenReturn(SUBPARTITION_VALUES);
        when(storageUnitService.getStorageUnitUploadCredential(businessObjectDataKey, CREATE_NEW_VERSION, STORAGE_NAME))
            .thenReturn(storageUnitUploadCredential);

        // Call the method under test.
        BusinessObjectDataUploadCredential result = businessObjectDataRestController
            .getBusinessObjectDataUploadCredential(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                DATA_VERSION, CREATE_NEW_VERSION, STORAGE_NAME, delimitedSubPartitionValues);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues);
        verify(storageUnitService).getStorageUnitUploadCredential(businessObjectDataKey, CREATE_NEW_VERSION, STORAGE_NAME);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new BusinessObjectDataUploadCredential(awsCredential, AWS_KMS_KEY_ID), result);
    }

    @Test
    public void testGetBusinessObjectDataVersions()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a delimited list of sub-partition values.
        String delimitedSubPartitionValues = String.join("|", SUBPARTITION_VALUES);

        // Create a list of business object data versions.
        BusinessObjectDataVersions businessObjectDataVersions = new BusinessObjectDataVersions(Arrays.asList(new BusinessObjectDataVersion()));

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues)).thenReturn(SUBPARTITION_VALUES);
        when(businessObjectDataService.getBusinessObjectDataVersions(businessObjectDataKey)).thenReturn(businessObjectDataVersions);

        // Call the method under test.
        BusinessObjectDataVersions result = businessObjectDataRestController
            .getBusinessObjectDataVersions(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_VALUE, delimitedSubPartitionValues,
                FORMAT_VERSION, DATA_VERSION);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues);
        verify(businessObjectDataService).getBusinessObjectDataVersions(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataVersions, result);
    }

    @Test
    public void testGetS3KeyPrefix()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a delimited list of sub-partition values.
        String delimitedSubPartitionValues = String.join("|", SUBPARTITION_VALUES);

        // Create an S3 key prefix information.
        S3KeyPrefixInformation s3KeyPrefixInformation = new S3KeyPrefixInformation();

        // Mock a servlet request.
        ServletRequest servletRequest = mock(ServletRequest.class);
        when(servletRequest.getParameterMap()).thenReturn(new HashMap<>());

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues)).thenReturn(SUBPARTITION_VALUES);
        when(storageUnitService.getS3KeyPrefix(businessObjectDataKey, PARTITION_KEY, STORAGE_NAME, CREATE_NEW_VERSION)).thenReturn(s3KeyPrefixInformation);

        // Call the method under test.
        S3KeyPrefixInformation result = businessObjectDataRestController
            .getS3KeyPrefix(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY, PARTITION_VALUE,
                delimitedSubPartitionValues, DATA_VERSION, STORAGE_NAME, CREATE_NEW_VERSION, servletRequest);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues);
        verify(storageUnitService).getS3KeyPrefix(businessObjectDataKey, PARTITION_KEY, STORAGE_NAME, CREATE_NEW_VERSION);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(s3KeyPrefixInformation, result);
    }

    @Test
    public void testInvalidateUnregisteredBusinessObjectData()
    {
        // Create an invalidate unregistered business object data request.
        BusinessObjectDataInvalidateUnregisteredRequest businessObjectDataInvalidateUnregisteredRequest = new BusinessObjectDataInvalidateUnregisteredRequest();

        // Create an invalidate unregistered business object data request.
        BusinessObjectDataInvalidateUnregisteredResponse businessObjectDataInvalidateUnregisteredResponse =
            new BusinessObjectDataInvalidateUnregisteredResponse();

        // Mock the external calls.
        when(businessObjectDataService.invalidateUnregisteredBusinessObjectData(businessObjectDataInvalidateUnregisteredRequest))
            .thenReturn(businessObjectDataInvalidateUnregisteredResponse);

        // Call the method under test.
        BusinessObjectDataInvalidateUnregisteredResponse result =
            businessObjectDataRestController.invalidateUnregisteredBusinessObjectData(businessObjectDataInvalidateUnregisteredRequest);

        // Verify the external calls.
        verify(businessObjectDataService).invalidateUnregisteredBusinessObjectData(businessObjectDataInvalidateUnregisteredRequest);
        verify(businessObjectDataDaoHelper).triggerNotificationsForInvalidateUnregisteredBusinessObjectData(businessObjectDataInvalidateUnregisteredResponse);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataInvalidateUnregisteredResponse, result);
    }

    @Test
    public void testRestoreBusinessObjectData()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a delimited list of sub-partition values.
        String delimitedSubPartitionValues = String.join("|", SUBPARTITION_VALUES);

        // Create a business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(ID);

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues)).thenReturn(SUBPARTITION_VALUES);
        when(businessObjectDataService.restoreBusinessObjectData(businessObjectDataKey, EXPIRATION_IN_DAYS)).thenReturn(businessObjectData);

        // Call the method under test.
        BusinessObjectData result = businessObjectDataRestController
            .restoreBusinessObjectData(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION,
                delimitedSubPartitionValues, EXPIRATION_IN_DAYS);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues);
        verify(businessObjectDataService).restoreBusinessObjectData(businessObjectDataKey, EXPIRATION_IN_DAYS);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectData, result);
    }

    @Test
    public void testRetryStoragePolicyTransition()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a delimited list of sub-partition values.
        String delimitedSubPartitionValues = String.join("|", SUBPARTITION_VALUES);

        // Create a business object data retry storage policy transition request.
        BusinessObjectDataRetryStoragePolicyTransitionRequest businessObjectDataRetryStoragePolicyTransitionRequest =
            new BusinessObjectDataRetryStoragePolicyTransitionRequest();

        // Create a business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(ID);

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues)).thenReturn(SUBPARTITION_VALUES);
        when(businessObjectDataService.retryStoragePolicyTransition(businessObjectDataKey, businessObjectDataRetryStoragePolicyTransitionRequest))
            .thenReturn(businessObjectData);

        // Call the method under test.
        BusinessObjectData result = businessObjectDataRestController
            .retryStoragePolicyTransition(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION,
                delimitedSubPartitionValues, businessObjectDataRetryStoragePolicyTransitionRequest);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues);
        verify(businessObjectDataService).retryStoragePolicyTransition(businessObjectDataKey, businessObjectDataRetryStoragePolicyTransitionRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectData, result);
    }

    @Test
    public void testSearchBusinessObjectData()
    {
        // Create a business object data search request.
        BusinessObjectDataSearchRequest businessObjectDataSearchRequest = new BusinessObjectDataSearchRequest();

        // Create a business object data search response with paging information.
        BusinessObjectDataSearchResultPagingInfoDto businessObjectDataSearchResultPagingInfoDto =
            new BusinessObjectDataSearchResultPagingInfoDto(Long.valueOf(PAGE_NUMBER_ONE), Long.valueOf(PAGE_SIZE_ONE_THOUSAND), PAGE_COUNT,
                TOTAL_RECORDS_ON_PAGE, TOTAL_RECORD_COUNT, MAX_RESULTS_PER_PAGE, new BusinessObjectDataSearchResult());

        // Create a mocked HTTP servlet response.
        HttpServletResponse httpServletResponse = mock(HttpServletResponse.class);

        // Mock the external calls.
        when(businessObjectDataService.searchBusinessObjectData(PAGE_NUMBER_ONE, PAGE_SIZE_ONE_THOUSAND, businessObjectDataSearchRequest))
            .thenReturn(businessObjectDataSearchResultPagingInfoDto);

        // Call the method under test.
        BusinessObjectDataSearchResult result = businessObjectDataRestController
            .searchBusinessObjectData(PAGE_NUMBER_ONE, PAGE_SIZE_ONE_THOUSAND, businessObjectDataSearchRequest, httpServletResponse);

        // Verify the external calls.
        verify(businessObjectDataService).searchBusinessObjectData(PAGE_NUMBER_ONE, PAGE_SIZE_ONE_THOUSAND, businessObjectDataSearchRequest);
        verifyNoMoreInteractionsHelper();

        // Verify interactions with the mocked objects.
        verify(httpServletResponse).setHeader(HerdBaseController.HTTP_HEADER_PAGING_PAGE_NUM, String.valueOf(PAGE_NUMBER_ONE));
        verify(httpServletResponse).setHeader(HerdBaseController.HTTP_HEADER_PAGING_PAGE_SIZE, String.valueOf(PAGE_SIZE_ONE_THOUSAND));
        verify(httpServletResponse).setHeader(HerdBaseController.HTTP_HEADER_PAGING_PAGE_COUNT, String.valueOf(PAGE_COUNT));
        verify(httpServletResponse).setHeader(HerdBaseController.HTTP_HEADER_PAGING_TOTAL_RECORDS_ON_PAGE, String.valueOf(TOTAL_RECORDS_ON_PAGE));
        verify(httpServletResponse).setHeader(HerdBaseController.HTTP_HEADER_PAGING_TOTAL_RECORD_COUNT, String.valueOf(TOTAL_RECORD_COUNT));
        verify(httpServletResponse).setHeader(HerdBaseController.HTTP_HEADER_PAGING_MAX_RESULTS_PER_PAGE, String.valueOf(MAX_RESULTS_PER_PAGE));
        verifyNoMoreInteractions(httpServletResponse);

        // Validate the results.
        assertEquals(businessObjectDataSearchResultPagingInfoDto.getBusinessObjectDataSearchResult(), result);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributes()
    {
        // Create a list of business object data keys with all possible number of sub-partition values.
        List<BusinessObjectDataKey> businessObjectDataKeys = new ArrayList<>();
        for (int subPartitionValuesCount = 0; subPartitionValuesCount <= SUBPARTITION_VALUES.size(); subPartitionValuesCount++)
        {
            businessObjectDataKeys.add(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES.subList(0, subPartitionValuesCount), DATA_VERSION));
        }

        // Create a business object data attributes update request.
        BusinessObjectDataAttributesUpdateRequest businessObjectDataAttributesUpdateRequest =
            new BusinessObjectDataAttributesUpdateRequest(Collections.singletonList(new Attribute(ATTRIBUTE_NAME, ATTRIBUTE_VALUE)));

        // Create a list of business object data instances one per business object data key.
        List<BusinessObjectData> businessObjectDataList = new ArrayList<>();
        for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys)
        {
            BusinessObjectData businessObjectData = new BusinessObjectData();
            businessObjectData.setNamespace(businessObjectDataKey.getNamespace());
            businessObjectData.setBusinessObjectDefinitionName(businessObjectDataKey.getBusinessObjectDefinitionName());
            businessObjectData.setBusinessObjectFormatUsage(businessObjectDataKey.getBusinessObjectFormatUsage());
            businessObjectData.setBusinessObjectFormatFileType(businessObjectDataKey.getBusinessObjectFormatFileType());
            businessObjectData.setBusinessObjectFormatVersion(businessObjectDataKey.getBusinessObjectFormatVersion());
            businessObjectData.setPartitionValue(businessObjectDataKey.getPartitionValue());
            businessObjectData.setSubPartitionValues(businessObjectDataKey.getSubPartitionValues());
            businessObjectData.setVersion(businessObjectDataKey.getBusinessObjectDataVersion());
            businessObjectDataList.add(businessObjectData);
        }

        // Mock the external calls.
        for (int subPartitionValuesCount = 0; subPartitionValuesCount <= SUBPARTITION_VALUES.size(); subPartitionValuesCount++)
        {
            when(businessObjectDataService
                .updateBusinessObjectDataAttributes(businessObjectDataKeys.get(subPartitionValuesCount), businessObjectDataAttributesUpdateRequest))
                .thenReturn(businessObjectDataList.get(subPartitionValuesCount));
        }

        // Call the methods under test and validate the results.
        assertEquals(businessObjectDataList.get(0), businessObjectDataRestController
            .updateBusinessObjectDataAttributes(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                DATA_VERSION, businessObjectDataAttributesUpdateRequest));
        assertEquals(businessObjectDataList.get(1), businessObjectDataRestController
            .updateBusinessObjectDataAttributes(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), DATA_VERSION, businessObjectDataAttributesUpdateRequest));
        assertEquals(businessObjectDataList.get(2), businessObjectDataRestController
            .updateBusinessObjectDataAttributes(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), DATA_VERSION, businessObjectDataAttributesUpdateRequest));
        assertEquals(businessObjectDataList.get(3), businessObjectDataRestController
            .updateBusinessObjectDataAttributes(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), DATA_VERSION, businessObjectDataAttributesUpdateRequest));
        assertEquals(businessObjectDataList.get(4), businessObjectDataRestController
            .updateBusinessObjectDataAttributes(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION,
                businessObjectDataAttributesUpdateRequest));

        // Verify the external calls.
        for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys)
        {
            verify(businessObjectDataService).updateBusinessObjectDataAttributes(businessObjectDataKey, businessObjectDataAttributesUpdateRequest);
        }
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateBusinessObjectDataRetentionInformation()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a delimited list of sub-partition values.
        String delimitedSubPartitionValues = String.join("|", SUBPARTITION_VALUES);

        // Create a business object data retention information update request.
        BusinessObjectDataRetentionInformationUpdateRequest businessObjectDataRetentionInformationUpdateRequest =
            new BusinessObjectDataRetentionInformationUpdateRequest(RETENTION_EXPIRATION_DATE);

        // Create a business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(ID);

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues)).thenReturn(SUBPARTITION_VALUES);
        when(businessObjectDataService.updateBusinessObjectDataRetentionInformation(businessObjectDataKey, businessObjectDataRetentionInformationUpdateRequest))
            .thenReturn(businessObjectData);

        // Call the method under test.
        BusinessObjectData result = businessObjectDataRestController
            .updateBusinessObjectDataRetentionInformation(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                DATA_VERSION, delimitedSubPartitionValues, businessObjectDataRetentionInformationUpdateRequest);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues);
        verify(businessObjectDataService)
            .updateBusinessObjectDataRetentionInformation(businessObjectDataKey, businessObjectDataRetentionInformationUpdateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectData, result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataDaoHelper, businessObjectDataService, herdStringHelper, storageUnitService);
    }
}
