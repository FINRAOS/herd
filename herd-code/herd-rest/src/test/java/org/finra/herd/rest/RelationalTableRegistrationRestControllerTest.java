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

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.model.api.xml.RelationalTableRegistrationDeleteResponse;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.service.RelationalTableRegistrationService;
import org.finra.herd.service.impl.BusinessObjectDataServiceImpl;

public class RelationalTableRegistrationRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private RelationalTableRegistrationRestController relationalTableRegistrationRestController;

    @Mock
    private RelationalTableRegistrationService relationalTableRegistrationService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDeleteRelationalTableRegistration()
    {
        // Create the objects needed for the mock test.
        BusinessObjectData businessObjectData1 = new BusinessObjectData();
        businessObjectData1.setId(businessObjectData1.getId());
        businessObjectData1.setNamespace(BDEF_NAMESPACE);
        businessObjectData1.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectData1.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectData1.setVersion(0);
        businessObjectData1.setStatus("VALID");
        businessObjectData1.setLatestVersion(true);
        businessObjectData1.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        businessObjectData1.setPartitionValue(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE);
        businessObjectData1.setPartitionKey(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY);

        BusinessObjectData businessObjectData2 = new BusinessObjectData();
        businessObjectData2.setId(businessObjectData2.getId());
        businessObjectData2.setNamespace(BDEF_NAMESPACE);
        businessObjectData2.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectData2.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectData2.setVersion(0);
        businessObjectData2.setStatus("VALID");
        businessObjectData2.setLatestVersion(true);
        businessObjectData2.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        businessObjectData2.setPartitionValue(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE);
        businessObjectData2.setPartitionKey(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY);

        List<BusinessObjectData> businessObjectDataList = new ArrayList<>();
        businessObjectDataList.add(businessObjectData1);
        businessObjectDataList.add(businessObjectData2);

        RelationalTableRegistrationDeleteResponse expectedRelationalTableRegistrationDeleteResponse =
            new RelationalTableRegistrationDeleteResponse(businessObjectDataList);

        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, null);

        // Setup external calls.
        when(relationalTableRegistrationService.deleteRelationalTableRegistration(businessObjectFormatKey))
            .thenReturn(expectedRelationalTableRegistrationDeleteResponse);

        // Call the method being tested.
        RelationalTableRegistrationDeleteResponse relationalTableRegistrationDeleteResponse =
            relationalTableRegistrationRestController.deleteRelationalTableRegistration(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE);

        // Verify the external calls.
        verify(relationalTableRegistrationService).deleteRelationalTableRegistration(businessObjectFormatKey);
        verifyNoMoreInteractions(relationalTableRegistrationService);

        // Validate the returned object.
        assertEquals(expectedRelationalTableRegistrationDeleteResponse, relationalTableRegistrationDeleteResponse);
    }

    @Test
    public void testRelationalTableRegistrationRestController()
    {
        RelationalTableRegistrationCreateRequest createRequest = new RelationalTableRegistrationCreateRequest();
        createRequest.setNamespace(BDEF_NAMESPACE);
        createRequest.setDataProviderName(DATA_PROVIDER_NAME);
        createRequest.setBusinessObjectDefinitionName(BDEF_NAME);
        createRequest.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        createRequest.setRelationalTableName(RELATIONAL_TABLE_NAME);
        createRequest.setStorageName(STORAGE_NAME);

        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(businessObjectData.getId());
        businessObjectData.setNamespace(BDEF_NAMESPACE);
        businessObjectData.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectData.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectData.setVersion(0);
        businessObjectData.setStatus("VALID");
        businessObjectData.setLatestVersion(true);
        businessObjectData.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        businessObjectData.setPartitionValue(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE);
        businessObjectData.setPartitionKey(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY);


        when(relationalTableRegistrationService.createRelationalTableRegistration(createRequest, APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE))
            .thenReturn(businessObjectData);

        BusinessObjectData returnedBusinessObjectData =
            relationalTableRegistrationRestController.createRelationalTableRegistration(createRequest, APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);

        // Verify the external calls.
        verify(relationalTableRegistrationService).createRelationalTableRegistration(createRequest, APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
        verifyNoMoreInteractions(relationalTableRegistrationService);

        // Validate the returned object.
        assertEquals(businessObjectData, returnedBusinessObjectData);
    }

    @Test
    public void testRelationalTableRegistrationRestControllerWithAppendToExistingBusinessObjectDefinitionRequestParameterSetToTrue()
    {
        RelationalTableRegistrationCreateRequest createRequest = new RelationalTableRegistrationCreateRequest();
        createRequest.setNamespace(BDEF_NAMESPACE);
        createRequest.setDataProviderName(DATA_PROVIDER_NAME);
        createRequest.setBusinessObjectDefinitionName(BDEF_NAME);
        createRequest.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        createRequest.setRelationalTableName(RELATIONAL_TABLE_NAME);
        createRequest.setStorageName(STORAGE_NAME);

        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(businessObjectData.getId());
        businessObjectData.setNamespace(BDEF_NAMESPACE);
        businessObjectData.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectData.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectData.setVersion(0);
        businessObjectData.setStatus("VALID");
        businessObjectData.setLatestVersion(true);
        businessObjectData.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        businessObjectData.setPartitionValue(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE);
        businessObjectData.setPartitionKey(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY);


        when(relationalTableRegistrationService.createRelationalTableRegistration(createRequest, APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_TRUE))
            .thenReturn(businessObjectData);

        BusinessObjectData returnedBusinessObjectData =
            relationalTableRegistrationRestController.createRelationalTableRegistration(createRequest, APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_TRUE);

        // Verify the external calls.
        verify(relationalTableRegistrationService).createRelationalTableRegistration(createRequest, APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_TRUE);
        verifyNoMoreInteractions(relationalTableRegistrationService);

        // Validate the returned object.
        assertEquals(businessObjectData, returnedBusinessObjectData);
    }
}
