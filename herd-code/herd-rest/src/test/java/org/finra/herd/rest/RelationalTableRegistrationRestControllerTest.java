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

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
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
