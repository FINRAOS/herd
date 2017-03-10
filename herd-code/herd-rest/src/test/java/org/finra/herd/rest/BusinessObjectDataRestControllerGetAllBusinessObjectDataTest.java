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

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.service.BusinessObjectDataService;
import org.finra.herd.service.StorageUnitService;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;

/**
 * This class tests get all business object data functionality within the business object data REST controller.
 */
public class BusinessObjectDataRestControllerGetAllBusinessObjectDataTest extends AbstractRestTest
{
    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @InjectMocks
    private BusinessObjectDataRestController businessObjectDataRestController;

    @Mock
    private BusinessObjectDataService businessObjectDataService;

    @Mock
    private StorageUnitService storageUnitService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetAllBusinessObjectDataByBusinessObjectDefinition()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create an expected response object.
        BusinessObjectDataKeys businessObjectDataKeys = new BusinessObjectDataKeys(Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION)));

        // Mock the external calls.
        when(businessObjectDataService.getAllBusinessObjectDataByBusinessObjectDefinition(businessObjectDefinitionKey)).thenReturn(businessObjectDataKeys);

        // Call the method being tested.
        BusinessObjectDataKeys response = businessObjectDataRestController.getAllBusinessObjectDataByBusinessObjectDefinition(BDEF_NAMESPACE, BDEF_NAME);

        // Verify the external calls.
        verify(businessObjectDataService).getAllBusinessObjectDataByBusinessObjectDefinition(businessObjectDefinitionKey);
        verifyNoMoreInteractionsHelper();

        // Validate the returned object.
        assertEquals(businessObjectDataKeys, response);
    }

    @Test
    public void testGetAllBusinessObjectDataByBusinessObjectFormatAutowired()
    {
        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Create an expected response object.
        BusinessObjectDataKeys businessObjectDataKeys = new BusinessObjectDataKeys(Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION)));

        // Mock the external calls.
        when(businessObjectDataService.getAllBusinessObjectDataByBusinessObjectFormat(businessObjectFormatKey)).thenReturn(businessObjectDataKeys);

        // Call the method being tested.
        BusinessObjectDataKeys response = businessObjectDataRestController
            .getAllBusinessObjectDataByBusinessObjectFormat(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Verify the external calls.
        verify(businessObjectDataService).getAllBusinessObjectDataByBusinessObjectFormat(businessObjectFormatKey);
        verifyNoMoreInteractionsHelper();

        // Validate the returned object.
        assertEquals(businessObjectDataKeys, response);
    }

    /**
     * Checks if any of the mocks has any unverified interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataDaoHelper, businessObjectDataService, storageUnitService);
    }
}
