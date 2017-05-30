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

import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.CustomDdl;
import org.finra.herd.model.api.xml.CustomDdlCreateRequest;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.api.xml.CustomDdlKeys;
import org.finra.herd.model.api.xml.CustomDdlUpdateRequest;
import org.finra.herd.service.CustomDdlService;

/**
 * This class tests various functionality within the custom DDL REST controller.
 */
public class CustomDdlRestControllerTest extends AbstractRestTest
{
    @Mock
    private CustomDdlService customDdlService;

    @InjectMocks
    private CustomDdlRestController customDdlRestController;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateCustomDdl()
    {
        CustomDdlCreateRequest request = customDdlServiceTestHelper
            .createCustomDdlCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        CustomDdl customDdl =
            new CustomDdl(100, new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME), TEST_DDL);

        when(customDdlService.createCustomDdl(request)).thenReturn(customDdl);

        // Create a custom DDL.
        CustomDdl resultCustomDdl = customDdlRestController.createCustomDdl(request);

        // Validate the returned object.
        customDdlServiceTestHelper
            .validateCustomDdl(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL,
                resultCustomDdl);

        // Verify the external calls.
        verify(customDdlService).createCustomDdl(request);
        verifyNoMoreInteractions(customDdlService);
        // Validate the returned object.
        assertEquals(customDdl, resultCustomDdl);
    }

    @Test
    public void testGetCustomDdl()
    {
        CustomDdl customDdl =
            new CustomDdl(100, new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME), TEST_DDL);
        CustomDdlKey customDdlKey = new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME);

        when(customDdlService.getCustomDdl(customDdlKey)).thenReturn(customDdl);

        // Retrieve the custom DDL.
        CustomDdl resultCustomDdl =
            customDdlRestController.getCustomDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME);

        // Validate the returned object.
        customDdlServiceTestHelper
            .validateCustomDdl(resultCustomDdl.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME,
                TEST_DDL, resultCustomDdl);

        // Verify the external calls.
        verify(customDdlService).getCustomDdl(customDdlKey);
        verifyNoMoreInteractions(customDdlService);
        // Validate the returned object.
        assertEquals(customDdl, resultCustomDdl);
    }

    @Test
    public void testGetCustomDdls()
    {
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        CustomDdlKeys customDdlKeys =
            new CustomDdlKeys(Arrays.asList(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "ddl")));

        when(customDdlService.getCustomDdls(businessObjectFormatKey)).thenReturn(customDdlKeys);
        // Retrieve a list of custom DDL keys.
        CustomDdlKeys resultCustomDdlKeys =
            customDdlRestController.getCustomDdls(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Verify the external calls.
        verify(customDdlService).getCustomDdls(businessObjectFormatKey);
        verifyNoMoreInteractions(customDdlService);
        // Validate the returned object.
        assertEquals(customDdlKeys, resultCustomDdlKeys);
    }

    @Test
    public void testUpdateCustomDdl()
    {
        CustomDdlUpdateRequest request = customDdlServiceTestHelper.createCustomDdlUpdateRequest(TEST_DDL_2);

        CustomDdlKey customDdlKey = new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME);

        CustomDdl customDdl =
            new CustomDdl(100, new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME), TEST_DDL_2);

        when(customDdlService.updateCustomDdl(customDdlKey, request)).thenReturn(customDdl);
        // Update the custom DDL.
        CustomDdl updatedCustomDdl =
            customDdlRestController.updateCustomDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, request);

        // Verify the external calls.
        verify(customDdlService).updateCustomDdl(customDdlKey, request);
        verifyNoMoreInteractions(customDdlService);
        // Validate the returned object.
        assertEquals(customDdl, updatedCustomDdl);
    }

    @Test
    public void testDeleteCustomDdl()
    {
        CustomDdlKey customDdlKey = new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME);

        CustomDdl customDdl = new CustomDdl(100, customDdlKey, TEST_DDL);

        when(customDdlService.deleteCustomDdl(customDdlKey)).thenReturn(customDdl);
        // Delete this custom DDL.
        CustomDdl deletedCustomDdl =
            customDdlRestController.deleteCustomDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME);

        // Validate the returned object.
        customDdlServiceTestHelper
            .validateCustomDdl(customDdl.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL,
                deletedCustomDdl);

        // Verify the external calls.
        verify(customDdlService).deleteCustomDdl(customDdlKey);
        verifyNoMoreInteractions(customDdlService);
        // Validate the returned object.
        assertEquals(customDdl, deletedCustomDdl);
    }
}
