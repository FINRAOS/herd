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

import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAME;
import static org.finra.herd.dao.AbstractDaoTest.EXTERNAL_INTERFACE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.NAMESPACE;
import static org.finra.herd.service.AbstractServiceTest.ID;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterface;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceKey;
import org.finra.herd.service.BusinessObjectFormatExternalInterfaceService;

public class BusinessObjectFormatExternalInterfaceRestControllerTest
{
    @InjectMocks
    private BusinessObjectFormatExternalInterfaceRestController businessObjectFormatExternalInterfaceRestController;

    @Mock
    private BusinessObjectFormatExternalInterfaceService businessObjectFormatExternalInterfaceService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectFormatExternalInterface()
    {
        // Create a business object format external interface create request.
        BusinessObjectFormatExternalInterfaceCreateRequest businessObjectFormatExternalInterfaceCreateRequest =
            new BusinessObjectFormatExternalInterfaceCreateRequest(
                new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE));

        // Create a business object format external interface.
        BusinessObjectFormatExternalInterface businessObjectFormatExternalInterface = new BusinessObjectFormatExternalInterface(ID,
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE));

        // Mock the external calls.
        when(businessObjectFormatExternalInterfaceService.createBusinessObjectFormatExternalInterface(businessObjectFormatExternalInterfaceCreateRequest))
            .thenReturn(businessObjectFormatExternalInterface);

        // Call the method under test.
        BusinessObjectFormatExternalInterface result =
            businessObjectFormatExternalInterfaceRestController.createBusinessObjectFormatExternalInterface(businessObjectFormatExternalInterfaceCreateRequest);

        // Validate the result.
        assertEquals(businessObjectFormatExternalInterface, result);

        // Verify the external calls.
        verify(businessObjectFormatExternalInterfaceService).createBusinessObjectFormatExternalInterface(businessObjectFormatExternalInterfaceCreateRequest);
        verifyNoMoreInteractions(businessObjectFormatExternalInterfaceService);
    }

    @Test
    public void testDeleteBusinessObjectFormatExternalInterface()
    {
        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Create a business object format external interface.
        BusinessObjectFormatExternalInterface businessObjectFormatExternalInterface =
            new BusinessObjectFormatExternalInterface(ID, businessObjectFormatExternalInterfaceKey);

        // Mock the external calls.
        when(businessObjectFormatExternalInterfaceService.deleteBusinessObjectFormatExternalInterface(businessObjectFormatExternalInterfaceKey))
            .thenReturn(businessObjectFormatExternalInterface);

        // Call the method under test.
        BusinessObjectFormatExternalInterface result = businessObjectFormatExternalInterfaceRestController
            .deleteBusinessObjectFormatExternalInterface(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Validate the result.
        assertEquals(businessObjectFormatExternalInterface, result);

        // Verify the external calls.
        verify(businessObjectFormatExternalInterfaceService).deleteBusinessObjectFormatExternalInterface(businessObjectFormatExternalInterfaceKey);
        verifyNoMoreInteractions(businessObjectFormatExternalInterfaceService);
    }

    @Test
    public void testGetBusinessObjectFormatExternalInterface()
    {
        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Create a business object format external interface.
        BusinessObjectFormatExternalInterface businessObjectFormatExternalInterface =
            new BusinessObjectFormatExternalInterface(ID, businessObjectFormatExternalInterfaceKey);

        // Mock the external calls.
        when(businessObjectFormatExternalInterfaceService.getBusinessObjectFormatExternalInterface(businessObjectFormatExternalInterfaceKey))
            .thenReturn(businessObjectFormatExternalInterface);

        // Call the method under test.
        BusinessObjectFormatExternalInterface result = businessObjectFormatExternalInterfaceRestController
            .getBusinessObjectFormatExternalInterface(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Validate the result.
        assertEquals(businessObjectFormatExternalInterface, result);

        // Verify the external calls.
        verify(businessObjectFormatExternalInterfaceService).getBusinessObjectFormatExternalInterface(businessObjectFormatExternalInterfaceKey);
        verifyNoMoreInteractions(businessObjectFormatExternalInterfaceService);
    }
}
