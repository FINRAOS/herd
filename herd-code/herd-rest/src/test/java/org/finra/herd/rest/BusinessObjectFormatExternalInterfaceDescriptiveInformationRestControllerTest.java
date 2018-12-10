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
import static org.finra.herd.dao.AbstractDaoTest.DESCRIPTION;
import static org.finra.herd.dao.AbstractDaoTest.DISPLAY_NAME_FIELD;
import static org.finra.herd.dao.AbstractDaoTest.EXTERNAL_INTERFACE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.NAMESPACE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceDescriptiveInformation;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceKey;
import org.finra.herd.service.BusinessObjectFormatExternalInterfaceDescriptiveInformationService;

public class BusinessObjectFormatExternalInterfaceDescriptiveInformationRestControllerTest
{
    @InjectMocks
    private BusinessObjectFormatExternalInterfaceDescriptiveInformationRestController businessObjectFormatExternalInterfaceDescriptiveInformationRestController;

    @Mock
    private BusinessObjectFormatExternalInterfaceDescriptiveInformationService businessObjectFormatExternalInterfaceDescriptiveInformationService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetBusinessObjectFormatExternalInterfaceDescriptiveInformation()
    {
        // Create a business object format external interface key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Create business object format external interface descriptive information.
        BusinessObjectFormatExternalInterfaceDescriptiveInformation businessObjectFormatExternalInterfaceDescriptiveInformation =
            new BusinessObjectFormatExternalInterfaceDescriptiveInformation(businessObjectFormatExternalInterfaceKey, DISPLAY_NAME_FIELD, DESCRIPTION);

        // Mock the external calls.
        when(businessObjectFormatExternalInterfaceDescriptiveInformationService
            .getBusinessObjectFormatExternalInterfaceDescriptiveInformation(businessObjectFormatExternalInterfaceKey))
            .thenReturn(businessObjectFormatExternalInterfaceDescriptiveInformation);

        // Call the method under test.
        BusinessObjectFormatExternalInterfaceDescriptiveInformation result = businessObjectFormatExternalInterfaceDescriptiveInformationRestController
            .getBusinessObjectFormatExternalInterface(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Validate the result.
        assertEquals(businessObjectFormatExternalInterfaceDescriptiveInformation, result);

        // Verify the external calls.
        verify(businessObjectFormatExternalInterfaceDescriptiveInformationService)
            .getBusinessObjectFormatExternalInterfaceDescriptiveInformation(businessObjectFormatExternalInterfaceKey);
        verifyNoMoreInteractions(businessObjectFormatExternalInterfaceDescriptiveInformationService);
    }
}
