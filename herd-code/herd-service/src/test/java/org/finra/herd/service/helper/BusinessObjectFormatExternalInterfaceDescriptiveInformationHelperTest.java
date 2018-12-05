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
package org.finra.herd.service.helper;

import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAME;
import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAME_2;
import static org.finra.herd.dao.AbstractDaoTest.EXTERNAL_INTERFACE;
import static org.finra.herd.dao.AbstractDaoTest.EXTERNAL_INTERFACE_2;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE_2;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE_2;
import static org.finra.herd.dao.AbstractDaoTest.NAMESPACE;
import static org.finra.herd.dao.AbstractDaoTest.NAMESPACE_2;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceDescriptiveInformationKey;

public class BusinessObjectFormatExternalInterfaceDescriptiveInformationHelperTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @InjectMocks
    private BusinessObjectFormatExternalInterfaceDescriptiveInformationHelper businessObjectFormatExternalInterfaceDescriptiveInformationHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }


    @Test
    public void testValidateAndTrimBusinessObjectFormatExternalInterfaceDescriptiveInformationKey()
    {
        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceDescriptiveInformationKey businessObjectFormatExternalInterfaceDescriptiveInformationKey =
            new BusinessObjectFormatExternalInterfaceDescriptiveInformationKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                EXTERNAL_INTERFACE);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("namespace", NAMESPACE)).thenReturn(NAMESPACE_2);
        when(alternateKeyHelper.validateStringParameter("business object definition name", BDEF_NAME)).thenReturn(BDEF_NAME_2);
        when(alternateKeyHelper.validateStringParameter("business object format usage", FORMAT_USAGE_CODE)).thenReturn(FORMAT_USAGE_CODE_2);
        when(alternateKeyHelper.validateStringParameter("business object format file type", FORMAT_FILE_TYPE_CODE)).thenReturn(FORMAT_FILE_TYPE_CODE_2);
        when(alternateKeyHelper.validateStringParameter("An", "external interface name", EXTERNAL_INTERFACE)).thenReturn(EXTERNAL_INTERFACE_2);

        // Call the method under test.
        businessObjectFormatExternalInterfaceDescriptiveInformationHelper
            .validateAndTrimBusinessObjectFormatExternalInterfaceDescriptiveInformationKey(businessObjectFormatExternalInterfaceDescriptiveInformationKey);

        // Validate the results.
        assertEquals(new BusinessObjectFormatExternalInterfaceDescriptiveInformationKey(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2,
            EXTERNAL_INTERFACE_2), businessObjectFormatExternalInterfaceDescriptiveInformationKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("namespace", NAMESPACE);
        verify(alternateKeyHelper).validateStringParameter("business object definition name", BDEF_NAME);
        verify(alternateKeyHelper).validateStringParameter("business object format usage", FORMAT_USAGE_CODE);
        verify(alternateKeyHelper).validateStringParameter("business object format file type", FORMAT_FILE_TYPE_CODE);
        verify(alternateKeyHelper).validateStringParameter("An", "external interface name", EXTERNAL_INTERFACE);
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimBusinessObjectFormatExternalInterfaceKeyBusinessObjectFormatExternalInterfaceDescriptiveInformationKeyIsNull()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A business object format external interface descriptive information key must be specified.");

        // Call the method under test.
        businessObjectFormatExternalInterfaceDescriptiveInformationHelper.validateAndTrimBusinessObjectFormatExternalInterfaceDescriptiveInformationKey(null);

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }
}
