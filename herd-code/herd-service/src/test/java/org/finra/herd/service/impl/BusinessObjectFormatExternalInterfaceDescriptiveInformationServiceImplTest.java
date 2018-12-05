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
package org.finra.herd.service.impl;

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
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceDescriptiveInformationKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatExternalInterfaceDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatExternalInterfaceDescriptiveInformationHelper;
import org.finra.herd.service.helper.ExternalInterfaceDaoHelper;

public class BusinessObjectFormatExternalInterfaceDescriptiveInformationServiceImplTest
{
    @Mock
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Mock
    private BusinessObjectFormatExternalInterfaceDaoHelper businessObjectFormatExternalInterfaceDaoHelper;

    @Mock
    private BusinessObjectFormatExternalInterfaceDescriptiveInformationHelper businessObjectFormatExternalInterfaceDescriptiveInformationHelper;

    @InjectMocks
    private BusinessObjectFormatExternalInterfaceDescriptiveInformationServiceImpl businessObjectFormatExternalInterfaceDescriptiveInformationService;

    @Mock
    private ExternalInterfaceDaoHelper externalInterfaceDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetBusinessObjectFormatExternalInterfaceDescriptiveInformation()
    {
        // Create a business object format external interface descriptive information key.
        BusinessObjectFormatExternalInterfaceDescriptiveInformationKey businessObjectFormatExternalInterfaceDescriptiveInformationKey =
            new BusinessObjectFormatExternalInterfaceDescriptiveInformationKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                EXTERNAL_INTERFACE);

        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Create a business object format key
        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);

        // Create an external interface entity.
        ExternalInterfaceEntity externalInterfaceEntity = new ExternalInterfaceEntity();

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a business object external interface descriptive information object
        BusinessObjectFormatExternalInterfaceDescriptiveInformation businessObjectFormatExternalInterfaceDescriptiveInformation =
            new BusinessObjectFormatExternalInterfaceDescriptiveInformation(businessObjectFormatExternalInterfaceDescriptiveInformationKey, DISPLAY_NAME_FIELD,
                DESCRIPTION);

        // Mock the external calls.
        when(externalInterfaceDaoHelper.getExternalInterfaceEntity(EXTERNAL_INTERFACE)).thenReturn(externalInterfaceEntity);
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectFormatExternalInterfaceDescriptiveInformationHelper
            .createBusinessObjectFormatExternalInterfaceDescriptiveInformationFromEntities(businessObjectFormatEntity, externalInterfaceEntity))
            .thenReturn(businessObjectFormatExternalInterfaceDescriptiveInformation);

        // Call the method under test.
        BusinessObjectFormatExternalInterfaceDescriptiveInformation result = businessObjectFormatExternalInterfaceDescriptiveInformationService
            .getBusinessObjectFormatExternalInterfaceDescriptiveInformation(businessObjectFormatExternalInterfaceDescriptiveInformationKey);

        // Validate the results.
        assertEquals(businessObjectFormatExternalInterfaceDescriptiveInformation, result);

        // Verify the external calls.
        verify(businessObjectFormatExternalInterfaceDescriptiveInformationHelper)
            .validateAndTrimBusinessObjectFormatExternalInterfaceDescriptiveInformationKey(businessObjectFormatExternalInterfaceDescriptiveInformationKey);
        verify(businessObjectFormatExternalInterfaceDaoHelper).getBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatExternalInterfaceKey);
        verify(externalInterfaceDaoHelper).getExternalInterfaceEntity(EXTERNAL_INTERFACE);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectFormatExternalInterfaceDescriptiveInformationHelper)
            .createBusinessObjectFormatExternalInterfaceDescriptiveInformationFromEntities(businessObjectFormatEntity, externalInterfaceEntity);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectFormatDaoHelper, businessObjectFormatExternalInterfaceDaoHelper,
            businessObjectFormatExternalInterfaceDescriptiveInformationHelper, externalInterfaceDaoHelper);
    }
}
