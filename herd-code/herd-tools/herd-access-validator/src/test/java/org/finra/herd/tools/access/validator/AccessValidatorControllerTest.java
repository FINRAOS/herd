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
package org.finra.herd.tools.access.validator;

import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_DATA_VERSION_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_BASE_URL_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_PASSWORD_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_USERNAME_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.NAMESPACE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.PRIMARY_PARTITION_VALUE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.SUB_PARTITION_VALUES_PROPERTY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.sdk.api.ApplicationApi;
import org.finra.herd.sdk.api.BusinessObjectDataApi;
import org.finra.herd.sdk.api.CurrentUserApi;

public class AccessValidatorControllerTest extends AbstractAccessValidatorTest
{
    @InjectMocks
    private AccessValidatorController accessValidatorController;

    @Mock
    private HerdApiClientOperations herdApiClientOperations;

    @Mock
    private PropertiesHelper propertiesHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testValidateAccess() throws Exception
    {
        // Create a test properties file path.
        final File propertiesFile = new File(PROPERTIES_FILE_PATH);

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_VERSION.toString());
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_DATA_VERSION.toString());
        when(propertiesHelper.getProperty(NAMESPACE_PROPERTY)).thenReturn(NAMESPACE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY)).thenReturn(BUSINESS_OBJECT_DEFINITION_NAME);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_USAGE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        when(propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY)).thenReturn(PRIMARY_PARTITION_VALUE);
        when(propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY)).thenReturn(SUB_PARTITION_VALUES);

        // Call the method under test.
        accessValidatorController.validateAccess(propertiesFile);

        // Verify the external calls.
        verify(propertiesHelper).loadProperties(propertiesFile);
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(NAMESPACE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY);
        verify(propertiesHelper).getProperty(PRIMARY_PARTITION_VALUE_PROPERTY);
        verify(propertiesHelper).getProperty(SUB_PARTITION_VALUES_PROPERTY);
        verify(herdApiClientOperations)
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAccessMissingOptionalProperties() throws Exception
    {
        // Create a test properties file path.
        final File propertiesFile = new File(PROPERTIES_FILE_PATH);

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY)).thenReturn(null);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY)).thenReturn(null);
        when(propertiesHelper.getProperty(NAMESPACE_PROPERTY)).thenReturn(NAMESPACE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY)).thenReturn(BUSINESS_OBJECT_DEFINITION_NAME);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_USAGE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        when(propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY)).thenReturn(PRIMARY_PARTITION_VALUE);
        when(propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY)).thenReturn(SUB_PARTITION_VALUES);

        // Call the method under test.
        accessValidatorController.validateAccess(propertiesFile);

        // Verify the external calls.
        verify(propertiesHelper).loadProperties(propertiesFile);
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(NAMESPACE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY);
        verify(propertiesHelper).getProperty(PRIMARY_PARTITION_VALUE_PROPERTY);
        verify(propertiesHelper).getProperty(SUB_PARTITION_VALUES_PROPERTY);
        verify(herdApiClientOperations)
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(null), eq(null), eq(null), eq(false), eq(false));
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(herdApiClientOperations, propertiesHelper);
    }
}
