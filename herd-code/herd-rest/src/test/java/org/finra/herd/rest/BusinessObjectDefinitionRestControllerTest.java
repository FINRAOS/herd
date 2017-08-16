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
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptiveInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionUpdateRequest;
import org.finra.herd.model.api.xml.DescriptiveBusinessObjectFormat;
import org.finra.herd.model.api.xml.DescriptiveBusinessObjectFormatUpdateRequest;
import org.finra.herd.model.api.xml.SampleDataFile;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.service.BusinessObjectDefinitionService;

/**
 * This class tests various functionality within the business object definition REST controller.
 */
public class BusinessObjectDefinitionRestControllerTest extends AbstractRestTest
{
    // Constant to hold the data provider name option for the business object definition search
    public static final String FIELD_DATA_PROVIDER_NAME = "dataProviderName";

    // Constant to hold the display name option for the business object definition search
    public static final String FIELD_DISPLAY_NAME = "displayName";

    // Constant to hold the short description option for the business object definition search
    public static final String FIELD_SHORT_DESCRIPTION = "shortDescription";

    @InjectMocks
    private BusinessObjectDefinitionRestController businessObjectDefinitionRestController;

    @Mock
    private BusinessObjectDefinitionService businessObjectDefinitionService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectDefinition()
    {
        // Create a business object definition.
        BusinessObjectDefinitionCreateRequest request =
            new BusinessObjectDefinitionCreateRequest(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1)));

        // Create a business object definition.
        BusinessObjectDefinition businessObjectDefinition =
            new BusinessObjectDefinition(ID, BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, SHORT_DESCRIPTION, BDEF_DISPLAY_NAME,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1)),
                new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                Arrays.asList(new SampleDataFile(DIRECTORY_PATH, FILE_NAME)), CREATED_BY, UPDATED_BY, UPDATED_ON, NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS);

        // Mock the external calls.
        when(businessObjectDefinitionService.createBusinessObjectDefinition(request)).thenReturn(businessObjectDefinition);

        // Call the method under test.
        BusinessObjectDefinition result = businessObjectDefinitionRestController.createBusinessObjectDefinition(request);

        // Verify the external calls.
        verify(businessObjectDefinitionService).createBusinessObjectDefinition(request);
        verifyNoMoreInteractions(businessObjectDefinitionService);

        // Validate the returned object.
        assertEquals(businessObjectDefinition, result);
    }

    @Test
    public void testDeleteBusinessObjectDefinition()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create a business object definition.
        BusinessObjectDefinition businessObjectDefinition =
            new BusinessObjectDefinition(ID, BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, SHORT_DESCRIPTION, BDEF_DISPLAY_NAME,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1)),
                new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                Arrays.asList(new SampleDataFile(DIRECTORY_PATH, FILE_NAME)), CREATED_BY, UPDATED_BY, UPDATED_ON, NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS);

        // Mock the external calls.
        when(businessObjectDefinitionService.deleteBusinessObjectDefinition(businessObjectDefinitionKey)).thenReturn(businessObjectDefinition);

        // Call the method under test.
        BusinessObjectDefinition result = businessObjectDefinitionRestController.deleteBusinessObjectDefinition(BDEF_NAMESPACE, BDEF_NAME);

        // Verify the external calls.
        verify(businessObjectDefinitionService).deleteBusinessObjectDefinition(businessObjectDefinitionKey);
        verifyNoMoreInteractions(businessObjectDefinitionService);

        // Validate the returned object.
        assertEquals(businessObjectDefinition, result);
    }

    @Test
    public void testGetBusinessObjectDefinition()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create a business object definition.
        BusinessObjectDefinition businessObjectDefinition =
            new BusinessObjectDefinition(ID, BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, SHORT_DESCRIPTION, BDEF_DISPLAY_NAME,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1)),
                new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                Arrays.asList(new SampleDataFile(DIRECTORY_PATH, FILE_NAME)), CREATED_BY, UPDATED_BY, UPDATED_ON, NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS);

        // Mock the external calls.
        when(businessObjectDefinitionService.getBusinessObjectDefinition(businessObjectDefinitionKey, NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY))
            .thenReturn(businessObjectDefinition);

        // Call the method under test.
        BusinessObjectDefinition result = businessObjectDefinitionRestController
            .getBusinessObjectDefinition(BDEF_NAMESPACE, BDEF_NAME, NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);

        // Verify the external calls.
        verify(businessObjectDefinitionService).getBusinessObjectDefinition(businessObjectDefinitionKey, NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);
        verifyNoMoreInteractions(businessObjectDefinitionService);

        // Validate the results.
        assertEquals(businessObjectDefinition, result);
    }

    @Test
    public void testGetBusinessObjectDefinitions()
    {
        // Create business object definition keys.
        BusinessObjectDefinitionKeys businessObjectDefinitionKeys =
            new BusinessObjectDefinitionKeys(Arrays.asList(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME)));

        // Mock the external calls.
        when(businessObjectDefinitionService.getBusinessObjectDefinitions()).thenReturn(businessObjectDefinitionKeys);

        // Call the method under test.
        BusinessObjectDefinitionKeys result = businessObjectDefinitionRestController.getBusinessObjectDefinitions();

        // Verify the external calls.
        verify(businessObjectDefinitionService).getBusinessObjectDefinitions();
        verifyNoMoreInteractions(businessObjectDefinitionService);

        // Validate the results.
        assertEquals(businessObjectDefinitionKeys, result);
    }

    @Test
    public void testGetBusinessObjectDefinitionsByNamespace()
    {
        // Create business object definition keys.
        BusinessObjectDefinitionKeys businessObjectDefinitionKeys =
            new BusinessObjectDefinitionKeys(Arrays.asList(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME)));

        // Mock the external calls.
        when(businessObjectDefinitionService.getBusinessObjectDefinitions(BDEF_NAMESPACE)).thenReturn(businessObjectDefinitionKeys);

        // Call the method under test.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionRestController.getBusinessObjectDefinitions(BDEF_NAMESPACE);

        // Verify the external calls.
        verify(businessObjectDefinitionService).getBusinessObjectDefinitions(BDEF_NAMESPACE);
        verifyNoMoreInteractions(businessObjectDefinitionService);

        // Validate the results.
        assertEquals(businessObjectDefinitionKeys, resultKeys);
    }

    @Test
    public void testSearchBusinessObjectDefinition()
    {
        // Create a business object definition search request.
        BusinessObjectDefinitionSearchRequest request = new BusinessObjectDefinitionSearchRequest(Arrays.asList(
            new BusinessObjectDefinitionSearchFilter(EXCLUSION_SEARCH_FILTER,
                Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY)))));

        // Create a business object definition.
        BusinessObjectDefinition businessObjectDefinition =
            new BusinessObjectDefinition(ID, BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, SHORT_DESCRIPTION, BDEF_DISPLAY_NAME,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1)),
                new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                Arrays.asList(new SampleDataFile(DIRECTORY_PATH, FILE_NAME)), CREATED_BY, UPDATED_BY, UPDATED_ON, NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS);

        // Create a business object definition search response.
        BusinessObjectDefinitionSearchResponse businessObjectDefinitionSearchResponse =
            new BusinessObjectDefinitionSearchResponse(Arrays.asList(businessObjectDefinition));

        // Create a set of search optional fields.
        Set<String> fields = Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Mock the external calls.
        when(businessObjectDefinitionService.searchBusinessObjectDefinitions(request, fields)).thenReturn(businessObjectDefinitionSearchResponse);

        // Call the method under test.
        BusinessObjectDefinitionSearchResponse result =
            businessObjectDefinitionSearchResponse = businessObjectDefinitionRestController.searchBusinessObjectDefinitions(fields, request);

        // Verify the external calls.
        verify(businessObjectDefinitionService).searchBusinessObjectDefinitions(request, fields);
        verifyNoMoreInteractions(businessObjectDefinitionService);

        // Validate the results.
        assertEquals(businessObjectDefinitionSearchResponse, result);
    }

    @Test
    public void testUpdateBusinessObjectDefinition()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create a business object definition update request.
        BusinessObjectDefinitionUpdateRequest businessObjectDefinitionUpdateRequest =
            new BusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2)));

        // Create a business object definition.
        BusinessObjectDefinition businessObjectDefinition =
            new BusinessObjectDefinition(ID, BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, SHORT_DESCRIPTION, BDEF_DISPLAY_NAME,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1)),
                new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                Arrays.asList(new SampleDataFile(DIRECTORY_PATH, FILE_NAME)), CREATED_BY, UPDATED_BY, UPDATED_ON, NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS);

        // Mock the external calls.
        when(businessObjectDefinitionService.updateBusinessObjectDefinition(businessObjectDefinitionKey, businessObjectDefinitionUpdateRequest))
            .thenReturn(businessObjectDefinition);

        // Call the method under test.
        BusinessObjectDefinition result =
            businessObjectDefinitionRestController.updateBusinessObjectDefinition(BDEF_NAMESPACE, BDEF_NAME, businessObjectDefinitionUpdateRequest);

        // Verify the external calls.
        verify(businessObjectDefinitionService).updateBusinessObjectDefinition(businessObjectDefinitionKey, businessObjectDefinitionUpdateRequest);
        verifyNoMoreInteractions(businessObjectDefinitionService);

        // Validate the results.
        assertEquals(businessObjectDefinition, result);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptiveInformation()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create a business object definition descriptive information update request.
        BusinessObjectDefinitionDescriptiveInformationUpdateRequest request =
            new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                new DescriptiveBusinessObjectFormatUpdateRequest(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE));

        // Create a business object definition.
        BusinessObjectDefinition businessObjectDefinition =
            new BusinessObjectDefinition(ID, BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, SHORT_DESCRIPTION, BDEF_DISPLAY_NAME,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1)),
                new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                Arrays.asList(new SampleDataFile(DIRECTORY_PATH, FILE_NAME)), CREATED_BY, UPDATED_BY, UPDATED_ON, NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS);

        // Mock the external calls.
        when(businessObjectDefinitionService.updateBusinessObjectDefinitionDescriptiveInformation(businessObjectDefinitionKey, request))
            .thenReturn(businessObjectDefinition);

        // Call the method under test.
        BusinessObjectDefinition result =
            businessObjectDefinitionRestController.updateBusinessObjectDefinitionDescriptiveInformation(BDEF_NAMESPACE, BDEF_NAME, request);

        // Verify the external calls.
        verify(businessObjectDefinitionService).updateBusinessObjectDefinitionDescriptiveInformation(businessObjectDefinitionKey, request);
        verifyNoMoreInteractions(businessObjectDefinitionService);

        // Validate the results.
        assertEquals(businessObjectDefinition, result);
    }
}
