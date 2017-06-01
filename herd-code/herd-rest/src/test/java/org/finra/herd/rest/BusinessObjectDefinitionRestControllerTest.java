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
import java.util.Collections;
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
    public void testCreateBusinessObjectDefinition() throws Exception
    {
        // Create a business object definition.
        BusinessObjectDefinitionCreateRequest request =
            new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());

        BusinessObjectDefinition businessObjectDefinition = getBusinessObjectDefinition();

        when(businessObjectDefinitionService.createBusinessObjectDefinition(request)).thenReturn(businessObjectDefinition);

        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionRestController.createBusinessObjectDefinition(request);

        // Verify the external calls.
        verify(businessObjectDefinitionService).createBusinessObjectDefinition(request);
        verifyNoMoreInteractions(businessObjectDefinitionService);
        // Validate the returned object.
        assertEquals(businessObjectDefinition, resultBusinessObjectDefinition);
    }

    @Test
    public void testDeleteBusinessObjectDefinition() throws Exception
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        BusinessObjectDefinition businessObjectDefinition = getBusinessObjectDefinition();
        when(businessObjectDefinitionService.deleteBusinessObjectDefinition(businessObjectDefinitionKey)).thenReturn(businessObjectDefinition);

        // Delete this business object definition.
        BusinessObjectDefinition deletedBusinessObjectDefinition = businessObjectDefinitionRestController.deleteBusinessObjectDefinition(NAMESPACE, BDEF_NAME);

        // Verify the external calls.
        verify(businessObjectDefinitionService).deleteBusinessObjectDefinition(businessObjectDefinitionKey);
        verifyNoMoreInteractions(businessObjectDefinitionService);
        // Validate the returned object.
        assertEquals(businessObjectDefinition, deletedBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinition() throws Exception
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        BusinessObjectDefinition businessObjectDefinition = getBusinessObjectDefinition();
        when(businessObjectDefinitionService.getBusinessObjectDefinition(businessObjectDefinitionKey)).thenReturn(businessObjectDefinition);

        // Retrieve the business object definition.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionRestController.getBusinessObjectDefinition(NAMESPACE, BDEF_NAME);

        // Verify the external calls.
        verify(businessObjectDefinitionService).getBusinessObjectDefinition(businessObjectDefinitionKey);
        verifyNoMoreInteractions(businessObjectDefinitionService);
        // Validate the returned object.
        assertEquals(businessObjectDefinition, resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitions() throws Exception
    {
        BusinessObjectDefinitionKeys businessObjectDefinitionKeys =
            new BusinessObjectDefinitionKeys(businessObjectDefinitionDaoTestHelper.getTestBusinessObjectDefinitionKeys());

        when(businessObjectDefinitionService.getBusinessObjectDefinitions(NAMESPACE)).thenReturn(businessObjectDefinitionKeys);

        // Retrieve a list of business object definition keys for the specified namespace.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionRestController.getBusinessObjectDefinitions(NAMESPACE);

        // Verify the external calls.
        verify(businessObjectDefinitionService).getBusinessObjectDefinitions(NAMESPACE);
        verifyNoMoreInteractions(businessObjectDefinitionService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionKeys, resultKeys);
    }

    @Test
    public void testSearchBusinessObjectDefinition()
    {
        BusinessObjectDefinitionSearchResponse businessObjectDefinitionSearchResponse = new BusinessObjectDefinitionSearchResponse();
        businessObjectDefinitionSearchResponse.setBusinessObjectDefinitions(Arrays.asList(getBusinessObjectDefinition()));
        Set<String> set = Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);
        BusinessObjectDefinitionSearchRequest request = new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter(false,
            Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY)))));

        when(businessObjectDefinitionService.searchBusinessObjectDefinitions(request, set)).thenReturn(businessObjectDefinitionSearchResponse);

        // Tests with tag filter.
        BusinessObjectDefinitionSearchResponse resultSearchResponse =
            businessObjectDefinitionSearchResponse = businessObjectDefinitionRestController.searchBusinessObjectDefinitions(set, request);
        // Verify the external calls.
        verify(businessObjectDefinitionService).searchBusinessObjectDefinitions(request, set);
        verifyNoMoreInteractions(businessObjectDefinitionService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionSearchResponse, resultSearchResponse);
    }

    @Test
    public void testUpdateBusinessObjectDefinition() throws Exception
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        BusinessObjectDefinition businessObjectDefinition = getBusinessObjectDefinition();
        BusinessObjectDefinitionUpdateRequest businessObjectDefinitionUpdateRequest =
            new BusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2, businessObjectDefinitionServiceTestHelper.getNewAttributes2());

        when(businessObjectDefinitionService.updateBusinessObjectDefinition(businessObjectDefinitionKey, businessObjectDefinitionUpdateRequest))
            .thenReturn(businessObjectDefinition);

        // Perform an update by changing the description and updating the attributes.
        BusinessObjectDefinition updatedBusinessObjectDefinition =
            businessObjectDefinitionRestController.updateBusinessObjectDefinition(NAMESPACE, BDEF_NAME, businessObjectDefinitionUpdateRequest);

        // Verify the external calls.
        verify(businessObjectDefinitionService).updateBusinessObjectDefinition(businessObjectDefinitionKey, businessObjectDefinitionUpdateRequest);
        verifyNoMoreInteractions(businessObjectDefinitionService);
        // Validate the returned object.
        assertEquals(businessObjectDefinition, updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptiveInformation() throws Exception
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        BusinessObjectDefinition businessObjectDefinition = getBusinessObjectDefinition();
        BusinessObjectDefinitionDescriptiveInformationUpdateRequest request =
            new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_UPDATE_REQUEST);

        when(businessObjectDefinitionService.updateBusinessObjectDefinitionDescriptiveInformation(businessObjectDefinitionKey, request))
            .thenReturn(businessObjectDefinition);
        // Perform an update by changing the description and updating the attributes.
        BusinessObjectDefinition updatedBusinessObjectDefinition =
            businessObjectDefinitionRestController.updateBusinessObjectDefinitionDescriptiveInformation(NAMESPACE, BDEF_NAME, request);

        // Verify the external calls.
        verify(businessObjectDefinitionService).updateBusinessObjectDefinitionDescriptiveInformation(businessObjectDefinitionKey, request);
        verifyNoMoreInteractions(businessObjectDefinitionService);
        // Validate the returned object.
        assertEquals(businessObjectDefinition, updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionMissingOptionalParametersPassedAsWhitespace()
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        BusinessObjectDefinition businessObjectDefinition = getBusinessObjectDefinition();
        BusinessObjectDefinitionUpdateRequest request = new BusinessObjectDefinitionUpdateRequest(BLANK_TEXT, BLANK_TEXT,
            Collections.singletonList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT)));

        when(businessObjectDefinitionService.updateBusinessObjectDefinition(businessObjectDefinitionKey, request)).thenReturn(businessObjectDefinition);

        // Perform an update without specifying any of the optional parameters (passing whitespace characters).
        BusinessObjectDefinition updatedBusinessObjectDefinition =
            businessObjectDefinitionRestController.updateBusinessObjectDefinition(NAMESPACE, BDEF_NAME, request);
        // Verify the external calls.
        verify(businessObjectDefinitionService).updateBusinessObjectDefinition(businessObjectDefinitionKey, request);
        verifyNoMoreInteractions(businessObjectDefinitionService);
        // Validate the returned object.
        assertEquals(businessObjectDefinition, updatedBusinessObjectDefinition);
    }

    private BusinessObjectDefinition getBusinessObjectDefinition()
    {
        BusinessObjectDefinition businessObjectDefinition = new BusinessObjectDefinition();
        businessObjectDefinition.setNamespace(NAMESPACE);
        businessObjectDefinition.setBusinessObjectDefinitionName(BDEF_NAME);

        return businessObjectDefinition;
    }
}
