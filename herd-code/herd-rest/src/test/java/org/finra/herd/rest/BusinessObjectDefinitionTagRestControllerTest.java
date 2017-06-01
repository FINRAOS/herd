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
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTag;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKeys;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.service.BusinessObjectDefinitionTagService;

/**
 * This class tests various functionality within the business object definition column REST controller.
 */
public class BusinessObjectDefinitionTagRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private BusinessObjectDefinitionTagRestController businessObjectDefinitionTagRestController;

    @Mock
    private BusinessObjectDefinitionTagService businessObjectDefinitionTagService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectDefinitionTag()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create a business object definition tag key.
        BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey =
            new BusinessObjectDefinitionTagKey(businessObjectDefinitionKey, new TagKey(TAG_TYPE, TAG_CODE));
        BusinessObjectDefinitionTag businessObjectDefinitionTag = new BusinessObjectDefinitionTag(ID, businessObjectDefinitionTagKey);

        BusinessObjectDefinitionTagCreateRequest request = new BusinessObjectDefinitionTagCreateRequest(businessObjectDefinitionTagKey);

        when(businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(request)).thenReturn(businessObjectDefinitionTag);

        // Create a business object definition tag.
        BusinessObjectDefinitionTag result = businessObjectDefinitionTagRestController.createBusinessObjectDefinitionTag(request);

        // Verify the external calls.
        verify(businessObjectDefinitionTagService).createBusinessObjectDefinitionTag(request);
        verifyNoMoreInteractions(businessObjectDefinitionTagService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionTag, result);
    }

    @Test
    public void testDeleteBusinessObjectDefinitionTag()
    {
        // Create a business object definition tag key.
        BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey =
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        BusinessObjectDefinitionTag businessObjectDefinitionTag = new BusinessObjectDefinitionTag(ID, businessObjectDefinitionTagKey);

        when(businessObjectDefinitionTagService.deleteBusinessObjectDefinitionTag(businessObjectDefinitionTagKey)).thenReturn(businessObjectDefinitionTag);

        // Delete this business object definition tag.
        BusinessObjectDefinitionTag result =
            businessObjectDefinitionTagRestController.deleteBusinessObjectDefinitionTag(BDEF_NAMESPACE, BDEF_NAME, TAG_TYPE, TAG_CODE);

        // Verify the external calls.
        verify(businessObjectDefinitionTagService).deleteBusinessObjectDefinitionTag(businessObjectDefinitionTagKey);
        verifyNoMoreInteractions(businessObjectDefinitionTagService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionTag, result);
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByBusinessObjectDefinition()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);
        // Create tag keys.
        List<TagKey> tagKeys = Arrays.asList(new TagKey(TAG_TYPE, TAG_CODE), new TagKey(TAG_TYPE_2, TAG_CODE_2));
        BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey =
            new BusinessObjectDefinitionTagKey(businessObjectDefinitionKey, new TagKey(TAG_TYPE, TAG_CODE));
        BusinessObjectDefinitionTagKeys BusinessObjectDefinitionTagKeys = new BusinessObjectDefinitionTagKeys(Arrays.asList(businessObjectDefinitionTagKey));

        when(businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByBusinessObjectDefinition(businessObjectDefinitionKey))
            .thenReturn(BusinessObjectDefinitionTagKeys);

        // Get business object definition tags by business object definition.
        BusinessObjectDefinitionTagKeys result =
            businessObjectDefinitionTagRestController.getBusinessObjectDefinitionTagsByBusinessObjectDefinition(BDEF_NAMESPACE, BDEF_NAME);

        // Verify the external calls.
        verify(businessObjectDefinitionTagService).getBusinessObjectDefinitionTagsByBusinessObjectDefinition(businessObjectDefinitionKey);
        verifyNoMoreInteractions(businessObjectDefinitionTagService);
        // Validate the returned object.
        assertEquals(BusinessObjectDefinitionTagKeys, result);
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByTag()
    {
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);
        BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey =
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));
        BusinessObjectDefinitionTagKeys businessObjectDefinitionTagKeys = new BusinessObjectDefinitionTagKeys(Arrays.asList(businessObjectDefinitionTagKey));

        when(businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByTag(tagKey)).thenReturn(businessObjectDefinitionTagKeys);

        // Get business object definition tags by tag.
        BusinessObjectDefinitionTagKeys result = businessObjectDefinitionTagRestController.getBusinessObjectDefinitionTagsByTag(TAG_TYPE, TAG_CODE);

        // Verify the external calls.
        verify(businessObjectDefinitionTagService).getBusinessObjectDefinitionTagsByTag(tagKey);
        verifyNoMoreInteractions(businessObjectDefinitionTagService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionTagKeys, result);
    }
}
