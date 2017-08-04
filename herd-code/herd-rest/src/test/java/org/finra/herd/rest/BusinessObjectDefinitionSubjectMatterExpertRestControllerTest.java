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
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpert;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKeys;
import org.finra.herd.service.BusinessObjectDefinitionSubjectMatterExpertService;

/**
 * This class tests various functionality within the business object definition subject matter expert REST controller.
 */
public class BusinessObjectDefinitionSubjectMatterExpertRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private BusinessObjectDefinitionSubjectMatterExpertRestController businessObjectDefinitionSubjectMatterExpertRestController;

    @Mock
    private BusinessObjectDefinitionSubjectMatterExpertService businessObjectDefinitionSubjectMatterExpertService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectDefinitionSubjectMatterExpert()
    {
        // Create a business object definition subject matter expert key.
        BusinessObjectDefinitionSubjectMatterExpertKey key = new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        BusinessObjectDefinitionSubjectMatterExpertCreateRequest request = new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(key);

        BusinessObjectDefinitionSubjectMatterExpert businessObjectDefinitionSubjectMatterExpert = new BusinessObjectDefinitionSubjectMatterExpert(ID, key);

        when(businessObjectDefinitionSubjectMatterExpertService.createBusinessObjectDefinitionSubjectMatterExpert(request))
            .thenReturn(businessObjectDefinitionSubjectMatterExpert);
        // Create a business object definition subject matter expert.
        BusinessObjectDefinitionSubjectMatterExpert resultBusinessObjectDefinitionSubjectMatterExpert =
            businessObjectDefinitionSubjectMatterExpertRestController.createBusinessObjectDefinitionSubjectMatterExpert(request);

        // Verify the external calls.
        verify(businessObjectDefinitionSubjectMatterExpertService).createBusinessObjectDefinitionSubjectMatterExpert(request);
        verifyNoMoreInteractions(businessObjectDefinitionSubjectMatterExpertService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionSubjectMatterExpert, resultBusinessObjectDefinitionSubjectMatterExpert);
    }

    @Test
    public void testDeleteBusinessObjectDefinitionSubjectMatterExpert()
    {
        // Create a business object definition subject matter expert key.
        BusinessObjectDefinitionSubjectMatterExpertKey key = new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        BusinessObjectDefinitionSubjectMatterExpert businessObjectDefinitionSubjectMatterExpert = new BusinessObjectDefinitionSubjectMatterExpert(ID, key);

        when(businessObjectDefinitionSubjectMatterExpertService.deleteBusinessObjectDefinitionSubjectMatterExpert(key))
            .thenReturn(businessObjectDefinitionSubjectMatterExpert);

        // Delete this business object definition subject matter expert.
        BusinessObjectDefinitionSubjectMatterExpert deletedBusinessObjectDefinitionSubjectMatterExpert =
            businessObjectDefinitionSubjectMatterExpertRestController.deleteBusinessObjectDefinitionSubjectMatterExpert(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Verify the external calls.
        verify(businessObjectDefinitionSubjectMatterExpertService).deleteBusinessObjectDefinitionSubjectMatterExpert(key);
        verifyNoMoreInteractions(businessObjectDefinitionSubjectMatterExpertService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionSubjectMatterExpert, deletedBusinessObjectDefinitionSubjectMatterExpert);
    }

    @Test
    public void testGetBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition() throws Exception
    {
        // Create business object definition subject matter expert keys. The keys are listed out of order to validate the sorting.
        List<BusinessObjectDefinitionSubjectMatterExpertKey> keys = Arrays
            .asList(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID_2),
                new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID));

        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        BusinessObjectDefinitionSubjectMatterExpertKeys businessObjectDefinitionSubjectMatterExpertKeys =
            new BusinessObjectDefinitionSubjectMatterExpertKeys(keys);

        when(businessObjectDefinitionSubjectMatterExpertService
            .getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(businessObjectDefinitionKey))
            .thenReturn(businessObjectDefinitionSubjectMatterExpertKeys);
        // Get a list of business object definition subject matter expert keys for the specified business object definition.
        BusinessObjectDefinitionSubjectMatterExpertKeys resultBusinessObjectDefinitionSubjectMatterExperts =
            businessObjectDefinitionSubjectMatterExpertRestController
                .getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(BDEF_NAMESPACE, BDEF_NAME);

        // Verify the external calls.
        verify(businessObjectDefinitionSubjectMatterExpertService)
            .getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(businessObjectDefinitionKey);
        verifyNoMoreInteractions(businessObjectDefinitionSubjectMatterExpertService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionSubjectMatterExpertKeys, resultBusinessObjectDefinitionSubjectMatterExperts);
    }
}
