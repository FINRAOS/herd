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

import static org.finra.herd.service.impl.BusinessObjectDefinitionColumnServiceImpl.DESCRIPTION_FIELD;
import static org.finra.herd.service.impl.BusinessObjectDefinitionColumnServiceImpl.SCHEMA_COLUMN_NAME_FIELD;
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

import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumn;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.service.BusinessObjectDefinitionColumnService;

/**
 * This class tests various functionality within the business object definition column REST controller.
 */
public class BusinessObjectDefinitionColumnRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private BusinessObjectDefinitionColumnRestController businessObjectDefinitionColumnRestController;

    @Mock
    private BusinessObjectDefinitionColumnService businessObjectDefinitionColumnService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectDefinitionColumn()
    {
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);
        BusinessObjectDefinitionColumn businessObjectDefinitionColumn =
            new BusinessObjectDefinitionColumn(ID, businessObjectDefinitionColumnKey, COLUMN_NAME, BDEF_COLUMN_DESCRIPTION);
        BusinessObjectDefinitionColumnCreateRequest businessObjectDefinitionColumnCreateRequest =
            new BusinessObjectDefinitionColumnCreateRequest(businessObjectDefinitionColumnKey, COLUMN_NAME, BDEF_COLUMN_DESCRIPTION);

        when(businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(businessObjectDefinitionColumnCreateRequest))
            .thenReturn(businessObjectDefinitionColumn);
        // Create a business object definition column.
        BusinessObjectDefinitionColumn resultBusinessObjectDefinitionColumn =
            businessObjectDefinitionColumnRestController.createBusinessObjectDefinitionColumn(businessObjectDefinitionColumnCreateRequest);

        // Verify the external calls.
        verify(businessObjectDefinitionColumnService).createBusinessObjectDefinitionColumn(businessObjectDefinitionColumnCreateRequest);
        verifyNoMoreInteractions(businessObjectDefinitionColumnService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionColumn, resultBusinessObjectDefinitionColumn);
    }

    @Test
    public void testDeleteBusinessObjectDefinitionColumn()
    {
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);
        BusinessObjectDefinitionColumn businessObjectDefinitionColumn =
            new BusinessObjectDefinitionColumn(ID, businessObjectDefinitionColumnKey, COLUMN_NAME, BDEF_COLUMN_DESCRIPTION);

        when(businessObjectDefinitionColumnService.deleteBusinessObjectDefinitionColumn(businessObjectDefinitionColumnKey))
            .thenReturn(businessObjectDefinitionColumn);

        BusinessObjectDefinitionColumn deletedBusinessObjectDefinitionColumn =
            businessObjectDefinitionColumnRestController.deleteBusinessObjectDefinitionColumn(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);
        // Verify the external calls.
        verify(businessObjectDefinitionColumnService).deleteBusinessObjectDefinitionColumn(businessObjectDefinitionColumnKey);
        verifyNoMoreInteractions(businessObjectDefinitionColumnService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionColumn, deletedBusinessObjectDefinitionColumn);
    }

    @Test
    public void testGetBusinessObjectDefinitionColumn()
    {
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);
        BusinessObjectDefinitionColumn businessObjectDefinitionColumn =
            new BusinessObjectDefinitionColumn(ID, businessObjectDefinitionColumnKey, COLUMN_NAME, BDEF_COLUMN_DESCRIPTION);

        when(businessObjectDefinitionColumnService.getBusinessObjectDefinitionColumn(businessObjectDefinitionColumnKey))
            .thenReturn(businessObjectDefinitionColumn);

        // Get the business object definition column.
        BusinessObjectDefinitionColumn resultBusinessObjectDefinitionColumn =
            businessObjectDefinitionColumnRestController.getBusinessObjectDefinitionColumn(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Verify the external calls.
        verify(businessObjectDefinitionColumnService).getBusinessObjectDefinitionColumn(businessObjectDefinitionColumnKey);
        verifyNoMoreInteractions(businessObjectDefinitionColumnService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionColumn, resultBusinessObjectDefinitionColumn);
    }

    @Test
    public void testGetBusinessObjectDefinitionColumns()
    {
        // Create and persist business object definition column entities.
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2), DESCRIPTION_2);
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME), DESCRIPTION);

        BusinessObjectDefinitionColumnKeys businessObjectDefinitionColumnKeys =
            new BusinessObjectDefinitionColumnKeys(Arrays.asList(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2)));

        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);
        when(businessObjectDefinitionColumnService.getBusinessObjectDefinitionColumns(businessObjectDefinitionKey))
            .thenReturn(businessObjectDefinitionColumnKeys);

        // Get a list of business object definition column keys.
        BusinessObjectDefinitionColumnKeys resultBusinessObjectDefinitionColumnKeys =
            businessObjectDefinitionColumnRestController.getBusinessObjectDefinitionColumns(BDEF_NAMESPACE, BDEF_NAME);
        // Verify the external calls.
        verify(businessObjectDefinitionColumnService).getBusinessObjectDefinitionColumns(businessObjectDefinitionKey);
        verifyNoMoreInteractions(businessObjectDefinitionColumnService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionColumnKeys, resultBusinessObjectDefinitionColumnKeys);
    }

    @Test
    public void testSearchBusinessObjectDefinitionColumns()
    {
        BusinessObjectDefinitionColumnSearchRequest request = new BusinessObjectDefinitionColumnSearchRequest(Arrays
            .asList(new BusinessObjectDefinitionColumnSearchFilter(Arrays.asList(new BusinessObjectDefinitionColumnSearchKey(BDEF_NAMESPACE, BDEF_NAME)))));

        BusinessObjectDefinitionColumnSearchResponse businessObjectDefinitionColumnSearchResponse = new BusinessObjectDefinitionColumnSearchResponse(Arrays
            .asList(new BusinessObjectDefinitionColumn(NO_ID, new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME), COLUMN_NAME,
                BDEF_COLUMN_DESCRIPTION),
                new BusinessObjectDefinitionColumn(NO_ID, new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2), COLUMN_NAME_2,
                    BDEF_COLUMN_DESCRIPTION_2)));

        Set<String> set = Sets.newHashSet(SCHEMA_COLUMN_NAME_FIELD, DESCRIPTION_FIELD);

        when(businessObjectDefinitionColumnService.searchBusinessObjectDefinitionColumns(request, set))
            .thenReturn(businessObjectDefinitionColumnSearchResponse);
        // Search the business object definition columns.
        BusinessObjectDefinitionColumnSearchResponse resultSearchResponse =
            businessObjectDefinitionColumnRestController.searchBusinessObjectDefinitionColumns(set, request);
        // Verify the external calls.
        verify(businessObjectDefinitionColumnService).searchBusinessObjectDefinitionColumns(request, set);
        verifyNoMoreInteractions(businessObjectDefinitionColumnService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionColumnSearchResponse, resultSearchResponse);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionColumn()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);
        BusinessObjectDefinitionColumnUpdateRequest request = new BusinessObjectDefinitionColumnUpdateRequest(BDEF_COLUMN_DESCRIPTION_2);

        BusinessObjectDefinitionColumn businessObjectDefinitionColumn =
            new BusinessObjectDefinitionColumn(ID, businessObjectDefinitionColumnKey, COLUMN_NAME, BDEF_COLUMN_DESCRIPTION);

        when(businessObjectDefinitionColumnService.updateBusinessObjectDefinitionColumn(businessObjectDefinitionColumnKey, request))
            .thenReturn(businessObjectDefinitionColumn);
        // Update the business object definition column.
        BusinessObjectDefinitionColumn updatedBusinessObjectDefinitionColumn =
            businessObjectDefinitionColumnRestController.updateBusinessObjectDefinitionColumn(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME, request);
        // Verify the external calls.
        verify(businessObjectDefinitionColumnService).updateBusinessObjectDefinitionColumn(businessObjectDefinitionColumnKey, request);
        verifyNoMoreInteractions(businessObjectDefinitionColumnService);
        // Validate the returned object.
        assertEquals(businessObjectDefinitionColumn, updatedBusinessObjectDefinitionColumn);
    }
}
