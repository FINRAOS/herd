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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.TransportClientFactory;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.functional.SearchFunctions;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;

/**
 * This class tests functionality within the search index helper service implementation.
 */
public class SearchIndexHelperServiceImplTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Mock
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Mock
    private SearchFunctions searchFunctions;

    @Mock
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @InjectMocks
    private SearchIndexHelperServiceImpl searchIndexHelperServiceImpl;

    @Mock
    private TransportClientFactory transportClientFactory;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testValidateSearchIndexSizeValidationFails()
    {
        // Mock the external calls. Please note that we mock index size not to be equal to the business object definition entity list size.
        when(searchFunctions.getNumberOfTypesInIndexFunction()).thenReturn((indexName, documentType) -> 1L);

        // Index all business object definitions defined in the system.
        boolean response = searchIndexHelperServiceImpl.validateSearchIndexSize(SEARCH_INDEX_NAME, SEARCH_INDEX_DOCUMENT_TYPE, 2);

        // Verify the external calls.
        verify(searchFunctions).getNumberOfTypesInIndexFunction();
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionHelper, searchFunctions, searchIndexDaoHelper, transportClientFactory);

        // Validate the results.
        assertFalse(response);
    }

    @Test
    public void testValidateSearchIndexSizeValidationPasses()
    {
        // Mock the external calls. Please note that we mock index size to be equal to the business object definition entity list size.
        when(searchFunctions.getNumberOfTypesInIndexFunction()).thenReturn((indexName, documentType) -> 2L);

        // Index all business object definitions defined in the system.
        boolean response = searchIndexHelperServiceImpl.validateSearchIndexSize(SEARCH_INDEX_NAME, SEARCH_INDEX_DOCUMENT_TYPE, 2);

        // Verify the external calls.
        verify(searchFunctions).getNumberOfTypesInIndexFunction();
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionHelper, searchFunctions, searchIndexDaoHelper, transportClientFactory);

        // Validate the results.
        assertTrue(response);
    }
}
