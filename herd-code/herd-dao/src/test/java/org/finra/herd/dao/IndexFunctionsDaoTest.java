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
package org.finra.herd.dao;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.searchbox.client.JestResult;
import io.searchbox.core.SearchResult;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.DocsStats;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.helper.JestClientHelper;
import org.finra.herd.dao.impl.IndexFunctionsDaoImpl;

public class IndexFunctionsDaoTest extends AbstractDaoTest
{
    @InjectMocks
    private IndexFunctionsDaoImpl indexFunctionsDao;

    @Mock
    private JestClientHelper jestClientHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testIndexFunction()
    {
        SearchResult jestResult = mock(SearchResult.class);
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        // Call the method under test
        indexFunctionsDao.createIndexDocument("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");

        // Verify the calls to external methods
        verify(jestClientHelper).executeAction(any());
        verify(jestResult).isSucceeded();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testValidateFunctionIndex()
    {
        SearchResult jestResult = mock(SearchResult.class);
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestResult.getSourceAsString()).thenReturn("");
        indexFunctionsDao.validateDocumentIndex("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");
        // Verify the calls to external methods
        verify(jestClientHelper, times(2)).executeAction(any());
        verify(jestResult).getSourceAsString();
        verify(jestResult).isSucceeded();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testValidateFunctionUpdate()
    {
        SearchResult jestResult = mock(SearchResult.class);
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestResult.getSourceAsString()).thenReturn("JSON_UPDATE");
        indexFunctionsDao.validateDocumentIndex("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");
        // Verify the calls to external methods
        verify(jestClientHelper, times(2)).executeAction(any());
        verify(jestResult).getSourceAsString();
        verify(jestResult).isSucceeded();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testValidateFunctionNoActionRequiredValidDocument()
    {
        SearchResult jestResult = mock(SearchResult.class);
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestResult.getSourceAsString()).thenReturn("JSON");
        indexFunctionsDao.validateDocumentIndex("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");
        // Verify the calls to external methods
        verify(jestClientHelper, times(1)).executeAction(any());
        verify(jestResult).getSourceAsString();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testIsValidFunction()
    {
        SearchResult jestResult = mock(SearchResult.class);
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestResult.getSourceAsString()).thenReturn("JSON");

        // Call the method under test
        boolean isValid = indexFunctionsDao.isValidDocumentIndex("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");
        assertThat("IsValid is false when it should have been true.", isValid, is(true));

        verify(jestClientHelper, times(1)).executeAction(any());
        verify(jestResult).getSourceAsString();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testIsValidFunctionEmpty()
    {
        SearchResult jestResult = mock(SearchResult.class);
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestResult.getSourceAsString()).thenReturn("");

        // Call the method under test
        boolean isValid = indexFunctionsDao.isValidDocumentIndex("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");
        assertThat("IsValid is true when it should have been false.", isValid, is(false));

        verify(jestClientHelper, times(1)).executeAction(any());
        verify(jestResult).getSourceAsString();
        verifyNoMoreInteractions(jestClientHelper);

    }

    @Test
    public void testIsValidFunctionNull()
    {
        SearchResult jestResult = mock(SearchResult.class);
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestResult.getSourceAsString()).thenReturn(null);

        // Call the method under test
        boolean isValid = indexFunctionsDao.isValidDocumentIndex("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");
        assertThat("IsValid is true when it should have been false.", isValid, is(false));

        verify(jestClientHelper, times(1)).executeAction(any());
        verify(jestResult).getSourceAsString();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testIsValidFunctionNotEqual()
    {
        SearchResult jestResult = mock(SearchResult.class);
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestResult.getSourceAsString()).thenReturn("JSON_NOT_EQUAL");

        // Call the method under test
        boolean isValid = indexFunctionsDao.isValidDocumentIndex("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");
        assertThat("IsValid is true when it should have been false.", isValid, is(false));

        verify(jestClientHelper, times(1)).executeAction(any());
        verify(jestResult).getSourceAsString();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testIndexExistsFunction()
    {
        JestResult jestResult = mock(JestResult.class);
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        indexFunctionsDao.isIndexExists("Index");
        verify(jestClientHelper).executeAction(any());
        verify(jestResult).isSucceeded();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testDeleteIndexFunction()
    {
        JestResult jestResult = mock(JestResult.class);
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        indexFunctionsDao.deleteIndex("Index");
        verify(jestClientHelper).executeAction(any());
        verify(jestResult).isSucceeded();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testCreateIndexFunction()
    {
        JestResult jestResult = mock(JestResult.class);
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        //indexFunctionsDao.createIndex("Index", "Document_Type", "Mapping", "Settings");


        // verifyNoMoreInteractions(jestClientHelper);
    }



    @Test
    public void testCreateIndexDocumentsFunction()
    {
        SearchResult jestResult = mock(SearchResult.class);
        JestResult jestResultAliases = mock(JestResult.class);
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("INDEX_NAME_1", "INDEX_NAME");
        jsonObject.addProperty("INDEX_NAME_2", "INDEX_NAME");

        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestResult.isSucceeded()).thenReturn(true);
        when(jestClientHelper.executeAction(any())).thenReturn(jestResultAliases);
        when(jestResultAliases.isSucceeded()).thenReturn(true);
        when(jestResultAliases.getJsonObject()).thenReturn(jsonObject);

        Map<String, String> documentMap = new HashMap<>();
        documentMap.put("1", "JSON");
        indexFunctionsDao.createIndexDocuments("INDEX_NAME", "DOCUMENT_TYPE", documentMap);

        verify(jestClientHelper, times(3)).executeAction(any());
        verify(jestResultAliases).getJsonObject();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testCreateIndexDocumentsFunctionWithFailures()
    {
        SearchResult jestResult = mock(SearchResult.class);
        JestResult jestResultAliases = mock(JestResult.class);
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("INDEX_NAME_1", "INDEX_NAME");
        jsonObject.addProperty("INDEX_NAME_2", "INDEX_NAME");

        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestClientHelper.executeAction(any())).thenReturn(jestResultAliases);
        when(jestResultAliases.isSucceeded()).thenReturn(true);
        when(jestResultAliases.getJsonObject()).thenReturn(jsonObject);
        when(jestResult.isSucceeded()).thenReturn(false);
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put("1", "JSON");
        indexFunctionsDao.createIndexDocuments("INDEX_NAME", "DOCUMENT_TYPE", documentMap);

        verify(jestClientHelper, times(3)).executeAction(any());
        verify(jestResultAliases).getJsonObject();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testDeleteDocumentByIdFunction()
    {
        JestResult jestResult = mock(JestResult.class);

        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestResult.isSucceeded()).thenReturn(true);

        indexFunctionsDao.deleteDocumentById("INDEX_NAME", "DOCUMENT_TYPE", "ID");
        verify(jestClientHelper).executeAction(any());
        verify(jestResult).isSucceeded();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testDeleteIndexDocumentsFunction()
    {
        SearchResult jestResult = mock(SearchResult.class);
        JestResult jestResultAliases = mock(JestResult.class);
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("INDEX_NAME_1", "INDEX_NAME");
        jsonObject.addProperty("INDEX_NAME_2", "INDEX_NAME");
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestResult.isSucceeded()).thenReturn(true);
        when(jestClientHelper.executeAction(any())).thenReturn(jestResultAliases);
        when(jestResultAliases.isSucceeded()).thenReturn(true);
        when(jestResultAliases.getJsonObject()).thenReturn(jsonObject);
        // Call the method under test
        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.add(1);
        indexFunctionsDao.deleteIndexDocuments("INDEX_NAME", "DOCUMENT_TYPE", businessObjectDefinitionIds);

        // Verify the calls to external methods
        verify(jestClientHelper, times(3)).executeAction(any());
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testDeleteIndexDocumentsFunctionWithFailures()
    {
        SearchResult jestResult = mock(SearchResult.class);
        JestResult jestResultAliases = mock(JestResult.class);
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("INDEX_NAME_1", "INDEX_NAME");
        jsonObject.addProperty("INDEX_NAME_2", "INDEX_NAME");
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestResult.isSucceeded()).thenReturn(false);
        when(jestClientHelper.executeAction(any())).thenReturn(jestResultAliases);
        when(jestResultAliases.isSucceeded()).thenReturn(true);
        when(jestResultAliases.getJsonObject()).thenReturn(jsonObject);
        // Call the method under test
        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.add(1);
        indexFunctionsDao.deleteIndexDocuments("INDEX_NAME", "DOCUMENT_TYPE", businessObjectDefinitionIds);

        // Verify the calls to external methods
        verify(jestClientHelper, times(3)).executeAction(any());
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testNumberOfTypesInIndexFunction()
    {
        JestResult searchResult = mock(SearchResult.class);
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(searchResult);
        when(searchResult.getSourceAsString()).thenReturn("100");
        indexFunctionsDao.getNumberOfTypesInIndex("INDEX_NAME", "DOCUMENT_TYPE");
        // Verify the calls to external methods
        verify(jestClientHelper).executeAction(any());
        verify(searchResult).getSourceAsString();

        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testIdsInIndexFunction()
    {
        JestResult jestResult = mock(JestResult.class);
        SearchResult searchResult = mock(SearchResult.class);
        List<String> idList = Arrays.asList("{id:1}");
        List<String> emptyList = new ArrayList<>();
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("_scroll_id", "100");
        when(jestClientHelper.searchExecute(any())).thenReturn(searchResult);
        when(searchResult.getSourceAsStringList()).thenReturn(idList);
        when(searchResult.getJsonObject()).thenReturn(jsonObject);
        when(jestClientHelper.searchScrollExecute(any())).thenReturn(jestResult);
        when(jestResult.getSourceAsStringList()).thenReturn(emptyList);
        indexFunctionsDao.getIdsInIndex("INDEX_NAME", "DOCUMENT_TYPE");
        verify(jestClientHelper).searchExecute(any());
        verify(searchResult, times(2)).getSourceAsStringList();
        verify(searchResult).getJsonObject();
        verify(jestClientHelper).searchScrollExecute(any());
        verify(jestResult).getSourceAsStringList();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testUpdateIndexDocumentsFunction()
    {
        SearchResult jestResult = mock(SearchResult.class);
        JestResult jestResultAliases = mock(JestResult.class);
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("INDEX_NAME_1", "INDEX_NAME");
        jsonObject.addProperty("INDEX_NAME_2", "INDEX_NAME");
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestResult.isSucceeded()).thenReturn(true);
        when(jestClientHelper.executeAction(any())).thenReturn(jestResultAliases);
        when(jestResultAliases.isSucceeded()).thenReturn(true);
        when(jestResultAliases.getJsonObject()).thenReturn(jsonObject);

        // Call the method under test
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put("1", "JSON");
        indexFunctionsDao.updateIndexDocuments("INDEX_NAME", "DOCUMENT_TYPE", documentMap);

        // Verify the calls to external methods
        verify(jestClientHelper, times(3)).executeAction(any());
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testUpdateIndexDocumentsFunctionWithFailures()
    {
        SearchResult jestResult = mock(SearchResult.class);
        JestResult jestResultAliases = mock(JestResult.class);
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("INDEX_NAME_1", "INDEX_NAME");
        jsonObject.addProperty("INDEX_NAME_2", "INDEX_NAME");
        // Build mocks
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);
        when(jestResult.isSucceeded()).thenReturn(false);
        when(jestClientHelper.executeAction(any())).thenReturn(jestResultAliases);
        when(jestResultAliases.isSucceeded()).thenReturn(true);
        when(jestResultAliases.getJsonObject()).thenReturn(jsonObject);

        // Call the method under test
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put("1", "JSON");
        indexFunctionsDao.updateIndexDocuments("INDEX_NAME", "DOCUMENT_TYPE", documentMap);

        // Verify the calls to external methods
        verify(jestClientHelper, times(3)).executeAction(any());
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testGetIndexSettings()
    {
        // Create a mocked jest result.
        JestResult jestResult = mock(JestResult.class);
        JsonObject jsonObject =
            new JsonParser().parse(String.format("{\"%s\": {\"settings\": {\"%s\": \"%s\"}}}", SEARCH_INDEX_NAME, ATTRIBUTE_NAME, ATTRIBUTE_VALUE))
                .getAsJsonObject();
        when(jestResult.isSucceeded()).thenReturn(true);
        when(jestResult.getJsonObject()).thenReturn(jsonObject);

        // Mock the external calls.
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);

        // Call the method under test.
        Settings result = indexFunctionsDao.getIndexSettings(SEARCH_INDEX_NAME);

        // Verify the external calls.
        verify(jestClientHelper).executeAction(any());
        verifyNoMoreInteractions(jestClientHelper);

        // Validate the results.
        assertEquals(Settings.builder().put(ATTRIBUTE_NAME, ATTRIBUTE_VALUE).build(), result);
    }

    @Test
    public void testGetIndexSettingsInvalidResponse()
    {
        // Create a mocked jest result.
        JestResult jestResult = mock(JestResult.class);
        JsonObject jsonObject = new JsonObject();
        when(jestResult.isSucceeded()).thenReturn(true);
        when(jestResult.getJsonObject()).thenReturn(jsonObject);

        // Mock the external calls.
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);

        // Try to call the method under test.
        try
        {
            indexFunctionsDao.getIndexSettings(SEARCH_INDEX_NAME);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Unexpected response received when attempting to retrieve settings for \"%s\" index.", SEARCH_INDEX_NAME), e.getMessage());
        }

        // Verify the external calls.
        verify(jestClientHelper).executeAction(any());
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testGetIndexSettingsJestClientExecuteNoSucceeded()
    {
        // Create a mocked jest result.
        JestResult jestResult = mock(JestResult.class);
        when(jestResult.isSucceeded()).thenReturn(false);
        when(jestResult.getErrorMessage()).thenReturn(ERROR_MESSAGE);

        // Mock the external calls.
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);

        // Try to call the method under test.
        try
        {
            indexFunctionsDao.getIndexSettings(SEARCH_INDEX_NAME);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Unable to retrieve settings for \"%s\" index. Error: %s", SEARCH_INDEX_NAME, ERROR_MESSAGE), e.getMessage());
        }

        // Verify the external calls.
        verify(jestClientHelper).executeAction(any());
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testGetIndexStats()
    {
        // Create a mocked jest result.
        JestResult jestResult = mock(JestResult.class);
        JsonObject jsonObject = new JsonParser().parse(String
            .format("{\"indices\": {\"%s\": {\"primaries\": {\"docs\": {\"count\": %d, \"deleted\": %d}}}}}", SEARCH_INDEX_NAME, LONG_VALUE, LONG_VALUE_2))
            .getAsJsonObject();
        when(jestResult.isSucceeded()).thenReturn(true);
        when(jestResult.getJsonObject()).thenReturn(jsonObject);

        // Mock the external calls.
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);

        // Call the method under test.
        DocsStats result = indexFunctionsDao.getIndexStats(SEARCH_INDEX_NAME);

        // Verify the external calls.
        verify(jestClientHelper).executeAction(any());
        verifyNoMoreInteractions(jestClientHelper);

        // Validate the results.
        assertNotNull(result);
        assertEquals(LONG_VALUE.longValue(), result.getCount());
        assertEquals(LONG_VALUE_2.longValue(), result.getDeleted());
    }

    @Test
    public void testGetIndexStatsInvalidResponse()
    {
        // Create a mocked jest result.
        JestResult jestResult = mock(JestResult.class);
        JsonObject jsonObject = new JsonObject();
        when(jestResult.isSucceeded()).thenReturn(true);
        when(jestResult.getJsonObject()).thenReturn(jsonObject);

        // Mock the external calls.
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);

        // Try to call the method under test.
        try
        {
            indexFunctionsDao.getIndexStats(SEARCH_INDEX_NAME);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Unexpected response received when attempting to retrieve stats for \"%s\" index.", SEARCH_INDEX_NAME), e.getMessage());
        }

        // Verify the external calls.
        verify(jestClientHelper).executeAction(any());
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testGetIndexStatsJestClientExecuteNoSucceeded()
    {
        // Create a mocked jest result.
        JestResult jestResult = mock(JestResult.class);
        when(jestResult.isSucceeded()).thenReturn(false);
        when(jestResult.getErrorMessage()).thenReturn(ERROR_MESSAGE);

        // Mock the external calls.
        when(jestClientHelper.executeAction(any())).thenReturn(jestResult);

        // Try to call the method under test.
        try
        {
            indexFunctionsDao.getIndexStats(SEARCH_INDEX_NAME);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Unable to retrieve stats for \"%s\" index. Error: %s", SEARCH_INDEX_NAME, ERROR_MESSAGE), e.getMessage());
        }

        // Verify the external calls.
        verify(jestClientHelper).executeAction(any());
        verifyNoMoreInteractions(jestClientHelper);
    }
}
