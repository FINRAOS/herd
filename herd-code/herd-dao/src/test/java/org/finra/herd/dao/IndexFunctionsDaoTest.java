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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchScroll;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.exception.ElasticsearchRestClientException;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JestClientHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.impl.IndexFunctionsDaoImpl;

public class IndexFunctionsDaoTest extends AbstractDaoTest
{
    @Mock
    private ElasticsearchRestHighLevelClientFactory elasticsearchRestHighLevelClientFactory;

    @InjectMocks
    private IndexFunctionsDaoImpl indexFunctionsDao;

    @Mock
    private HerdStringHelper herdStringHelper;

    @Mock
    private JsonHelper jsonHelper;

    @Mock
    private JestClientHelper jestClientHelper;


    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testIndexFunction() throws Exception
    {
        // Build mocks
        IndexResponse indexResponse = mock(IndexResponse.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        RestStatus restStatus = mock(RestStatus.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.index(any(IndexRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(indexResponse);
        when(indexResponse.status()).thenReturn(restStatus);

        // Call the method under test
        indexFunctionsDao.createIndexDocument(SEARCH_INDEX_NAME, SEARCH_INDEX_ID, SEARCH_INDEX_JSON_STRING);

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory).getRestHighLevelClient();
        verify(restHighLevelClient).index(any(IndexRequest.class), eq(RequestOptions.DEFAULT));
        verify(indexResponse).status();
        verify(restHighLevelClient).close();
        verifyNoMoreInteractions(elasticsearchRestHighLevelClientFactory, indexResponse, restHighLevelClient);
    }

    @Test(expected = ElasticsearchRestClientException.class)
    public void testIndexFunctionThrowsElasticsearchRestClientException() throws Exception
    {
        // Build mocks
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.index(any(IndexRequest.class), eq(RequestOptions.DEFAULT))).thenThrow(new IOException());

        // Call the method under test
        indexFunctionsDao.createIndexDocument(SEARCH_INDEX_NAME, SEARCH_INDEX_ID, SEARCH_INDEX_JSON_STRING);
    }

    @Test
    public void testValidateFunctionIndex() throws Exception
    {
        // Build mocks
        GetResponse getResponse = mock(GetResponse.class);
        IndexResponse indexResponse = mock(IndexResponse.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        RestStatus restStatus = mock(RestStatus.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.get(any(GetRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn(null);
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.index(any(IndexRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(indexResponse);
        when(indexResponse.status()).thenReturn(restStatus);

        // Call the method under test
        indexFunctionsDao.validateDocumentIndex(SEARCH_INDEX_NAME, SEARCH_INDEX_ID, SEARCH_INDEX_JSON_STRING);

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory, times(2)).getRestHighLevelClient();
        verify(restHighLevelClient).get(any(GetRequest.class), eq(RequestOptions.DEFAULT));
        verify(getResponse).getSourceAsString();
        verify(restHighLevelClient).index(any(IndexRequest.class), eq(RequestOptions.DEFAULT));
        verify(indexResponse).status();
        verify(restHighLevelClient, times(2)).close();
        verifyNoMoreInteractions(elasticsearchRestHighLevelClientFactory, getResponse, indexResponse, restHighLevelClient);
    }

    @Test
    public void testValidateFunctionUpdate() throws Exception
    {
        // Build mocks
        GetResponse getResponse = mock(GetResponse.class);
        IndexResponse indexResponse = mock(IndexResponse.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        RestStatus restStatus = mock(RestStatus.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.get(any(GetRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn(SEARCH_INDEX_JSON_STRING + "JSON UPDATE");
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.index(any(IndexRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(indexResponse);
        when(indexResponse.status()).thenReturn(restStatus);

        // Call the method under test
        indexFunctionsDao.validateDocumentIndex(SEARCH_INDEX_NAME, SEARCH_INDEX_ID, SEARCH_INDEX_JSON_STRING);

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory, times(2)).getRestHighLevelClient();
        verify(restHighLevelClient).get(any(GetRequest.class), eq(RequestOptions.DEFAULT));
        verify(getResponse).getSourceAsString();
        verify(restHighLevelClient).index(any(IndexRequest.class), eq(RequestOptions.DEFAULT));
        verify(indexResponse).status();
        verify(restHighLevelClient, times(2)).close();
        verifyNoMoreInteractions(elasticsearchRestHighLevelClientFactory, getResponse, indexResponse, restHighLevelClient);
    }

    @Test
    public void testValidateFunctionNoActionRequiredValidDocument() throws Exception
    {
        // Build mocks
        GetResponse getResponse = mock(GetResponse.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.get(any(GetRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn(SEARCH_INDEX_JSON_STRING);

        // Call the method under test
        indexFunctionsDao.validateDocumentIndex(SEARCH_INDEX_NAME, SEARCH_INDEX_ID, SEARCH_INDEX_JSON_STRING);

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory).getRestHighLevelClient();
        verify(restHighLevelClient).get(any(GetRequest.class), eq(RequestOptions.DEFAULT));
        verify(getResponse).getSourceAsString();
        verify(restHighLevelClient).close();
        verifyNoMoreInteractions(elasticsearchRestHighLevelClientFactory, getResponse, restHighLevelClient);
    }

    @Test(expected = ElasticsearchRestClientException.class)
    public void testValidateFunctionIndexThrowsElasticsearchRestClientException() throws Exception
    {
        // Build mocks
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.get(any(GetRequest.class), eq(RequestOptions.DEFAULT))).thenThrow(new IOException());

        // Call the method under test
        indexFunctionsDao.validateDocumentIndex(SEARCH_INDEX_NAME, SEARCH_INDEX_ID, SEARCH_INDEX_JSON_STRING);
    }

    @Test
    public void testIsValidFunction() throws Exception
    {
        // Build mocks
        GetResponse getResponse = mock(GetResponse.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.get(any(GetRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn(SEARCH_INDEX_JSON_STRING);

        // Call the method under test
        boolean isValid = indexFunctionsDao.isValidDocumentIndex(SEARCH_INDEX_NAME, SEARCH_INDEX_ID, SEARCH_INDEX_JSON_STRING);
        assertThat("IsValid is false when it should have been true.", isValid, is(true));

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory).getRestHighLevelClient();
        verify(restHighLevelClient).get(any(GetRequest.class), eq(RequestOptions.DEFAULT));
        verify(getResponse).getSourceAsString();
        verify(restHighLevelClient).close();
        verifyNoMoreInteractions(elasticsearchRestHighLevelClientFactory, getResponse, restHighLevelClient);
    }

    @Test
    public void testIsValidFunctionEmpty() throws Exception
    {
        // Build mocks
        GetResponse getResponse = mock(GetResponse.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.get(any(GetRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn(EMPTY_STRING);

        // Call the method under test
        boolean isValid = indexFunctionsDao.isValidDocumentIndex(SEARCH_INDEX_NAME, SEARCH_INDEX_ID, SEARCH_INDEX_JSON_STRING);
        assertThat("IsValid is true when it should have been false.", isValid, is(false));

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory).getRestHighLevelClient();
        verify(restHighLevelClient).get(any(GetRequest.class), eq(RequestOptions.DEFAULT));
        verify(getResponse).getSourceAsString();
        verify(restHighLevelClient).close();
        verifyNoMoreInteractions(elasticsearchRestHighLevelClientFactory, getResponse, restHighLevelClient);
    }

    @Test
    public void testIsValidFunctionNull() throws Exception
    {
        // Build mocks
        GetResponse getResponse = mock(GetResponse.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.get(any(GetRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn(null);

        // Call the method under test
        boolean isValid = indexFunctionsDao.isValidDocumentIndex(SEARCH_INDEX_NAME, SEARCH_INDEX_ID, SEARCH_INDEX_JSON_STRING);
        assertThat("IsValid is true when it should have been false.", isValid, is(false));

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory).getRestHighLevelClient();
        verify(restHighLevelClient).get(any(GetRequest.class), eq(RequestOptions.DEFAULT));
        verify(getResponse).getSourceAsString();
        verify(restHighLevelClient).close();
        verifyNoMoreInteractions(elasticsearchRestHighLevelClientFactory, getResponse, restHighLevelClient);
    }

    @Test
    public void testIsValidFunctionNotEqual() throws Exception
    {
        // Build mocks
        GetResponse getResponse = mock(GetResponse.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.get(any(GetRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn("JSON_NOT_EQUAL");

        // Call the method under test
        boolean isValid = indexFunctionsDao.isValidDocumentIndex(SEARCH_INDEX_NAME, SEARCH_INDEX_ID, SEARCH_INDEX_JSON_STRING);
        assertThat("IsValid is true when it should have been false.", isValid, is(false));

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory).getRestHighLevelClient();
        verify(restHighLevelClient).get(any(GetRequest.class), eq(RequestOptions.DEFAULT));
        verify(getResponse).getSourceAsString();
        verify(restHighLevelClient).close();
        verifyNoMoreInteractions(elasticsearchRestHighLevelClientFactory, getResponse, restHighLevelClient);
    }

    @Test(expected = ElasticsearchRestClientException.class)
    public void testIsValidFunctionThrowsElasticsearchRestClientException() throws Exception
    {
        // Build mocks
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.get(any(GetRequest.class), eq(RequestOptions.DEFAULT))).thenThrow(new IOException());

        // Call the method under test
        indexFunctionsDao.isValidDocumentIndex(SEARCH_INDEX_NAME, SEARCH_INDEX_ID, SEARCH_INDEX_JSON_STRING);
    }

    @Test
    public void testIndexExistsFunction() throws Exception
    {
        // Build mocks
        IndicesClient indicesClient = mock(IndicesClient.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
        when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(true);

        // Call the method under test
        boolean isValid = indexFunctionsDao.isIndexExists(SEARCH_INDEX_NAME);
        assertThat("IsValid is false when it should have been true.", isValid, is(true));

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory).getRestHighLevelClient();
        verify(restHighLevelClient).indices();
        verify(indicesClient).exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT));
        verify(restHighLevelClient).close();
        verifyNoMoreInteractions(elasticsearchRestHighLevelClientFactory, indicesClient, restHighLevelClient);
    }

    @Test(expected = ElasticsearchRestClientException.class)
    public void testIndexExistsFunctionThrowsElasticsearchRestClientException() throws Exception
    {
        // Build mocks
        IndicesClient indicesClient = mock(IndicesClient.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
        when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT))).thenThrow(new IOException());

        // Call the method under test
        indexFunctionsDao.isIndexExists(SEARCH_INDEX_NAME);
    }

    @Test
    public void testDeleteIndexFunction()
    {
        JestResult jestResult = mock(JestResult.class);
        // Build mocks
        when(jestClientHelper.execute(any())).thenReturn(jestResult);
        indexFunctionsDao.deleteIndex("Index");
        verify(jestClientHelper).execute(any());
        verify(jestResult).isSucceeded();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testCreateIndexFunction()
    {
        JestResult jestResult = mock(JestResult.class);

        // Build mocks
        when(jestClientHelper.execute(any())).thenReturn(jestResult);
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
        when(jestClientHelper.execute(any())).thenReturn(jestResult);
        when(jestResult.isSucceeded()).thenReturn(true);
        when(jestClientHelper.execute(any())).thenReturn(jestResultAliases);
        when(jestResultAliases.isSucceeded()).thenReturn(true);
        when(jestResultAliases.getJsonObject()).thenReturn(jsonObject);

        Map<String, String> documentMap = new HashMap<>();
        documentMap.put("1", "JSON");
        indexFunctionsDao.createIndexDocuments("INDEX_NAME", documentMap);

        verify(jestClientHelper, times(3)).execute(any());
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
        when(jestClientHelper.execute(any())).thenReturn(jestResult);
        when(jestClientHelper.execute(any())).thenReturn(jestResultAliases);
        when(jestResultAliases.isSucceeded()).thenReturn(true);
        when(jestResultAliases.getJsonObject()).thenReturn(jsonObject);
        when(jestResult.isSucceeded()).thenReturn(false);
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put("1", "JSON");
        indexFunctionsDao.createIndexDocuments("INDEX_NAME", documentMap);

        verify(jestClientHelper, times(3)).execute(any());
        verify(jestResultAliases).getJsonObject();
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testDeleteDocumentByIdFunction()
    {
        JestResult jestResult = mock(JestResult.class);

        when(jestClientHelper.execute(any())).thenReturn(jestResult);
        when(jestResult.isSucceeded()).thenReturn(true);

        indexFunctionsDao.deleteDocumentById("INDEX_NAME", "ID");
        verify(jestClientHelper).execute(any());
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
        when(jestClientHelper.execute(any())).thenReturn(jestResult);
        when(jestResult.isSucceeded()).thenReturn(true);
        when(jestClientHelper.execute(any())).thenReturn(jestResultAliases);
        when(jestResultAliases.isSucceeded()).thenReturn(true);
        when(jestResultAliases.getJsonObject()).thenReturn(jsonObject);
        // Call the method under test
        List<Long> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.add(1L);
        indexFunctionsDao.deleteIndexDocuments("INDEX_NAME", businessObjectDefinitionIds);

        // Verify the calls to external methods
        verify(jestClientHelper, times(3)).execute(any());
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
        when(jestClientHelper.execute(any())).thenReturn(jestResult);
        when(jestResult.isSucceeded()).thenReturn(false);
        when(jestClientHelper.execute(any())).thenReturn(jestResultAliases);
        when(jestResultAliases.isSucceeded()).thenReturn(true);
        when(jestResultAliases.getJsonObject()).thenReturn(jsonObject);
        // Call the method under test
        List<Long> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.add(1L);
        indexFunctionsDao.deleteIndexDocuments("INDEX_NAME", businessObjectDefinitionIds);

        // Verify the calls to external methods
        verify(jestClientHelper, times(3)).execute(any());
        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testNumberOfTypesInIndexFunction()
    {
        JestResult searchResult = mock(SearchResult.class);
        // Build mocks
        when(jestClientHelper.execute(any())).thenReturn(searchResult);
        when(searchResult.getSourceAsString()).thenReturn("100");
        indexFunctionsDao.getNumberOfTypesInIndex("INDEX_NAME");
        // Verify the calls to external methods
        verify(jestClientHelper).execute(any());
        verify(searchResult).getSourceAsString();

        verifyNoMoreInteractions(jestClientHelper);
    }

    @Test
    public void testIdsInIndexFunction()
    {
        JestResult jestResult = mock(JestResult.class);
        SearchResult searchResult = mock(SearchResult.class);
        List<String> idList = Lists.newArrayList("{id:1}");
        List<String> emptyList = new ArrayList<>();
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("_scroll_id", "100");
        when(jestClientHelper.execute(any(Search.class))).thenReturn(searchResult);
        when(searchResult.getSourceAsStringList()).thenReturn(idList);
        when(searchResult.getJsonObject()).thenReturn(jsonObject);
        when(jestClientHelper.execute(any(SearchScroll.class))).thenReturn(jestResult);
        when(jestResult.getSourceAsStringList()).thenReturn(emptyList);
        indexFunctionsDao.getIdsInIndex("INDEX_NAME");
        verify(jestClientHelper).execute(any(Search.class));
        verify(searchResult, times(2)).getSourceAsStringList();
        verify(searchResult).getJsonObject();
        verify(jestClientHelper).execute(any(SearchScroll.class));
        verify(jestResult).getSourceAsStringList();
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
        when(jestClientHelper.execute(any())).thenReturn(jestResult);
        when(jestResult.isSucceeded()).thenReturn(true);
        when(jestClientHelper.execute(any())).thenReturn(jestResultAliases);
        when(jestResultAliases.isSucceeded()).thenReturn(true);
        when(jestResultAliases.getJsonObject()).thenReturn(jsonObject);

        // Call the method under test
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put("1", "JSON");
        indexFunctionsDao.updateIndexDocuments("INDEX_NAME", documentMap);

        // Verify the calls to external methods
        verify(jestClientHelper, times(3)).execute(any());
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
        when(jestClientHelper.execute(any())).thenReturn(jestResult);
        when(jestResult.isSucceeded()).thenReturn(false);
        when(jestClientHelper.execute(any())).thenReturn(jestResultAliases);
        when(jestResultAliases.isSucceeded()).thenReturn(true);
        when(jestResultAliases.getJsonObject()).thenReturn(jsonObject);

        // Call the method under test
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put("1", "JSON");
        indexFunctionsDao.updateIndexDocuments("INDEX_NAME", documentMap);

        // Verify the calls to external methods
        verify(jestClientHelper, times(3)).execute(any());
        verifyNoMoreInteractions(jestClientHelper);
    }
}
