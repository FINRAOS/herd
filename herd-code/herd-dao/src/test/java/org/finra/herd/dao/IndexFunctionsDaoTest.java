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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchScroll;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.cluster.metadata.AliasMetaData;
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
    public void testDeleteIndexFunction() throws Exception
    {
        // Build mocks
        AcknowledgedResponse acknowledgedResponse = mock(AcknowledgedResponse.class);
        IndicesClient indicesClient = mock(IndicesClient.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
        when(indicesClient.delete(any(DeleteIndexRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(acknowledgedResponse);
        when(acknowledgedResponse.isAcknowledged()).thenReturn(true);

        // Call the method under test
        indexFunctionsDao.deleteIndex(SEARCH_INDEX_NAME);

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory).getRestHighLevelClient();
        verify(restHighLevelClient).indices();
        verify(indicesClient).delete(any(DeleteIndexRequest.class), eq(RequestOptions.DEFAULT));
        verify(acknowledgedResponse).isAcknowledged();
        verify(restHighLevelClient).close();
        verifyNoMoreInteractions(acknowledgedResponse, elasticsearchRestHighLevelClientFactory, indicesClient, restHighLevelClient);
    }

    @Test(expected = ElasticsearchRestClientException.class)
    public void testDeleteIndexFunctionThrowsElasticsearchRestClientException() throws Exception
    {
        // Build mocks
        IndicesClient indicesClient = mock(IndicesClient.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
        when(indicesClient.delete(any(DeleteIndexRequest.class), eq(RequestOptions.DEFAULT))).thenThrow(new IOException());

        // Call the method under test
        indexFunctionsDao.deleteIndex(SEARCH_INDEX_NAME);
    }

    @Test
    public void testCreateIndexFunction() throws Exception
    {
        // Build mocks
        CreateIndexResponse createIndexResponse = mock(CreateIndexResponse.class);
        IndicesClient indicesClient = mock(IndicesClient.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
        when(indicesClient.create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(createIndexResponse);
        when(createIndexResponse.isAcknowledged()).thenReturn(true);

        // Call the method under test
        indexFunctionsDao.createIndex(SEARCH_INDEX_NAME, SEARCH_INDEX_MAPPING, SEARCH_INDEX_SETTINGS_JSON, SEARCH_INDEX_ALIAS_BDEF);

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory).getRestHighLevelClient();
        verify(restHighLevelClient).indices();
        verify(indicesClient).create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT));
        verify(createIndexResponse).isAcknowledged();
        verify(restHighLevelClient).close();
        verifyNoMoreInteractions(elasticsearchRestHighLevelClientFactory, createIndexResponse, indicesClient, restHighLevelClient);
    }

    @Test(expected = ElasticsearchRestClientException.class)
    public void testCreateIndexFunctionThrowsElasticsearchRestClientException() throws Exception
    {
        // Build mocks
        IndicesClient indicesClient = mock(IndicesClient.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
        when(indicesClient.create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT))).thenThrow(new IOException());

        // Call the method under test
        indexFunctionsDao.createIndex(SEARCH_INDEX_NAME, SEARCH_INDEX_MAPPING, SEARCH_INDEX_SETTINGS_JSON, SEARCH_INDEX_ALIAS_BDEF);
    }

    @Test
    public void testCreateIndexDocumentsFunction() throws Exception
    {
        // Build mocks
        GetAliasesResponse getAliasesResponse = mock(GetAliasesResponse.class);
        IndicesClient indicesClient = mock(IndicesClient.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        AliasMetaData aliasMetaData = mock(AliasMetaData.class);
        BulkResponse bulkResponse = mock(BulkResponse.class);

        // Create objects needed for the test.
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put(SEARCH_INDEX_DOCUMENT, SEARCH_INDEX_DOCUMENT_JSON);

        Set<AliasMetaData> aliasMetaDataSet = new HashSet<>();
        aliasMetaDataSet.add(aliasMetaData);

        Map<String, Set<AliasMetaData>> aliases = new HashMap<>();
        aliases.put(SEARCH_INDEX_ALIAS_BDEF, aliasMetaDataSet);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
        when(indicesClient.getAlias(any(GetAliasesRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(getAliasesResponse);
        when(getAliasesResponse.getAliases()).thenReturn(aliases);
        when(restHighLevelClient.bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(bulkResponse);
        when(bulkResponse.hasFailures()).thenReturn(false);

        // Call the method under test
        indexFunctionsDao.createIndexDocuments(SEARCH_INDEX_NAME, documentMap);

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory, times(2)).getRestHighLevelClient();
        verify(restHighLevelClient).indices();
        verify(restHighLevelClient).bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT));
        verify(indicesClient).getAlias(any(GetAliasesRequest.class), eq(RequestOptions.DEFAULT));
        verify(getAliasesResponse).getAliases();
        verify(bulkResponse).hasFailures();
        verify(restHighLevelClient, times(2)).close();
        verifyNoMoreInteractions(bulkResponse, elasticsearchRestHighLevelClientFactory, getAliasesResponse, indicesClient, restHighLevelClient);
    }

    @Test
    public void testCreateIndexDocumentsFunctionWithFailures() throws Exception
    {
        // Build mocks
        GetAliasesResponse getAliasesResponse = mock(GetAliasesResponse.class);
        IndicesClient indicesClient = mock(IndicesClient.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        AliasMetaData aliasMetaData = mock(AliasMetaData.class);
        BulkResponse bulkResponse = mock(BulkResponse.class);

        // Create objects needed for the test.
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put(SEARCH_INDEX_DOCUMENT, SEARCH_INDEX_DOCUMENT_JSON);

        Set<AliasMetaData> aliasMetaDataSet = new HashSet<>();
        aliasMetaDataSet.add(aliasMetaData);

        Map<String, Set<AliasMetaData>> aliases = new HashMap<>();
        aliases.put(SEARCH_INDEX_ALIAS_BDEF, aliasMetaDataSet);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
        when(indicesClient.getAlias(any(GetAliasesRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(getAliasesResponse);
        when(getAliasesResponse.getAliases()).thenReturn(aliases);
        when(restHighLevelClient.bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(bulkResponse);
        when(bulkResponse.hasFailures()).thenReturn(true);
        when(bulkResponse.buildFailureMessage()).thenReturn(ERROR_MESSAGE);

        // Call the method under test
        indexFunctionsDao.createIndexDocuments(SEARCH_INDEX_NAME, documentMap);

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory, times(2)).getRestHighLevelClient();
        verify(restHighLevelClient).indices();
        verify(restHighLevelClient).bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT));
        verify(indicesClient).getAlias(any(GetAliasesRequest.class), eq(RequestOptions.DEFAULT));
        verify(getAliasesResponse).getAliases();
        verify(bulkResponse).hasFailures();
        verify(bulkResponse).buildFailureMessage();
        verify(restHighLevelClient, times(2)).close();
        verifyNoMoreInteractions(bulkResponse, elasticsearchRestHighLevelClientFactory, getAliasesResponse, indicesClient, restHighLevelClient);
    }

    @Test(expected = ElasticsearchRestClientException.class)
    public void testCreateIndexDocumentsFunctionThrowsElasticsearchRestClientExceptionWhenGetAliasRequest() throws Exception
    {
        // Build mocks
        GetAliasesResponse getAliasesResponse = mock(GetAliasesResponse.class);
        IndicesClient indicesClient = mock(IndicesClient.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        AliasMetaData aliasMetaData = mock(AliasMetaData.class);
        BulkResponse bulkResponse = mock(BulkResponse.class);

        // Create objects needed for the test.
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put(SEARCH_INDEX_DOCUMENT, SEARCH_INDEX_DOCUMENT_JSON);

        Set<AliasMetaData> aliasMetaDataSet = new HashSet<>();
        aliasMetaDataSet.add(aliasMetaData);

        Map<String, Set<AliasMetaData>> aliases = new HashMap<>();
        aliases.put(SEARCH_INDEX_ALIAS_BDEF, aliasMetaDataSet);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
        when(indicesClient.getAlias(any(GetAliasesRequest.class), eq(RequestOptions.DEFAULT))).thenThrow(new IOException());

        // Call the method under test
        indexFunctionsDao.createIndexDocuments(SEARCH_INDEX_NAME, documentMap);
    }

    @Test(expected = ElasticsearchRestClientException.class)
    public void testCreateIndexDocumentsFunctionThrowsElasticsearchRestClientExceptionWhenBulkRequest() throws Exception
    {
        // Build mocks
        GetAliasesResponse getAliasesResponse = mock(GetAliasesResponse.class);
        IndicesClient indicesClient = mock(IndicesClient.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        AliasMetaData aliasMetaData = mock(AliasMetaData.class);
        BulkResponse bulkResponse = mock(BulkResponse.class);

        // Create objects needed for the test.
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put(SEARCH_INDEX_DOCUMENT, SEARCH_INDEX_DOCUMENT_JSON);

        Set<AliasMetaData> aliasMetaDataSet = new HashSet<>();
        aliasMetaDataSet.add(aliasMetaData);

        Map<String, Set<AliasMetaData>> aliases = new HashMap<>();
        aliases.put(SEARCH_INDEX_ALIAS_BDEF, aliasMetaDataSet);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
        when(indicesClient.getAlias(any(GetAliasesRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(getAliasesResponse);
        when(getAliasesResponse.getAliases()).thenReturn(aliases);
        when(restHighLevelClient.bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT))).thenThrow(new IOException());

        // Call the method under test
        indexFunctionsDao.createIndexDocuments(SEARCH_INDEX_NAME, documentMap);
    }

    @Test
    public void testDeleteDocumentByIdFunction() throws Exception
    {
        // Build mocks
        DeleteResponse deleteResponse = mock(DeleteResponse.class);
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        RestStatus restStatus = mock(RestStatus.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.delete(any(DeleteRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(deleteResponse);
        when(deleteResponse.status()).thenReturn(restStatus);

        // Call the method under test
        indexFunctionsDao.deleteDocumentById(SEARCH_INDEX_NAME, SEARCH_INDEX_DOCUMENT_ID);

        // Verify the calls to external methods
        verify(elasticsearchRestHighLevelClientFactory).getRestHighLevelClient();
        verify(restHighLevelClient).delete(any(DeleteRequest.class), eq(RequestOptions.DEFAULT));
        verify(deleteResponse).status();
        verify(restHighLevelClient).close();
        verifyNoMoreInteractions(elasticsearchRestHighLevelClientFactory, deleteResponse, restHighLevelClient);
    }

    @Test(expected = ElasticsearchRestClientException.class)
    public void testDeleteDocumentByIdFunctionThrowsElasticsearchRestClientException() throws Exception
    {
        // Build mocks
        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);

        // Mock the calls to external methods
        when(elasticsearchRestHighLevelClientFactory.getRestHighLevelClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.delete(any(DeleteRequest.class), eq(RequestOptions.DEFAULT))).thenThrow(new IOException());

        // Call the method under test
        indexFunctionsDao.deleteDocumentById(SEARCH_INDEX_NAME, SEARCH_INDEX_DOCUMENT_ID);
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
