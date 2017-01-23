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
package org.finra.herd.service.functional;

import static org.finra.herd.service.functional.ElasticsearchFunctions.ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME;
import static org.finra.herd.service.functional.ElasticsearchFunctions.ELASTIC_SEARCH_SCROLL_PAGE_SIZE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.TagEntity;

/**
 * ElasticsearchFunctionsTest
 */
public class ElasticsearchFunctionsTest
{
    @InjectMocks
    private ElasticsearchFunctions searchFunctions;

    @Mock
    private HerdStringHelper herdStringHelper;

    @Mock
    private JsonHelper jsonHelper;

    @Mock
    private TransportClient transportClient;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testIndexFunction()
    {
        QuadConsumer<String, String, String, String> indexFunction = searchFunctions.getIndexFunction();
        assertThat("Function is null.", indexFunction, not(nullValue()));
        assertThat("Index function not an instance of QuadConsumer.", indexFunction, instanceOf(QuadConsumer.class));

        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        @SuppressWarnings("unchecked")
        ListenableActionFuture<IndexResponse> listenableActionFuture = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareIndex("INDEX_NAME", "DOCUMENT_TYPE", "ID")).thenReturn(indexRequestBuilder);
        when(indexRequestBuilder.execute()).thenReturn(listenableActionFuture);

        // Call the method under test
        indexFunction.accept("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareIndex("INDEX_NAME", "DOCUMENT_TYPE", "ID");
        verify(indexRequestBuilder, times(1)).execute();
    }

    @Test
    public void testValidateFunctionIndex()
    {
        QuadConsumer<String, String, String, String> validateFunction = searchFunctions.getValidateFunction();
        assertThat("Function is null.", validateFunction, not(nullValue()));
        assertThat("Validate function not an instance of QuadConsumer.", validateFunction, instanceOf(QuadConsumer.class));

        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        GetResponse getResponse = mock(GetResponse.class);
        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);

        @SuppressWarnings("unchecked")
        ListenableActionFuture<GetResponse> listenableActionFutureGetResponse = mock(ListenableActionFuture.class);

        @SuppressWarnings("unchecked")
        ListenableActionFuture<IndexResponse> listenableActionFutureIndexResponse = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID")).thenReturn(getRequestBuilder);
        when(getRequestBuilder.execute()).thenReturn(listenableActionFutureGetResponse);
        when(listenableActionFutureGetResponse.actionGet()).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn(null);
        when(transportClient.prepareIndex("INDEX_NAME", "DOCUMENT_TYPE", "ID")).thenReturn(indexRequestBuilder);
        when(indexRequestBuilder.execute()).thenReturn(listenableActionFutureIndexResponse);

        // Call the method under test
        validateFunction.accept("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID");
        verify(getRequestBuilder, times(1)).execute();
        verify(listenableActionFutureGetResponse, times(1)).actionGet();
        verify(getResponse, times(1)).getSourceAsString();
        verify(transportClient, times(1)).prepareIndex("INDEX_NAME", "DOCUMENT_TYPE", "ID");
        verify(indexRequestBuilder, times(1)).execute();
    }

    @Test
    public void testValidateFunctionUpdate()
    {
        QuadConsumer<String, String, String, String> validateFunction = searchFunctions.getValidateFunction();
        assertThat("Function is null.", validateFunction, not(nullValue()));
        assertThat("Validate function not an instance of QuadConsumer.", validateFunction, instanceOf(QuadConsumer.class));

        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        GetResponse getResponse = mock(GetResponse.class);
        UpdateRequestBuilder updateRequestBuilder = mock(UpdateRequestBuilder.class);

        @SuppressWarnings("unchecked")
        ListenableActionFuture<GetResponse> listenableActionFutureGetResponse = mock(ListenableActionFuture.class);

        @SuppressWarnings("unchecked")
        ListenableActionFuture<UpdateResponse> listenableActionFutureUpdateResponse = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID")).thenReturn(getRequestBuilder);
        when(getRequestBuilder.execute()).thenReturn(listenableActionFutureGetResponse);
        when(listenableActionFutureGetResponse.actionGet()).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn("JSON_UPDATE");
        when(transportClient.prepareUpdate("INDEX_NAME", "DOCUMENT_TYPE", "ID")).thenReturn(updateRequestBuilder);
        when(updateRequestBuilder.execute()).thenReturn(listenableActionFutureUpdateResponse);

        // Call the method under test
        validateFunction.accept("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID");
        verify(getRequestBuilder, times(1)).execute();
        verify(listenableActionFutureGetResponse, times(1)).actionGet();
        verify(getResponse, times(1)).getSourceAsString();
        verify(transportClient, times(1)).prepareUpdate("INDEX_NAME", "DOCUMENT_TYPE", "ID");
        verify(updateRequestBuilder, times(1)).execute();
    }

    @Test
    public void testValidateFunctionNoActionRequiredValidDocument()
    {
        QuadConsumer<String, String, String, String> validateFunction = searchFunctions.getValidateFunction();
        assertThat("Function is null.", validateFunction, not(nullValue()));
        assertThat("Validate function not an instance of QuadConsumer.", validateFunction, instanceOf(QuadConsumer.class));

        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        GetResponse getResponse = mock(GetResponse.class);

        @SuppressWarnings("unchecked")
        ListenableActionFuture<GetResponse> listenableActionFutureGetResponse = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID")).thenReturn(getRequestBuilder);
        when(getRequestBuilder.execute()).thenReturn(listenableActionFutureGetResponse);
        when(listenableActionFutureGetResponse.actionGet()).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn("JSON");

        // Call the method under test
        validateFunction.accept("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID");
        verify(getRequestBuilder, times(1)).execute();
        verify(listenableActionFutureGetResponse, times(1)).actionGet();
        verify(getResponse, times(1)).getSourceAsString();
    }

    @Test
    public void testIsValidFunction()
    {
        QuadPredicate<String, String, String, String> isValidFunction = searchFunctions.getIsValidFunction();
        assertThat("Function is null.", isValidFunction, not(nullValue()));
        assertThat("Is valid function not an instance of QuadPredicate.", isValidFunction, instanceOf(QuadPredicate.class));

        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        GetResponse getResponse = mock(GetResponse.class);

        @SuppressWarnings("unchecked")
        ListenableActionFuture<GetResponse> listenableActionFutureGetResponse = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID")).thenReturn(getRequestBuilder);
        when(getRequestBuilder.execute()).thenReturn(listenableActionFutureGetResponse);
        when(listenableActionFutureGetResponse.actionGet()).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn("JSON");

        // Call the method under test
        boolean isValid = isValidFunction.test("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");

        assertThat("IsValid is false when it should have been true.", isValid, is(true));

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID");
        verify(getRequestBuilder, times(1)).execute();
        verify(listenableActionFutureGetResponse, times(1)).actionGet();
        verify(getResponse, times(1)).getSourceAsString();
    }

    @Test
    public void testIsValidFunctionEmpty()
    {
        QuadPredicate<String, String, String, String> isValidFunction = searchFunctions.getIsValidFunction();
        assertThat("Function is null.", isValidFunction, not(nullValue()));
        assertThat("Is valid function not an instance of QuadPredicate.", isValidFunction, instanceOf(QuadPredicate.class));

        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        GetResponse getResponse = mock(GetResponse.class);

        @SuppressWarnings("unchecked")
        ListenableActionFuture<GetResponse> listenableActionFutureGetResponse = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID")).thenReturn(getRequestBuilder);
        when(getRequestBuilder.execute()).thenReturn(listenableActionFutureGetResponse);
        when(listenableActionFutureGetResponse.actionGet()).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn("");

        // Call the method under test
        boolean isValid = isValidFunction.test("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");

        assertThat("IsValid is true when it should have been false.", isValid, is(false));

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID");
        verify(getRequestBuilder, times(1)).execute();
        verify(listenableActionFutureGetResponse, times(1)).actionGet();
        verify(getResponse, times(1)).getSourceAsString();
    }

    @Test
    public void testIsValidFunctionNull()
    {
        QuadPredicate<String, String, String, String> isValidFunction = searchFunctions.getIsValidFunction();
        assertThat("Function is null.", isValidFunction, not(nullValue()));
        assertThat("Is valid function not an instance of QuadPredicate.", isValidFunction, instanceOf(QuadPredicate.class));

        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        GetResponse getResponse = mock(GetResponse.class);

        @SuppressWarnings("unchecked")
        ListenableActionFuture<GetResponse> listenableActionFutureGetResponse = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID")).thenReturn(getRequestBuilder);
        when(getRequestBuilder.execute()).thenReturn(listenableActionFutureGetResponse);
        when(listenableActionFutureGetResponse.actionGet()).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn(null);

        // Call the method under test
        boolean isValid = isValidFunction.test("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");

        assertThat("IsValid is true when it should have been false.", isValid, is(false));

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID");
        verify(getRequestBuilder, times(1)).execute();
        verify(listenableActionFutureGetResponse, times(1)).actionGet();
        verify(getResponse, times(1)).getSourceAsString();
    }

    @Test
    public void testIsValidFunctionNotEqual()
    {
        QuadPredicate<String, String, String, String> isValidFunction = searchFunctions.getIsValidFunction();
        assertThat("Function is null.", isValidFunction, not(nullValue()));
        assertThat("Is valid function not an instance of QuadPredicate.", isValidFunction, instanceOf(QuadPredicate.class));

        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        GetResponse getResponse = mock(GetResponse.class);

        @SuppressWarnings("unchecked")
        ListenableActionFuture<GetResponse> listenableActionFutureGetResponse = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID")).thenReturn(getRequestBuilder);
        when(getRequestBuilder.execute()).thenReturn(listenableActionFutureGetResponse);
        when(listenableActionFutureGetResponse.actionGet()).thenReturn(getResponse);
        when(getResponse.getSourceAsString()).thenReturn("JSON_NOT_EQUAL");

        // Call the method under test
        boolean isValid = isValidFunction.test("INDEX_NAME", "DOCUMENT_TYPE", "ID", "JSON");

        assertThat("IsValid is true when it should have been false.", isValid, is(false));

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareGet("INDEX_NAME", "DOCUMENT_TYPE", "ID");
        verify(getRequestBuilder, times(1)).execute();
        verify(listenableActionFutureGetResponse, times(1)).actionGet();
        verify(getResponse, times(1)).getSourceAsString();
    }

    @Test
    public void testIndexExistsFunction()
    {
        Predicate<String> indexExistsFunction = searchFunctions.getIndexExistsFunction();
        assertThat("Function is null.", indexExistsFunction, not(nullValue()));
        assertThat("Index exists function not an instance of Predicate.", indexExistsFunction, instanceOf(Predicate.class));
    }

    @Test
    public void testDeleteIndexFunction()
    {
        Consumer<String> deleteIndexFunction = searchFunctions.getDeleteIndexFunction();
        assertThat("Function is null.", deleteIndexFunction, not(nullValue()));
        assertThat("Delete index function not an instance of Consumer.", deleteIndexFunction, instanceOf(Consumer.class));
    }

    @Test
    public void testCreateIndexFunction()
    {
        TriConsumer<String, String, String> createIndexFunction = searchFunctions.getCreateIndexFunction();
        assertThat("Function is null.", createIndexFunction, not(nullValue()));
        assertThat("Create index function not an instance of TriConsumer.", createIndexFunction, instanceOf(TriConsumer.class));

    }

    @Test
    public void testCreateIndexDocumentsFunction()
    {
        TriConsumer<String, String, Map<String, String>> createIndexDocumentsFunction = searchFunctions.getCreateIndexDocumentsFunction();
        assertThat("Function is null.", createIndexDocumentsFunction, not(nullValue()));
        assertThat("Create index documents function is not an instance of TriConsumer.", createIndexDocumentsFunction, instanceOf(TriConsumer.class));

        BulkRequestBuilder bulkRequestBuilder = mock(BulkRequestBuilder.class);
        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        BulkResponse bulkResponse = mock(BulkResponse.class);

        // Mock the call to external methods
        when(transportClient.prepareBulk()).thenReturn(bulkRequestBuilder);
        when(transportClient.prepareIndex("INDEX_NAME", "DOCUMENT_TYPE", "1")).thenReturn(indexRequestBuilder);
        when(bulkRequestBuilder.get()).thenReturn(bulkResponse);
        when(bulkResponse.hasFailures()).thenReturn(false);

        // Call the method under test
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put("1", "JSON");
        createIndexDocumentsFunction.accept("INDEX_NAME", "DOCUMENT_TYPE", documentMap);

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareBulk();
        verify(transportClient, times(1)).prepareIndex("INDEX_NAME", "DOCUMENT_TYPE", "1");
        verify(bulkRequestBuilder, times(1)).get();
        verify(bulkResponse, times(1)).hasFailures();
    }

    @Test
    public void testCreateIndexDocumentsFunctionWithFailures()
    {
        TriConsumer<String, String, Map<String, String>> createIndexDocumentsFunction = searchFunctions.getCreateIndexDocumentsFunction();
        assertThat("Function is null.", createIndexDocumentsFunction, not(nullValue()));
        assertThat("Create index documents function is not an instance of TriConsumer.", createIndexDocumentsFunction, instanceOf(TriConsumer.class));

        BulkRequestBuilder bulkRequestBuilder = mock(BulkRequestBuilder.class);
        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        BulkResponse bulkResponse = mock(BulkResponse.class);

        // Mock the call to external methods
        when(transportClient.prepareBulk()).thenReturn(bulkRequestBuilder);
        when(transportClient.prepareIndex("INDEX_NAME", "DOCUMENT_TYPE", "1")).thenReturn(indexRequestBuilder);
        when(bulkRequestBuilder.get()).thenReturn(bulkResponse);
        when(bulkResponse.hasFailures()).thenReturn(true);

        // Call the method under test
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put("1", "JSON");
        createIndexDocumentsFunction.accept("INDEX_NAME", "DOCUMENT_TYPE", documentMap);

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareBulk();
        verify(transportClient, times(1)).prepareIndex("INDEX_NAME", "DOCUMENT_TYPE", "1");
        verify(bulkRequestBuilder, times(1)).get();
        verify(bulkResponse, times(1)).hasFailures();
    }

    @Test
    public void testDeleteDocumentByIdFunction()
    {
        TriConsumer<String, String, String> deleteDocumentByIdFunction = searchFunctions.getDeleteDocumentByIdFunction();
        assertThat("Function is null.", deleteDocumentByIdFunction, not(nullValue()));
        assertThat("Delete document by id function not an instance of TriConsumer.", deleteDocumentByIdFunction, instanceOf(TriConsumer.class));

        DeleteRequestBuilder deleteRequestBuilder = mock(DeleteRequestBuilder.class);
        @SuppressWarnings("unchecked")
        ListenableActionFuture<DeleteResponse> listenableActionFuture = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareDelete("INDEX_NAME", "DOCUMENT_TYPE", "ID")).thenReturn(deleteRequestBuilder);
        when(deleteRequestBuilder.execute()).thenReturn(listenableActionFuture);

        // Call the method under test
        deleteDocumentByIdFunction.accept("INDEX_NAME", "DOCUMENT_TYPE", "ID");

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareDelete("INDEX_NAME", "DOCUMENT_TYPE", "ID");
        verify(deleteRequestBuilder, times(1)).execute();
    }

    @Test
    public void testDeleteIndexDocumentsFunction()
    {
        TriConsumer<String, String, List<Integer>> deleteIndexDocumentsFunction = searchFunctions.getDeleteIndexDocumentsFunction();
        assertThat("Function is null.", deleteIndexDocumentsFunction, not(nullValue()));
        assertThat("Delete index documents function is not an instance of TriConsumer.", deleteIndexDocumentsFunction, instanceOf(TriConsumer.class));

        BulkRequestBuilder bulkRequestBuilder = mock(BulkRequestBuilder.class);
        DeleteRequestBuilder deleteRequestBuilder = mock(DeleteRequestBuilder.class);
        BulkResponse bulkResponse = mock(BulkResponse.class);

        // Mock the call to external methods
        when(transportClient.prepareBulk()).thenReturn(bulkRequestBuilder);
        when(transportClient.prepareDelete("INDEX_NAME", "DOCUMENT_TYPE", "1")).thenReturn(deleteRequestBuilder);
        when(bulkRequestBuilder.get()).thenReturn(bulkResponse);
        when(bulkResponse.hasFailures()).thenReturn(false);

        // Call the method under test
        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.add(1);
        deleteIndexDocumentsFunction.accept("INDEX_NAME", "DOCUMENT_TYPE", businessObjectDefinitionIds);

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareBulk();
        verify(transportClient, times(1)).prepareDelete("INDEX_NAME", "DOCUMENT_TYPE", "1");
        verify(bulkRequestBuilder, times(1)).get();
        verify(bulkResponse, times(1)).hasFailures();
    }

    @Test
    public void testDeleteIndexDocumentsFunctionWithFailures()
    {
        TriConsumer<String, String, List<Integer>> deleteIndexDocumentsFunction = searchFunctions.getDeleteIndexDocumentsFunction();
        assertThat("Function is null.", deleteIndexDocumentsFunction, not(nullValue()));
        assertThat("Delete index documents function is not an instance of TriConsumer.", deleteIndexDocumentsFunction, instanceOf(TriConsumer.class));

        BulkRequestBuilder bulkRequestBuilder = mock(BulkRequestBuilder.class);
        DeleteRequestBuilder deleteRequestBuilder = mock(DeleteRequestBuilder.class);
        BulkResponse bulkResponse = mock(BulkResponse.class);

        // Mock the call to external methods
        when(transportClient.prepareBulk()).thenReturn(bulkRequestBuilder);
        when(transportClient.prepareDelete("INDEX_NAME", "DOCUMENT_TYPE", "1")).thenReturn(deleteRequestBuilder);
        when(bulkRequestBuilder.get()).thenReturn(bulkResponse);
        when(bulkResponse.hasFailures()).thenReturn(true);

        // Call the method under test
        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.add(1);
        deleteIndexDocumentsFunction.accept("INDEX_NAME", "DOCUMENT_TYPE", businessObjectDefinitionIds);

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareBulk();
        verify(transportClient, times(1)).prepareDelete("INDEX_NAME", "DOCUMENT_TYPE", "1");
        verify(bulkRequestBuilder, times(1)).get();
        verify(bulkResponse, times(1)).hasFailures();
    }

    @Test
    public void testNumberOfTypesInIndexFunction()
    {
        BiFunction<String, String, Long> numberOfTypesInIndexFunction = searchFunctions.getNumberOfTypesInIndexFunction();
        assertThat("Function is null.", numberOfTypesInIndexFunction, not(nullValue()));
        assertThat("Number of types in index function not an instance of BiFunction.", numberOfTypesInIndexFunction, instanceOf(BiFunction.class));

        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithTypes = mock(SearchRequestBuilder.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);

        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFuture = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareSearch("INDEX_NAME")).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setTypes("DOCUMENT_TYPE")).thenReturn(searchRequestBuilderWithTypes);
        when(searchRequestBuilderWithTypes.execute()).thenReturn(listenableActionFuture);
        when(listenableActionFuture.actionGet()).thenReturn(searchResponse);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getTotalHits()).thenReturn(100L);

        // Call the method under test
        long numberOfTypesInIndex = numberOfTypesInIndexFunction.apply("INDEX_NAME", "DOCUMENT_TYPE");

        assertThat("Number of types in index is incorrect.", numberOfTypesInIndex, is(100L));

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareSearch("INDEX_NAME");
        verify(searchRequestBuilder, times(1)).setTypes("DOCUMENT_TYPE");
        verify(searchRequestBuilderWithTypes, times(1)).execute();
        verify(listenableActionFuture, times(1)).actionGet();
        verify(searchResponse, times(1)).getHits();
        verify(searchHits, times(1)).getTotalHits();
    }

    @Test
    public void testIdsInIndexFunction()
    {
        BiFunction<String, String, List<String>> idsInIndexFunction = searchFunctions.getIdsInIndexFunction();
        assertThat("Function is null.", idsInIndexFunction, not(nullValue()));
        assertThat("Ids in index function not an instance of BiFunction.", idsInIndexFunction, instanceOf(BiFunction.class));

        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithTypes = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithQuery = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithScroll = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithSize = mock(SearchRequestBuilder.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);
        SearchHit searchHit1 = mock(SearchHit.class);
        SearchHit searchHit2 = mock(SearchHit.class);
        SearchScrollRequestBuilder searchScrollRequestBuilder = mock(SearchScrollRequestBuilder.class);
        SearchHit[] searchHitArray = new SearchHit[2];
        searchHitArray[0] = searchHit1;
        searchHitArray[1] = searchHit2;
        SearchResponse searchResponseScroll = mock(SearchResponse.class);
        SearchHits searchHitsScroll = mock(SearchHits.class);
        SearchHit[] searchHitArrayScroll = new SearchHit[0];

        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFutureScroll = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareSearch("INDEX_NAME")).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setTypes("DOCUMENT_TYPE")).thenReturn(searchRequestBuilderWithTypes);
        when(searchRequestBuilderWithTypes.setQuery(any())).thenReturn(searchRequestBuilderWithQuery);
        when(searchRequestBuilderWithQuery.setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME))).thenReturn(searchRequestBuilderWithScroll);
        when(searchRequestBuilderWithScroll.setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE)).thenReturn(searchRequestBuilderWithSize);
        when(searchRequestBuilder.execute()).thenReturn(listenableActionFuture);
        when(listenableActionFuture.actionGet()).thenReturn(searchResponse);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchHits.hits()).thenReturn(searchHitArray);
        when(transportClient.prepareSearchScroll(any())).thenReturn(searchScrollRequestBuilder);
        when(searchScrollRequestBuilder.execute()).thenReturn(listenableActionFutureScroll);
        when(listenableActionFutureScroll.actionGet()).thenReturn(searchResponseScroll);
        when(searchResponseScroll.getHits()).thenReturn(searchHitsScroll);
        when(searchHitsScroll.hits()).thenReturn(searchHitArrayScroll);

        // Call the method under test
        List<String> idsInIndex = idsInIndexFunction.apply("INDEX_NAME", "DOCUMENT_TYPE");

        assertThat("Ids in index list is null.", idsInIndex, not(nullValue()));

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareSearch("INDEX_NAME");
        verify(searchRequestBuilder, times(1)).setTypes("DOCUMENT_TYPE");
        verify(searchRequestBuilderWithTypes, times(1)).setQuery(any());
        verify(searchRequestBuilderWithQuery, times(1)).setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME));
        verify(searchRequestBuilderWithScroll, times(1)).setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE);
        verify(searchRequestBuilder, times(1)).execute();
        verify(listenableActionFuture, times(1)).actionGet();
        verify(searchResponse, times(1)).getHits();
        verify(searchHits, times(1)).hits();
        verify(transportClient, times(1)).prepareSearchScroll(any());
        verify(searchScrollRequestBuilder, times(1)).execute();
        verify(listenableActionFutureScroll, times(1)).actionGet();
        verify(searchResponseScroll, times(1)).getHits();
        verify(searchHitsScroll, times(1)).hits();
    }

    @Test
    public void testFindAllBusinessObjectDefinitionsFunction() throws Exception
    {
        BiFunction<String, String, List<BusinessObjectDefinitionEntity>> findAllBusinessObjectDefinitionsFunction =
            searchFunctions.getFindAllBusinessObjectDefinitionsFunction();
        assertThat("Function is null.", findAllBusinessObjectDefinitionsFunction, not(nullValue()));
        assertThat("Find all business object definitions function not an instance of BiFunction.", findAllBusinessObjectDefinitionsFunction,
            instanceOf(BiFunction.class));

        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithTypes = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithScroll = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithSize = mock(SearchRequestBuilder.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);
        SearchHit searchHit1 = mock(SearchHit.class);
        SearchHit searchHit2 = mock(SearchHit.class);
        SearchScrollRequestBuilder searchScrollRequestBuilder = mock(SearchScrollRequestBuilder.class);
        SearchHit[] searchHitArray = new SearchHit[2];
        searchHitArray[0] = searchHit1;
        searchHitArray[1] = searchHit2;
        SearchResponse searchResponseScroll = mock(SearchResponse.class);
        SearchHits searchHitsScroll = mock(SearchHits.class);
        SearchHit[] searchHitArrayScroll = new SearchHit[0];

        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFutureScroll = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareSearch("INDEX_NAME")).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setTypes("DOCUMENT_TYPE")).thenReturn(searchRequestBuilderWithTypes);
        when(searchRequestBuilderWithTypes.setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME))).thenReturn(searchRequestBuilderWithScroll);
        when(searchRequestBuilderWithScroll.setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE)).thenReturn(searchRequestBuilderWithSize);
        when(searchRequestBuilder.execute()).thenReturn(listenableActionFuture);
        when(listenableActionFuture.actionGet()).thenReturn(searchResponse);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchHits.hits()).thenReturn(searchHitArray);
        when(transportClient.prepareSearchScroll(any())).thenReturn(searchScrollRequestBuilder);
        when(searchScrollRequestBuilder.execute()).thenReturn(listenableActionFutureScroll);
        when(listenableActionFutureScroll.actionGet()).thenReturn(searchResponseScroll);
        when(searchResponseScroll.getHits()).thenReturn(searchHitsScroll);
        when(searchHitsScroll.hits()).thenReturn(searchHitArrayScroll);
        when(jsonHelper.unmarshallJsonToObject(any(), any())).thenReturn(new BusinessObjectDefinitionEntity());

        // Call the method under test
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = findAllBusinessObjectDefinitionsFunction.apply("INDEX_NAME", "DOCUMENT_TYPE");

        assertThat("Business object definition entity list is null.", businessObjectDefinitionEntityList, not(nullValue()));

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareSearch("INDEX_NAME");
        verify(searchRequestBuilder, times(1)).setTypes("DOCUMENT_TYPE");
        verify(searchRequestBuilderWithTypes, times(1)).setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME));
        verify(searchRequestBuilderWithScroll, times(1)).setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE);
        verify(searchRequestBuilder, times(1)).execute();
        verify(listenableActionFuture, times(1)).actionGet();
        verify(searchResponse, times(1)).getHits();
        verify(searchHits, times(1)).hits();
        verify(transportClient, times(1)).prepareSearchScroll(any());
        verify(searchScrollRequestBuilder, times(1)).execute();
        verify(listenableActionFutureScroll, times(1)).actionGet();
        verify(searchResponseScroll, times(1)).getHits();
        verify(searchHitsScroll, times(1)).hits();
        verify(jsonHelper, times(2)).unmarshallJsonToObject(any(), any());
    }

    @Test
    public void testFindAllBusinessObjectDefinitionsFunctionException() throws Exception
    {
        BiFunction<String, String, List<BusinessObjectDefinitionEntity>> findAllBusinessObjectDefinitionsFunction =
            searchFunctions.getFindAllBusinessObjectDefinitionsFunction();
        assertThat("Function is null.", findAllBusinessObjectDefinitionsFunction, not(nullValue()));
        assertThat("Find all business object definitions function not an instance of BiFunction.", findAllBusinessObjectDefinitionsFunction,
            instanceOf(BiFunction.class));

        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithTypes = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithScroll = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithSize = mock(SearchRequestBuilder.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);
        SearchHit searchHit1 = mock(SearchHit.class);
        SearchHit searchHit2 = mock(SearchHit.class);
        SearchScrollRequestBuilder searchScrollRequestBuilder = mock(SearchScrollRequestBuilder.class);
        SearchHit[] searchHitArray = new SearchHit[2];
        searchHitArray[0] = searchHit1;
        searchHitArray[1] = searchHit2;
        SearchResponse searchResponseScroll = mock(SearchResponse.class);
        SearchHits searchHitsScroll = mock(SearchHits.class);
        SearchHit[] searchHitArrayScroll = new SearchHit[0];

        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFutureScroll = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareSearch("INDEX_NAME")).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setTypes("DOCUMENT_TYPE")).thenReturn(searchRequestBuilderWithTypes);
        when(searchRequestBuilderWithTypes.setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME))).thenReturn(searchRequestBuilderWithScroll);
        when(searchRequestBuilderWithScroll.setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE)).thenReturn(searchRequestBuilderWithSize);
        when(searchRequestBuilder.execute()).thenReturn(listenableActionFuture);
        when(listenableActionFuture.actionGet()).thenReturn(searchResponse);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchHits.hits()).thenReturn(searchHitArray);
        when(transportClient.prepareSearchScroll(any())).thenReturn(searchScrollRequestBuilder);
        when(searchScrollRequestBuilder.execute()).thenReturn(listenableActionFutureScroll);
        when(listenableActionFutureScroll.actionGet()).thenReturn(searchResponseScroll);
        when(searchResponseScroll.getHits()).thenReturn(searchHitsScroll);
        when(searchHitsScroll.hits()).thenReturn(searchHitArrayScroll);
        when(jsonHelper.unmarshallJsonToObject(any(), any())).thenThrow(new IOException());

        // Call the method under test
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = findAllBusinessObjectDefinitionsFunction.apply("INDEX_NAME", "DOCUMENT_TYPE");

        assertThat("Business object definition entity list is null.", businessObjectDefinitionEntityList, not(nullValue()));

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareSearch("INDEX_NAME");
        verify(searchRequestBuilder, times(1)).setTypes("DOCUMENT_TYPE");
        verify(searchRequestBuilderWithTypes, times(1)).setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME));
        verify(searchRequestBuilderWithScroll, times(1)).setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE);
        verify(searchRequestBuilder, times(1)).execute();
        verify(listenableActionFuture, times(1)).actionGet();
        verify(searchResponse, times(1)).getHits();
        verify(searchHits, times(1)).hits();
        verify(transportClient, times(1)).prepareSearchScroll(any());
        verify(searchScrollRequestBuilder, times(1)).execute();
        verify(listenableActionFutureScroll, times(1)).actionGet();
        verify(searchResponseScroll, times(1)).getHits();
        verify(searchHitsScroll, times(1)).hits();
        verify(jsonHelper, times(2)).unmarshallJsonToObject(any(), any());
    }

    @Test
    public void testSearchBusinessObjectDefinitionsByTagCodeAndTagTypeCodeFunction() throws Exception
    {
        QuadFunction<String, String, String, String, List<BusinessObjectDefinitionEntity>> searchBusinessObjectDefinitionsByTagCodeAndTagTypeFunction =
            searchFunctions.getSearchBusinessObjectDefinitionsByTagCodeAndTagTypeFunction();
        assertThat("Function is null.", searchBusinessObjectDefinitionsByTagCodeAndTagTypeFunction, not(nullValue()));
        assertThat("Search business object definitions by tag code and tag type code function not an instance of QuadFunction.",
            searchBusinessObjectDefinitionsByTagCodeAndTagTypeFunction, instanceOf(QuadFunction.class));

        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithTypes = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithQuery = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithScroll = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithSize = mock(SearchRequestBuilder.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);
        SearchHit searchHit1 = mock(SearchHit.class);
        SearchHit searchHit2 = mock(SearchHit.class);
        SearchScrollRequestBuilder searchScrollRequestBuilder = mock(SearchScrollRequestBuilder.class);
        SearchHit[] searchHitArray = new SearchHit[2];
        searchHitArray[0] = searchHit1;
        searchHitArray[1] = searchHit2;
        SearchResponse searchResponseScroll = mock(SearchResponse.class);
        SearchHits searchHitsScroll = mock(SearchHits.class);
        SearchHit[] searchHitArrayScroll = new SearchHit[0];

        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFutureScroll = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareSearch("INDEX_NAME")).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setTypes("DOCUMENT_TYPE")).thenReturn(searchRequestBuilderWithTypes);
        when(searchRequestBuilderWithTypes.setQuery(any())).thenReturn(searchRequestBuilderWithQuery);
        when(searchRequestBuilderWithQuery.setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME))).thenReturn(searchRequestBuilderWithScroll);
        when(searchRequestBuilderWithScroll.setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE)).thenReturn(searchRequestBuilderWithSize);
        when(searchRequestBuilder.execute()).thenReturn(listenableActionFuture);
        when(listenableActionFuture.actionGet()).thenReturn(searchResponse);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchHits.hits()).thenReturn(searchHitArray);
        when(transportClient.prepareSearchScroll(any())).thenReturn(searchScrollRequestBuilder);
        when(searchScrollRequestBuilder.execute()).thenReturn(listenableActionFutureScroll);
        when(listenableActionFutureScroll.actionGet()).thenReturn(searchResponseScroll);
        when(searchResponseScroll.getHits()).thenReturn(searchHitsScroll);
        when(searchHitsScroll.hits()).thenReturn(searchHitArrayScroll);
        when(jsonHelper.unmarshallJsonToObject(any(), any())).thenReturn(new BusinessObjectDefinitionEntity());

        // Call the method under test
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList =
            searchBusinessObjectDefinitionsByTagCodeAndTagTypeFunction.apply("INDEX_NAME", "DOCUMENT_TYPE", "TAG_CODE", "TAG_TYPE_CODE");

        assertThat("Business object definition entity list is null.", businessObjectDefinitionEntityList, not(nullValue()));

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareSearch("INDEX_NAME");
        verify(searchRequestBuilder, times(1)).setTypes("DOCUMENT_TYPE");
        verify(searchRequestBuilderWithTypes, times(1)).setQuery(any());
        verify(searchRequestBuilderWithQuery, times(1)).setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME));
        verify(searchRequestBuilderWithScroll, times(1)).setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE);
        verify(searchRequestBuilder, times(1)).execute();
        verify(listenableActionFuture, times(1)).actionGet();
        verify(searchResponse, times(1)).getHits();
        verify(searchHits, times(1)).hits();
        verify(transportClient, times(1)).prepareSearchScroll(any());
        verify(searchScrollRequestBuilder, times(1)).execute();
        verify(listenableActionFutureScroll, times(1)).actionGet();
        verify(searchResponseScroll, times(1)).getHits();
        verify(searchHitsScroll, times(1)).hits();
        verify(jsonHelper, times(2)).unmarshallJsonToObject(any(), any());
    }

    @Test
    public void testSearchBusinessObjectDefinitionsByTagCodeAndTagTypeCodeFunctionException() throws Exception
    {
        QuadFunction<String, String, String, String, List<BusinessObjectDefinitionEntity>> searchBusinessObjectDefinitionsByTagCodeAndTagTypeFunction =
            searchFunctions.getSearchBusinessObjectDefinitionsByTagCodeAndTagTypeFunction();
        assertThat("Function is null.", searchBusinessObjectDefinitionsByTagCodeAndTagTypeFunction, not(nullValue()));
        assertThat("Search business object definitions by tag code and tag type code function not an instance of QuadFunction.",
            searchBusinessObjectDefinitionsByTagCodeAndTagTypeFunction, instanceOf(QuadFunction.class));

        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithTypes = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithQuery = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithScroll = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithSize = mock(SearchRequestBuilder.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);
        SearchHit searchHit1 = mock(SearchHit.class);
        SearchHit searchHit2 = mock(SearchHit.class);
        SearchScrollRequestBuilder searchScrollRequestBuilder = mock(SearchScrollRequestBuilder.class);
        SearchHit[] searchHitArray = new SearchHit[2];
        searchHitArray[0] = searchHit1;
        searchHitArray[1] = searchHit2;
        SearchResponse searchResponseScroll = mock(SearchResponse.class);
        SearchHits searchHitsScroll = mock(SearchHits.class);
        SearchHit[] searchHitArrayScroll = new SearchHit[0];

        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFutureScroll = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareSearch("INDEX_NAME")).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setTypes("DOCUMENT_TYPE")).thenReturn(searchRequestBuilderWithTypes);
        when(searchRequestBuilderWithTypes.setQuery(any())).thenReturn(searchRequestBuilderWithQuery);
        when(searchRequestBuilderWithQuery.setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME))).thenReturn(searchRequestBuilderWithScroll);
        when(searchRequestBuilderWithScroll.setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE)).thenReturn(searchRequestBuilderWithSize);
        when(searchRequestBuilder.execute()).thenReturn(listenableActionFuture);
        when(listenableActionFuture.actionGet()).thenReturn(searchResponse);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchHits.hits()).thenReturn(searchHitArray);
        when(transportClient.prepareSearchScroll(any())).thenReturn(searchScrollRequestBuilder);
        when(searchScrollRequestBuilder.execute()).thenReturn(listenableActionFutureScroll);
        when(listenableActionFutureScroll.actionGet()).thenReturn(searchResponseScroll);
        when(searchResponseScroll.getHits()).thenReturn(searchHitsScroll);
        when(searchHitsScroll.hits()).thenReturn(searchHitArrayScroll);
        when(jsonHelper.unmarshallJsonToObject(any(), any())).thenThrow(new IOException());

        // Call the method under test
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList =
            searchBusinessObjectDefinitionsByTagCodeAndTagTypeFunction.apply("INDEX_NAME", "DOCUMENT_TYPE", "TAG_CODE", "TAG_TYPE_CODE");

        assertThat("Business object definition entity list is null.", businessObjectDefinitionEntityList, not(nullValue()));

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareSearch("INDEX_NAME");
        verify(searchRequestBuilder, times(1)).setTypes("DOCUMENT_TYPE");
        verify(searchRequestBuilderWithTypes, times(1)).setQuery(any());
        verify(searchRequestBuilderWithQuery, times(1)).setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME));
        verify(searchRequestBuilderWithScroll, times(1)).setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE);
        verify(searchRequestBuilder, times(1)).execute();
        verify(listenableActionFuture, times(1)).actionGet();
        verify(searchResponse, times(1)).getHits();
        verify(searchHits, times(1)).hits();
        verify(transportClient, times(1)).prepareSearchScroll(any());
        verify(searchScrollRequestBuilder, times(1)).execute();
        verify(listenableActionFutureScroll, times(1)).actionGet();
        verify(searchResponseScroll, times(1)).getHits();
        verify(searchHitsScroll, times(1)).hits();
        verify(jsonHelper, times(2)).unmarshallJsonToObject(any(), any());
    }

    @Test
    public void testSearchBusinessObjectDefinitionsByTagsFunction() throws Exception
    {
        TriFunction<String, String, List<TagEntity>, List<BusinessObjectDefinitionEntity>> searchBusinessObjectDefinitionsByTagsFunction =
            searchFunctions.getSearchBusinessObjectDefinitionsByTagsFunction();
        assertThat("Function is null.", searchBusinessObjectDefinitionsByTagsFunction, not(nullValue()));
        assertThat("Search business object definitions by tags function not an instance of TriFunction.", searchBusinessObjectDefinitionsByTagsFunction,
            instanceOf(TriFunction.class));

        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithTypes = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithQuery = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithScroll = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithSize = mock(SearchRequestBuilder.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);
        SearchHit searchHit1 = mock(SearchHit.class);
        SearchHit searchHit2 = mock(SearchHit.class);
        SearchScrollRequestBuilder searchScrollRequestBuilder = mock(SearchScrollRequestBuilder.class);
        SearchHit[] searchHitArray = new SearchHit[2];
        searchHitArray[0] = searchHit1;
        searchHitArray[1] = searchHit2;
        SearchResponse searchResponseScroll = mock(SearchResponse.class);
        SearchHits searchHitsScroll = mock(SearchHits.class);
        SearchHit[] searchHitArrayScroll = new SearchHit[0];

        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFutureScroll = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareSearch("INDEX_NAME")).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setTypes("DOCUMENT_TYPE")).thenReturn(searchRequestBuilderWithTypes);
        when(searchRequestBuilderWithTypes.setQuery(any())).thenReturn(searchRequestBuilderWithQuery);
        when(searchRequestBuilderWithQuery.setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME))).thenReturn(searchRequestBuilderWithScroll);
        when(searchRequestBuilderWithScroll.setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE)).thenReturn(searchRequestBuilderWithSize);
        when(searchRequestBuilder.execute()).thenReturn(listenableActionFuture);
        when(listenableActionFuture.actionGet()).thenReturn(searchResponse);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchHits.hits()).thenReturn(searchHitArray);
        when(transportClient.prepareSearchScroll(any())).thenReturn(searchScrollRequestBuilder);
        when(searchScrollRequestBuilder.execute()).thenReturn(listenableActionFutureScroll);
        when(listenableActionFutureScroll.actionGet()).thenReturn(searchResponseScroll);
        when(searchResponseScroll.getHits()).thenReturn(searchHitsScroll);
        when(searchHitsScroll.hits()).thenReturn(searchHitArrayScroll);
        when(herdStringHelper.join(new ArrayList<>(), ",", "\\")).thenReturn("JOINED_STRING");
        when(jsonHelper.unmarshallJsonToObject(any(), any())).thenReturn(new BusinessObjectDefinitionEntity());

        // Call the method under test
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList =
            searchBusinessObjectDefinitionsByTagsFunction.apply("INDEX_NAME", "DOCUMENT_TYPE", new ArrayList<>());

        assertThat("Business object definition entity list is null.", businessObjectDefinitionEntityList, not(nullValue()));

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareSearch("INDEX_NAME");
        verify(searchRequestBuilder, times(1)).setTypes("DOCUMENT_TYPE");
        verify(searchRequestBuilderWithTypes, times(1)).setQuery(any());
        verify(searchRequestBuilderWithQuery, times(1)).setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME));
        verify(searchRequestBuilderWithScroll, times(1)).setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE);
        verify(searchRequestBuilder, times(1)).execute();
        verify(listenableActionFuture, times(1)).actionGet();
        verify(searchResponse, times(1)).getHits();
        verify(searchHits, times(1)).hits();
        verify(transportClient, times(1)).prepareSearchScroll(any());
        verify(searchScrollRequestBuilder, times(1)).execute();
        verify(listenableActionFutureScroll, times(1)).actionGet();
        verify(searchResponseScroll, times(1)).getHits();
        verify(searchHitsScroll, times(1)).hits();
        verify(herdStringHelper, times(1)).join(new ArrayList<>(), ",", "\\");
        verify(jsonHelper, times(2)).unmarshallJsonToObject(any(), any());
    }

    @Test
    public void testSearchBusinessObjectDefinitionsByTagsFunctionException() throws Exception
    {
        TriFunction<String, String, List<TagEntity>, List<BusinessObjectDefinitionEntity>> searchBusinessObjectDefinitionsByTagsFunction =
            searchFunctions.getSearchBusinessObjectDefinitionsByTagsFunction();
        assertThat("Function is null.", searchBusinessObjectDefinitionsByTagsFunction, not(nullValue()));
        assertThat("Search business object definitions by tags function not an instance of TriFunction.", searchBusinessObjectDefinitionsByTagsFunction,
            instanceOf(TriFunction.class));

        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithTypes = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithQuery = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithScroll = mock(SearchRequestBuilder.class);
        SearchRequestBuilder searchRequestBuilderWithSize = mock(SearchRequestBuilder.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);
        SearchHit searchHit1 = mock(SearchHit.class);
        SearchHit searchHit2 = mock(SearchHit.class);
        SearchScrollRequestBuilder searchScrollRequestBuilder = mock(SearchScrollRequestBuilder.class);
        SearchHit[] searchHitArray = new SearchHit[2];
        searchHitArray[0] = searchHit1;
        searchHitArray[1] = searchHit2;
        SearchResponse searchResponseScroll = mock(SearchResponse.class);
        SearchHits searchHitsScroll = mock(SearchHits.class);
        SearchHit[] searchHitArrayScroll = new SearchHit[0];

        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        @SuppressWarnings("unchecked")
        ListenableActionFuture<SearchResponse> listenableActionFutureScroll = mock(ListenableActionFuture.class);

        // Mock the call to external methods
        when(transportClient.prepareSearch("INDEX_NAME")).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setTypes("DOCUMENT_TYPE")).thenReturn(searchRequestBuilderWithTypes);
        when(searchRequestBuilderWithTypes.setQuery(any())).thenReturn(searchRequestBuilderWithQuery);
        when(searchRequestBuilderWithQuery.setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME))).thenReturn(searchRequestBuilderWithScroll);
        when(searchRequestBuilderWithScroll.setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE)).thenReturn(searchRequestBuilderWithSize);
        when(searchRequestBuilder.execute()).thenReturn(listenableActionFuture);
        when(listenableActionFuture.actionGet()).thenReturn(searchResponse);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchHits.hits()).thenReturn(searchHitArray);
        when(transportClient.prepareSearchScroll(any())).thenReturn(searchScrollRequestBuilder);
        when(searchScrollRequestBuilder.execute()).thenReturn(listenableActionFutureScroll);
        when(listenableActionFutureScroll.actionGet()).thenReturn(searchResponseScroll);
        when(searchResponseScroll.getHits()).thenReturn(searchHitsScroll);
        when(searchHitsScroll.hits()).thenReturn(searchHitArrayScroll);
        when(herdStringHelper.join(new ArrayList<>(), ",", "\\")).thenReturn("JOINED_STRING");
        when(jsonHelper.unmarshallJsonToObject(any(), any())).thenThrow(new IOException());

        // Call the method under test
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList =
            searchBusinessObjectDefinitionsByTagsFunction.apply("INDEX_NAME", "DOCUMENT_TYPE", new ArrayList<>());

        assertThat("Business object definition entity list is null.", businessObjectDefinitionEntityList, not(nullValue()));

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareSearch("INDEX_NAME");
        verify(searchRequestBuilder, times(1)).setTypes("DOCUMENT_TYPE");
        verify(searchRequestBuilderWithTypes, times(1)).setQuery(any());
        verify(searchRequestBuilderWithQuery, times(1)).setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME));
        verify(searchRequestBuilderWithScroll, times(1)).setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE);
        verify(searchRequestBuilder, times(1)).execute();
        verify(listenableActionFuture, times(1)).actionGet();
        verify(searchResponse, times(1)).getHits();
        verify(searchHits, times(1)).hits();
        verify(transportClient, times(1)).prepareSearchScroll(any());
        verify(searchScrollRequestBuilder, times(1)).execute();
        verify(listenableActionFutureScroll, times(1)).actionGet();
        verify(searchResponseScroll, times(1)).getHits();
        verify(searchHitsScroll, times(1)).hits();
        verify(herdStringHelper, times(1)).join(new ArrayList<>(), ",", "\\");
        verify(jsonHelper, times(2)).unmarshallJsonToObject(any(), any());
    }

    @Test
    public void testUpdateIndexDocumentsFunction()
    {
        TriConsumer<String, String, Map<String, String>> updateIndexDocumentsFunction = searchFunctions.getUpdateIndexDocumentsFunction();
        assertThat("Function is null.", updateIndexDocumentsFunction, not(nullValue()));
        assertThat("Create index documents function is not an instance of TriConsumer.", updateIndexDocumentsFunction, instanceOf(TriConsumer.class));

        BulkRequestBuilder bulkRequestBuilder = mock(BulkRequestBuilder.class);
        UpdateRequestBuilder updateRequestBuilder = mock(UpdateRequestBuilder.class);
        BulkResponse bulkResponse = mock(BulkResponse.class);

        // Mock the call to external methods
        when(transportClient.prepareBulk()).thenReturn(bulkRequestBuilder);
        when(transportClient.prepareUpdate("INDEX_NAME", "DOCUMENT_TYPE", "1")).thenReturn(updateRequestBuilder);
        when(bulkRequestBuilder.get()).thenReturn(bulkResponse);
        when(bulkResponse.hasFailures()).thenReturn(false);

        // Call the method under test
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put("1", "JSON");
        updateIndexDocumentsFunction.accept("INDEX_NAME", "DOCUMENT_TYPE", documentMap);

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareBulk();
        verify(transportClient, times(1)).prepareUpdate("INDEX_NAME", "DOCUMENT_TYPE", "1");
        verify(bulkRequestBuilder, times(1)).get();
        verify(bulkResponse, times(1)).hasFailures();
    }

    @Test
    public void testUpdateIndexDocumentsFunctionWithFailures()
    {
        TriConsumer<String, String, Map<String, String>> updateIndexDocumentsFunction = searchFunctions.getUpdateIndexDocumentsFunction();
        assertThat("Function is null.", updateIndexDocumentsFunction, not(nullValue()));
        assertThat("Create index documents function is not an instance of TriConsumer.", updateIndexDocumentsFunction, instanceOf(TriConsumer.class));

        BulkRequestBuilder bulkRequestBuilder = mock(BulkRequestBuilder.class);
        UpdateRequestBuilder updateRequestBuilder = mock(UpdateRequestBuilder.class);
        BulkResponse bulkResponse = mock(BulkResponse.class);

        // Mock the call to external methods
        when(transportClient.prepareBulk()).thenReturn(bulkRequestBuilder);
        when(transportClient.prepareUpdate("INDEX_NAME", "DOCUMENT_TYPE", "1")).thenReturn(updateRequestBuilder);
        when(bulkRequestBuilder.get()).thenReturn(bulkResponse);
        when(bulkResponse.hasFailures()).thenReturn(true);

        // Call the method under test
        Map<String, String> documentMap = new HashMap<>();
        documentMap.put("1", "JSON");
        updateIndexDocumentsFunction.accept("INDEX_NAME", "DOCUMENT_TYPE", documentMap);

        // Verify the calls to external methods
        verify(transportClient, times(1)).prepareBulk();
        verify(transportClient, times(1)).prepareUpdate("INDEX_NAME", "DOCUMENT_TYPE", "1");
        verify(bulkRequestBuilder, times(1)).get();
        verify(bulkResponse, times(1)).hasFailures();
    }
}
