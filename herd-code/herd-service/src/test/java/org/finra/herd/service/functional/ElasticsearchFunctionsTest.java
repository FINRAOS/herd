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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * ElasticsearchFunctionsTest
 */
public class ElasticsearchFunctionsTest
{
    @InjectMocks
    private ElasticsearchFunctions searchFunctions;

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

    // QuadPredicate<String, String, String, String>
    @Test
    public void testGetIsValidFunction()
    {
    }

    // Predicate<String>
    @Test
    public void testGetIndexExistsFunction()
    {
    }

    // Consumer<String>
    @Test
    public void testGetDeleteIndexFunction()
    {
    }

    // TriConsumer<String, String, String>
    @Test
    public void testGetCreateIndexFunction()
    {

    }

    // TriConsumer<String, String, String>
    @Test
    public void testGetDeleteDocumentByIdFunction()
    {

    }

    // BiFunction<String, String, Long>
    @Test
    public void getNumberOfTypesInIndexFunction()
    {

    }

    // BiFunction<String, String, List<String>>
    @Test
    public void getIdsInIndexFunction()
    {

    }
}
