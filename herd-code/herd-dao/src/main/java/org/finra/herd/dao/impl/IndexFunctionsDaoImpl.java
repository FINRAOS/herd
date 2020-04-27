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
package org.finra.herd.dao.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.ElasticsearchRestHighLevelClientFactory;
import org.finra.herd.dao.IndexFunctionsDao;
import org.finra.herd.dao.exception.ElasticsearchRestClientException;

@Repository
public class IndexFunctionsDaoImpl extends AbstractHerdDao implements IndexFunctionsDao
{
    /**
     * The logger used to write messages to the log
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexFunctionsDaoImpl.class);

    /**
     * Scroll keep alive in milliseconds
     */
    private static final int ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME = 60000;

    /**
     * Elasticsearch rest client
     */
    @Autowired
    private ElasticsearchRestHighLevelClientFactory elasticsearchRestHighLevelClientFactory;


    @Override
    public final void createIndexDocument(final String indexName, final String id, final String json)
    {
        LOGGER.info("Creating Index Document, indexName={}, id={}.", indexName, id);

        // Build the index request.
        IndexRequest indexRequest = new IndexRequest(indexName).id(id).source(json, XContentType.JSON);

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            // Make the create index document request.
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

            LOGGER.info("Created Index Document, indexName={}, id={}, status={}.", indexName, id, indexResponse.status().getStatus());
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }
    }

    @Override
    public boolean isValidDocumentIndex(final String indexName, final String id, final String json)
    {
        LOGGER.info("Validating Index Document, indexName={}, id={}.", indexName, id);

        // Build the get request.
        GetRequest getRequest = new GetRequest(indexName, id);

        // Create the get response object.
        GetResponse getResponse;

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            // Make the get request.
            getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }

        // Retrieve the JSON string from the get response
        final String jsonStringFromIndex = getResponse.getSourceAsString();

        // Return true if the json from the index is not null or empty and the json from the index matches the object from the database
        return StringUtils.isNotEmpty(jsonStringFromIndex) && jsonStringFromIndex.equals(json);
    }

    @Override
    public final boolean isIndexExists(final String indexName)
    {
        LOGGER.info("Checking index existence, indexName={}.", indexName);

        // Build the get index request.
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);

        // Create the index exists boolean flag.
        boolean isIndexExists;

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            // Make the get index request.
            isIndexExists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }

        return isIndexExists;
    }

    @Override
    public final void deleteIndex(final String indexName)
    {
        LOGGER.info("Deleting Elasticsearch index, indexName={}.", indexName);

        // Build the delete index request
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            // Call the Elasticsearch REST client to delete the index and receive the response.
            AcknowledgedResponse deleteIndexResponse = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);

            LOGGER.info("Deleted Elasticsearch index, indexName={}, isAcknowledge={}.", indexName, deleteIndexResponse.isAcknowledged());
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }
    }

    @Override
    public final void validateDocumentIndex(final String indexName, final String id, final String json)
    {
        LOGGER.info("Validating Elasticsearch document, indexName={}, id={}.", indexName, id);

        // Build the get request.
        GetRequest getRequest = new GetRequest(indexName, id);

        // Create the get response object.
        GetResponse getResponse;

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            // Make the get request.
            getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }

        // Retrieve the JSON string from the get response
        final String jsonStringFromIndex = getResponse.getSourceAsString();

        // If the document does not exist in the index add the document to the index
        if (StringUtils.isEmpty(jsonStringFromIndex))
        {
            LOGGER.warn("Document does not exist in the index, adding the document to the index.");

            // Create the index document.
            createIndexDocument(indexName, id, json);
        }
        // Else if the JSON does not match the JSON from the index update the index
        else if (!json.equals(jsonStringFromIndex))
        {
            LOGGER.warn("Document does not match the document in the index, updating the document in the index.");

            // Note that create index will update the document if it exists.
            createIndexDocument(indexName, id, json);
        }
    }

    @Override
    public void createIndexDocuments(final String indexName, final Map<String, String> documentMap)
    {
        LOGGER.info("Creating Elasticsearch index documents, indexName={}.", indexName);

        List<String> allIndices = getAliases(indexName);

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            for (String index : allIndices)
            {
                // Prepare a bulk request
                BulkRequest bulkRequest = new BulkRequest();

                // For each document prepare an insert request and add it to the bulk request
                documentMap.forEach((id, jsonString) -> bulkRequest.add(new IndexRequest(index).id(id).source(jsonString, XContentType.JSON)));

                // Make the bulk request.
                BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                // If there are failures log them
                if (!bulkResponse.hasFailures())
                {
                    LOGGER.error("Bulk response error={}.", bulkResponse.buildFailureMessage());
                }
            }
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }
    }

    @Override
    public final void createIndex(final String indexName, final String mapping, final String settings, final String alias)
    {
        LOGGER.info("Creating Elasticsearch index, indexName={}.", indexName);

        // Build the create index request.
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.settings(settings, XContentType.JSON);
        createIndexRequest.mapping(mapping, XContentType.JSON);
        createIndexRequest.alias(new Alias(alias));

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            // Make the create index request.
            CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

            LOGGER.info("Created Elasticsearch index, indexName={}, isAcknowledge={}.", indexName, createIndexResponse.isAcknowledged());
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }
    }

    @Override
    public final void deleteDocumentById(final String indexName, final String id)
    {
        LOGGER.info("Deleting Elasticsearch document from index, indexName={}, id={}.", indexName, id);

        // Build the delete request.
        DeleteRequest deleteRequest = new DeleteRequest(indexName, id);

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            // Make the delete request.
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);

            LOGGER.info("Deleted Elasticsearch document from index, indexName={}, id={}, status={}.", indexName, id, deleteResponse.status());
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }
    }

    @Override
    public final void deleteIndexDocuments(final String indexName, final List<Long> ids)
    {
        LOGGER.info("Deleting Elasticsearch documents from index, indexName={}, ids={}.", indexName,
            ids.stream().map(Object::toString).collect(Collectors.joining(",")));

        List<String> allIndices = getAliases(indexName);

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            // For each index in the list of indices.
            for (String index : allIndices)
            {
                // Prepare a bulk request
                BulkRequest bulkRequest = new BulkRequest();

                // For each document prepare an insert request and add it to the bulk request
                ids.forEach(id -> bulkRequest.add(new DeleteRequest(index, id.toString())));

                BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                // If there are failures log them
                if (!bulkResponse.hasFailures())
                {
                    LOGGER.error("Bulk response error={}.", bulkResponse.buildFailureMessage());
                }
            }
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }
    }

    @Override
    public long getNumberOfTypesInIndex(final String indexName)
    {
        LOGGER.info("Counting the number of documents in index={}.", indexName);

        // Build the count request.
        CountRequest countRequest = new CountRequest(indexName);
        countRequest.query(QueryBuilders.matchAllQuery());

        // Create the count response object.
        CountResponse countResponse;

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            // Make the count request.
            countResponse = restHighLevelClient.count(countRequest, RequestOptions.DEFAULT);
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }

        return countResponse.getCount();
    }

    @Override
    public final List<String> getIdsInIndex(final String indexName)
    {
        LOGGER.info("Get the ids for the documents in index={}.", indexName);

        // Create an array list for storing the ids
        List<String> idList = new ArrayList<>();

        // Build a scroll with a time out value.
        final Scroll scroll = new Scroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME));

        // Build the search request.
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            // Make the search request.
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            // While there are hits available, scroll through the results and add them to the id list
            while (searchHits != null && searchHits.length > 0)
            {
                // Add each search hit to the id list.
                for (SearchHit searchHit : searchResponse.getHits().getHits())
                {
                    idList.add(searchHit.getId());
                }

                // Build a search scroll request.
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);

                // Make the search scroll request.
                searchResponse = restHighLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);

                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }

            // Build a clear scroll request.
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);

            // Make the clear scroll request.
            ClearScrollResponse clearScrollResponse = restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);

            boolean succeeded = clearScrollResponse.isSucceeded();

            LOGGER.info("Retrieved the ids for the documents in index={}, succeeded={}.", indexName, succeeded);
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }

        return idList;
    }

    @Override
    public final void updateIndexDocuments(final String indexName, final Map<String, String> documentMap)
    {
        LOGGER.info("Updating Elasticsearch index documents, indexName={}.", indexName);

        List<String> allIndices = getAliases(indexName);

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            for (String index : allIndices)
            {
                // Prepare a bulk request
                BulkRequest bulkRequest = new BulkRequest();

                // For each document prepare an insert request and add it to the bulk request
                documentMap.forEach((id, jsonString) -> bulkRequest.add(new IndexRequest(index).id(id).source(jsonString, XContentType.JSON)));

                // Make the bulk request
                BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                // If there are failures log them
                if (!bulkResponse.hasFailures())
                {
                    LOGGER.error("Bulk response error={}.", bulkResponse.buildFailureMessage());
                }
            }
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }
    }

    @Override
    public Settings getIndexSettings(final String indexName)
    {
        LOGGER.info("Get the Elasticsearch index settings, indexName={}.", indexName);

        // Build the get settings request.
        GetSettingsRequest request = new GetSettingsRequest().indices(indexName);

        // Create a get settings reponse object.
        GetSettingsResponse getSettingsResponse;

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            // Make the get settings request.
            getSettingsResponse = restHighLevelClient.indices().getSettings(request, RequestOptions.DEFAULT);
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }

        return getSettingsResponse.getIndexToSettings().get(indexName);
    }

    @Override
    public DocsStats getIndexStats(final String indexName)
    {
        LOGGER.info("Get the Elasticsearch index stats, indexName={}.", indexName);

        // For now simply return a new DocStats object.
        // Note: this is the same functionality as ESv1.
        return new DocsStats();
    }

    private List<String> getAliases(final String aliasName)
    {
        // Build the get aliases request.
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest(aliasName);

        // Create a get alias response object.
        GetAliasesResponse getAliasesResponse;

        // Get the Elasticsearch REST high level client. The REST high level client is auto closeable, so use try with resources.
        try (final RestHighLevelClient restHighLevelClient = elasticsearchRestHighLevelClientFactory.getRestHighLevelClient())
        {
            // Make the get aliases request.
            getAliasesResponse = restHighLevelClient.indices().getAlias(getAliasesRequest, RequestOptions.DEFAULT);
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Caught IOException while attempting to use the ElasticsearchRestHighLevelClient.", ioException);

            throw new ElasticsearchRestClientException();
        }

        // Get the aliases from the response.
        Map<String, Set<AliasMetaData>> aliases = getAliasesResponse.getAliases();

        // Return just the aliases.
        return new ArrayList<>(aliases.keySet());
    }
}
