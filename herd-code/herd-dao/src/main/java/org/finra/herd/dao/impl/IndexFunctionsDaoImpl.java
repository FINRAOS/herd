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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.gson.JsonObject;
import io.searchbox.action.Action;
import io.searchbox.client.JestResult;
import io.searchbox.indices.settings.GetSettings;
import io.searchbox.core.Delete;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.Stats;
import io.searchbox.params.Parameters;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.DocsStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.IndexFunctionsDao;
import org.finra.herd.dao.helper.ElasticsearchClientImpl;
import org.finra.herd.dao.helper.JestClientHelper;

@Repository
public class IndexFunctionsDaoImpl extends AbstractHerdDao implements IndexFunctionsDao
{
    /**
     * The logger used to write messages to the log
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexFunctionsDaoImpl.class);

    /**
     * Page size
     */
    public static final int ELASTIC_SEARCH_SCROLL_PAGE_SIZE = 100;

    /**
     * Scroll keep alive in milliseconds
     */
    public static final int ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME = 60000;

    /**
     * jest client helper
     */
    @Autowired
    private JestClientHelper jestClientHelper;

    /**
     * The index function will take as arguments indexName, documentType, id, json and add the document to the index.
     */
    @Override
    public final void createIndexDocument(String indexName, String documentType, String id, String json)
    {
        final IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(new ElasticsearchClientImpl(), IndexAction.INSTANCE);
        indexRequestBuilder.setId(id).setType(documentType).setIndex(indexName);
        indexRequestBuilder.setSource(json);
        final Search.Builder searchBuilder = new Search.Builder(indexRequestBuilder.toString());
        JestResult jestResult = jestClientHelper.searchExecute(searchBuilder.build());
    }

    @Override
    public boolean isValidDocumentIndex(String indexName, String documentType, String id, String json)
    {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(new ElasticsearchClientImpl(), SearchAction.INSTANCE);
        searchRequestBuilder.setIndices(indexName);
        searchRequestBuilder.setTypes(documentType);

        final Search.Builder searchBuilder = new Search.Builder(searchRequestBuilder.toString());
        SearchResult searchResult = jestClientHelper.searchExecute(searchBuilder.build());

        // Retrieve the JSON string from the get response
        final String jsonStringFromIndex = searchResult.getSourceAsString();

        // Return true if the json from the index is not null or empty and the json from the index matches the object from the database
        return StringUtils.isNotEmpty(jsonStringFromIndex) && jsonStringFromIndex.equals(json);
    }

    @Override
    public final boolean isIndexExists(String indexName)
    {

        Action action = new IndicesExists.Builder(indexName).build();
        JestResult result = jestClientHelper.executeAction(action);

        return result.isSucceeded();
    }

    /**
     * The delete index function will take as an argument the index name and will delete the index.
     */
    @Override
    public final void deleteIndex(String indexName)
    {
        Action action = new Delete.Builder(indexName).build();

        LOGGER.info("Deleting Elasticsearch index, indexName={}.", indexName);
        JestResult result = jestClientHelper.executeAction(action);

        LOGGER.info("Deleting Elasticsearch index, indexName={}. result successful is {} ", indexName, result.isSucceeded());
    }

    /**
     * The validate function will take as arguments indexName, documentType, id, json and validate the document against the index.
     */
    @Override
    public final void validateDocumentIndex(String indexName, String documentType, String id, String json)
    {
        LOGGER.info("Validating Elasticsearch document, indexName={}, documentType={}, id={}.", indexName, documentType, id);

        // Get the document from the index
        final GetRequestBuilder getRequestBuilder = new GetRequestBuilder(new ElasticsearchClientImpl(), GetAction.INSTANCE);
        getRequestBuilder.setIndex(indexName).setType(documentType);

        final Search.Builder searchBuilder = new Search.Builder(getRequestBuilder.toString());
        SearchResult searchResult = jestClientHelper.searchExecute(searchBuilder.build());

        // Retrieve the JSON string from the get response
        final String jsonStringFromIndex = searchResult.getSourceAsString();

        // If the document does not exist in the index add the document to the index
        if (StringUtils.isEmpty(jsonStringFromIndex))
        {
            LOGGER.warn("Document does not exist in the index, adding the document to the index.");
            final IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(new ElasticsearchClientImpl(), IndexAction.INSTANCE);
            indexRequestBuilder.setIndex(indexName).setType(documentType).setId(id);
            indexRequestBuilder.setSource(json);
            jestClientHelper.searchExecute(new Search.Builder(indexRequestBuilder.toString()).build());
        }
        // Else if the JSON does not match the JSON from the index update the index
        else if (!json.equals(jsonStringFromIndex))
        {
            LOGGER.warn("Document does not match the document in the index, updating the document in the index.");
            final UpdateRequestBuilder updateRequestBuilder = new UpdateRequestBuilder(new ElasticsearchClientImpl(), UpdateAction.INSTANCE);
            updateRequestBuilder.setIndex(indexName).setType(documentType).setId(id);
            updateRequestBuilder.setDoc(json);
            jestClientHelper.searchExecute(new Search.Builder(updateRequestBuilder.toString()).build());
        }
    }

    @Override
    public void createIndexDocuments(String indexName, String documentType, Map<String, String> documentMap)
    {
        LOGGER.info("Creating Elasticsearch index documents, indexName={}, documentType={}, documentMap={}.", indexName, documentType,
            Joiner.on(",").withKeyValueSeparator("=").join(documentMap));

        // Prepare a bulk request builder
        final BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(new ElasticsearchClientImpl(), BulkAction.INSTANCE);
        // For each document prepare an insert request and add it to the bulk request builder
        documentMap.forEach((id, jsonString) ->
        {
            final IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(new ElasticsearchClientImpl(), IndexAction.INSTANCE);
            indexRequestBuilder.setId(id);
            indexRequestBuilder.setIndex(indexName);
            indexRequestBuilder.setType(documentType);
            indexRequestBuilder.setSource(jsonString);
            bulkRequestBuilder.add(indexRequestBuilder);
        });

        final Search.Builder searchBuilder = new Search.Builder(bulkRequestBuilder.toString());

        SearchResult searchResult = jestClientHelper.searchExecute(searchBuilder.build());

        // If there are failures log them
        if (!searchResult.isSucceeded())
        {
            LOGGER.error("Bulk response error = {}", searchResult.getErrorMessage());
        }
    }

    /**
     * The create index function will take as arguments the index name, document type, and mapping and will create a new index.
     */
    @Override
    public final void createIndex(String indexName, String documentType, String mapping, String settings)
    {
        LOGGER.info("Creating Elasticsearch index, indexName={}, documentType={}.", indexName, documentType);

        final CreateIndexRequestBuilder createIndexRequestBuilder = new CreateIndexRequestBuilder(new ElasticsearchClientImpl(), CreateIndexAction.INSTANCE, indexName);
        createIndexRequestBuilder.setSettings(settings);
        createIndexRequestBuilder.addMapping(documentType, mapping);

        final Search.Builder searchBuilder = new Search.Builder(createIndexRequestBuilder.toString());

        SearchResult searchResult = jestClientHelper.searchExecute(new Search.Builder(searchBuilder.toString()).build());
        System.out.println(searchResult.isSucceeded());
    }

    /**
     * The delete document by id function will delete a document in the index by the document id.
     */
    @Override
    public final void deleteDocumentById(String indexName, String documentType, String id)
    {
        LOGGER.info("Deleting Elasticsearch document from index, indexName={}, documentType={}, id={}.", indexName, documentType, id);

        Action action = new Delete.Builder(indexName).id(id).type(documentType).build();

        JestResult result = jestClientHelper.executeAction(action);

        LOGGER.info("Deleting Elasticsearch document from index, indexName={}, documentType={}, id={} is successfully {}. ", indexName, documentType, id,
            result.isSucceeded());
    }

    /**
     * The delete index documents function will delete a list of document in the index by a list of document ids.
     */
    @Override
    public final void deleteIndexDocuments(String indexName, String documentType, List<Integer> ids)
    {
        LOGGER.info("Deleting Elasticsearch documents from index, indexName={}, documentType={}, ids={}.", indexName, documentType,
            ids.stream().map(Object::toString).collect(Collectors.joining(",")));

        // Prepare a bulk request builder
        final BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(new ElasticsearchClientImpl(), BulkAction.INSTANCE);

        // For each document prepare a delete request and add it to the bulk request builder
        ids.forEach(id ->
        {
            final DeleteRequestBuilder deleteRequestBuilder = new DeleteRequestBuilder(new ElasticsearchClientImpl(), DeleteAction.INSTANCE);
            deleteRequestBuilder.setId(id.toString()).setType(documentType).setIndex(indexName);
            bulkRequestBuilder.add(deleteRequestBuilder);
        });

        final Search.Builder searchBuilder = new Search.Builder(bulkRequestBuilder.toString());

        SearchResult searchResult = jestClientHelper.searchExecute(searchBuilder.build());

        // If there are failures log them
        if (!searchResult.isSucceeded())
        {
            LOGGER.error("Bulk response error = {}", searchResult.getErrorMessage());
        }

    }

    @Override
    public long getNumberOfTypesInIndex(String indexName, String documentType)
    {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(new ElasticsearchClientImpl(), SearchAction.INSTANCE);
        searchRequestBuilder.setIndices(indexName);
        searchRequestBuilder.setTypes(documentType);

        final Search.Builder searchBuilder = new Search.Builder(searchRequestBuilder.toString());
        SearchResult searchResult = jestClientHelper.searchExecute(searchBuilder.build());
        return searchResult.getTotal();
    }

    /**
     * The ids in index function will take as arguments the index name and the document type and will return a list of all the ids in the index.
     */
    @Override
    public final List<String> getIdsInIndex(String indexName, String documentType)
    {
        // Create an array list for storing the ids
        List<String> idList = new ArrayList<>();

        // Create a search request and set the scroll time and scroll size
        final SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(new ElasticsearchClientImpl(), SearchAction.INSTANCE);
        searchRequestBuilder.setTypes(documentType).setQuery(QueryBuilders.matchAllQuery()).setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME))
            .setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE);

        // Retrieve the search response
        final Search.Builder searchBuilder = new Search.Builder(searchRequestBuilder.toString());

        searchBuilder.setParameter(Parameters.SIZE, ELASTIC_SEARCH_SCROLL_PAGE_SIZE);
        searchBuilder.setParameter(Parameters.SCROLL, new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME).toString());

        JestResult jestResult = jestClientHelper.searchExecute(searchBuilder.build());


       /* // While there are hits available, page through the results and add them to the id list
        while (jestResult.getSourceAsObjectList() != 0)
        {
            for (SearchHit searchHit : hits)
            {
                idList.add(searchHit.id());
            }

            SearchScrollRequestBuilder searchScrollRequestBuilder = transportClient.prepareSearchScroll(searchResponse.getScrollId());
            searchScrollRequestBuilder.setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME));
            searchResponse = searchScrollRequestBuilder.execute().actionGet();
            searchHits = searchResponse.getHits();
            hits = searchHits.hits();
        }
*/
        return idList;
    }

    /**
     * The update index documents function will take as arguments the index name, document type, and a map of documents to update. The document map key is the
     * document id, and the value is the document as a JSON string.
     */
    @Override
    public final void updateIndexDocuments(String indexName, String documentType, Map<String, String> documentMap)
    {
        LOGGER.info("Updating Elasticsearch index documents, indexName={}, documentType={}, documentMap={}.", indexName, documentType,
            Joiner.on(",").withKeyValueSeparator("=").join(documentMap));

        // Prepare a bulk request builder
        final BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(new ElasticsearchClientImpl(), BulkAction.INSTANCE);

        // For each document prepare an update request and add it to the bulk request builder
        documentMap.forEach((id, jsonString) ->
        {
            final UpdateRequestBuilder updateRequestBuilder = new UpdateRequestBuilder(new ElasticsearchClientImpl(), UpdateAction.INSTANCE);
            updateRequestBuilder.setId(id).setIndex(indexName).setType(documentType);
            updateRequestBuilder.setDoc(jsonString);
            bulkRequestBuilder.add(updateRequestBuilder);
        });

        final Search.Builder searchBuilder = new Search.Builder(bulkRequestBuilder.toString());

        // Execute the bulk update request
        final SearchResult searchResult = jestClientHelper.searchExecute(searchBuilder.build());
        // If there are failures log them
        if (!searchResult.isSucceeded())
        {
            LOGGER.error("Bulk response error = {}", searchResult.getErrorMessage());
        }
    }

    @Override
    public Settings getIndexSettings(String indexName)
    {
        GetSettings getSettings = new GetSettings.Builder().addIndex(indexName).build();
        JestResult result = jestClientHelper.executeAction(getSettings);
        JsonObject json = result.getJsonObject().getAsJsonObject(indexName).getAsJsonObject("settings");
        Settings settings = Settings.builder().loadFromSource(json.toString()).build();
        return settings;
    }

    @Override
    public DocsStats getIndexStats(String indexName)
    {
        Action getStats = new Stats.Builder().addIndex(indexName).build();
        JestResult jestResult = jestClientHelper.executeAction(getStats);

        JsonObject statsJson = jestResult.getJsonObject().getAsJsonObject("indices").getAsJsonObject(indexName).getAsJsonObject("primaries");
        JsonObject docsJson = statsJson.getAsJsonObject("docs");
        DocsStats docsStats = new DocsStats(docsJson.get("count").getAsLong(), docsJson.get("deleted").getAsLong());

        return docsStats;
    }
}
