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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.searchbox.action.Action;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.Count;
import io.searchbox.core.Delete;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.Stats;
import io.searchbox.indices.aliases.AddAliasMapping;
import io.searchbox.indices.aliases.GetAliases;
import io.searchbox.indices.aliases.ModifyAliases;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.indices.settings.GetSettings;
import io.searchbox.params.Parameters;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

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
        Index index = new Index.Builder(json).index(indexName).type(documentType).id(id).build();
        JestResult jestResult = jestClientHelper.execute(index);
        LOGGER.info("Creating Index Document, indexName={}. successful is {}", indexName, jestResult.isSucceeded());
    }

    @Override
    public boolean isValidDocumentIndex(String indexName, String documentType, String id, String json)
    {
        Get get =  new Get.Builder(indexName,  id).type(documentType).build();
        JestResult jestResult = jestClientHelper.execute(get);

        // Retrieve the JSON string from the get response
        final String jsonStringFromIndex = jestResult.getSourceAsString();

        // Return true if the json from the index is not null or empty and the json from the index matches the object from the database
        return StringUtils.isNotEmpty(jsonStringFromIndex) && jsonStringFromIndex.equals(json);
    }

    @Override
    public final boolean isIndexExists(String indexName)
    {
        Action action = new IndicesExists.Builder(indexName).build();
        JestResult result = jestClientHelper.execute(action);

        return result.isSucceeded();
    }

    /**
     * The delete index function will take as an argument the index name and will delete the index.
     */
    @Override
    public final void deleteIndex(String indexName)
    {
        Action action = new DeleteIndex.Builder(indexName).build();

        LOGGER.info("Deleting Elasticsearch index, indexName={}.", indexName);
        JestResult result = jestClientHelper.execute(action);

        LOGGER.info("Deleting Elasticsearch index, indexName={}. result successful is {} ", indexName, result.isSucceeded());
    }

    /**
     * The validate function will take as arguments indexName, documentType, id, json and validate the document against the index.
     */
    @Override
    public final void validateDocumentIndex(String indexName, String documentType, String id, String json)
    {
        LOGGER.info("Validating Elasticsearch document, indexName={}, documentType={}, id={}.", indexName, documentType, id);

        Get get =  new Get.Builder(indexName,  id).type(documentType).build();

        JestResult jestResult = jestClientHelper.execute(get);

        // Retrieve the JSON string from the get response
        final String jsonStringFromIndex = jestResult.getSourceAsString();

        // If the document does not exist in the index add the document to the index
        if (StringUtils.isEmpty(jsonStringFromIndex))
        {
            LOGGER.warn("Document does not exist in the index, adding the document to the index.");
            Index index = new Index.Builder(json).index(indexName).type(documentType).id(id).build();
            jestResult = jestClientHelper.execute(index);
            LOGGER.info("adding the document to the index is {}", jestResult.isSucceeded());
        }
        // Else if the JSON does not match the JSON from the index update the index
        else if (!json.equals(jsonStringFromIndex))
        {
            LOGGER.warn("Document does not match the document in the index, updating the document in the index.");
            Index index = new Index.Builder(json).index(indexName).type(documentType).id(id).build();
            jestResult = jestClientHelper.execute(index);
            LOGGER.info("updating the document to the index is {}", jestResult.isSucceeded());
        }
    }

    @Override
    public void createIndexDocuments(String indexName, String documentType, Map<String, String> documentMap)
    {
        LOGGER.info("Creating Elasticsearch index documents, indexName={}, documentType={}", indexName, documentType);

        List<String> allIndices = getAliases(indexName);

        allIndices.forEach((index) -> {
        // Prepare a bulk request builder
        //final BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(new ElasticsearchClientImpl(), BulkAction.INSTANCE);
        Bulk.Builder bulkBuilder = new Bulk.Builder();
        // For each document prepare an insert request and add it to the bulk request builder
        documentMap.forEach((id, jsonString) ->
        {
            BulkableAction createIndex = new Index.Builder(jsonString).index(index).type(documentType).id(id).build();
            bulkBuilder.addAction(createIndex);

        });

        JestResult jestResult = jestClientHelper.execute(bulkBuilder.build());

        // If there are failures log them
        if (!jestResult.isSucceeded())
        {
            LOGGER.error("Bulk response error = {}", jestResult.getErrorMessage());
        }
        });
    }

    /**
     * The create index function will take as arguments the index name, document type, and mapping and will create a new index.
     */
    @Override
    public final void createIndex(String indexName, String documentType, String mapping, String settings, String alias)
    {
        LOGGER.info("Creating Elasticsearch index, indexName={}, documentType={}.", indexName, documentType);

        CreateIndex createIndex = new CreateIndex.Builder(indexName).settings(settings).build();
        PutMapping putMapping = new PutMapping.Builder(indexName, documentType, mapping).build();
        ModifyAliases modifyAliases = new ModifyAliases.Builder(new AddAliasMapping.Builder(indexName, alias).build()).build();

        JestResult jestResult = jestClientHelper.execute(createIndex);
        LOGGER.info("Creating Elasticsearch index, indexName={}, documentType={} successful={}", indexName, documentType, jestResult.isSucceeded());
        jestResult = jestClientHelper.execute(putMapping);
        LOGGER
            .info("Creating Elasticsearch index put mappings, indexName={}, documentType={} successful={}", indexName, documentType, jestResult.isSucceeded());
        jestResult = jestClientHelper.execute(modifyAliases);
        LOGGER.info("Creating Elasticsearch index alias, indexName={}, alias={}", indexName, alias, jestResult.isSucceeded());
        // If there are failures log them
        if (!jestResult.isSucceeded())
        {
            LOGGER.error("Error in index creation= {}", jestResult.getErrorMessage());
        }
   }

    /**
     * The delete document by id function will delete a document in the index by the document id.
     */
    @Override
    public final void deleteDocumentById(String indexName, String documentType, String id)
    {
        LOGGER.info("Deleting Elasticsearch document from index, indexName={}, documentType={}, id={}.", indexName, documentType, id);

        Action action = new Delete.Builder(id).index(indexName).type(documentType).build();

        JestResult result = jestClientHelper.execute(action);

        LOGGER.info("Deleting Elasticsearch document from index, indexName={}, documentType={}, id={} is successfully {}. ", indexName, documentType, id,
            result.isSucceeded());
    }

    /**
     * The delete index documents function will delete a list of document in the index by a list of document ids.
     */
    @Override
    public final void deleteIndexDocuments(String indexName, String documentType, List<Long> ids)
    {
        LOGGER.info("Deleting Elasticsearch documents from index, indexName={}, documentType={}, ids={}.", indexName, documentType,
            ids.stream().map(Object::toString).collect(Collectors.joining(",")));

        List<String> allIndices = getAliases(indexName);

        allIndices.forEach((index) -> {
        // Prepare a bulk request builder
        Bulk.Builder bulkBuilder = new Bulk.Builder();


        // For each document prepare a delete request and add it to the bulk request builder
        ids.forEach(id ->
        {
            BulkableAction action = new Delete.Builder(id.toString()).index(index).type(documentType).build();
            bulkBuilder.addAction(action);
        });

        JestResult jestResult = jestClientHelper.execute(bulkBuilder.build());
        // If there are failures log them
        if (!jestResult.isSucceeded())
        {
            LOGGER.error("Bulk response error = {}", jestResult.getErrorMessage());
        }
        });

    }

    @Override
    public long getNumberOfTypesInIndex(String indexName, String documentType)
    {
        Count count = new Count.Builder().addIndex(indexName).addType(documentType).build();

        JestResult jestResult  = jestClientHelper.execute(count);
        return Long.parseLong(jestResult.getSourceAsString());
    }

    /**
     * The ids in index function will take as arguments the index name and the document type and will return a list of all the ids in the index.
     */
    @Override
    public final List<String> getIdsInIndex(String indexName, String documentType)
    {
        // Create an array list for storing the ids
        List<String> idList = new ArrayList<>();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // Create a search request and set the scroll time and scroll size
        final SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(new ElasticsearchClientImpl(), SearchAction.INSTANCE);
        searchRequestBuilder.setIndices(indexName).setTypes(documentType).setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME))
            .setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE).setSource(searchSourceBuilder);

        // Retrieve the search response
        final Search.Builder searchBuilder = new Search.Builder(searchRequestBuilder.toString()).addIndex(indexName);

        searchBuilder.setParameter(Parameters.SIZE, ELASTIC_SEARCH_SCROLL_PAGE_SIZE);
        searchBuilder.setParameter(Parameters.SCROLL, new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME).toString());

        JestResult jestResult = jestClientHelper.execute(searchBuilder.build());

        // While there are hits available, page through the results and add them to the id list
        while (jestResult.getSourceAsStringList().size() != 0)
        {
            for (String jsonString : jestResult.getSourceAsStringList())
            {
                JsonElement root = new JsonParser().parse(jsonString);
                idList.add(root.getAsJsonObject().get("id").getAsString());
            }
            String scrollId = jestResult.getJsonObject().get("_scroll_id").getAsString();
            SearchScroll scroll = new SearchScroll.Builder(scrollId, new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME).toString()).build();
            jestResult = jestClientHelper.execute(scroll);

        }
        return idList;
    }

    /**
     * The update index documents function will take as arguments the index name, document type, and a map of documents to update. The document map key is the
     * document id, and the value is the document as a JSON string.
     */
    @Override
    public final void updateIndexDocuments(String indexName, String documentType, Map<String, String> documentMap)
    {
        LOGGER.info("Updating Elasticsearch index documents, indexName={}, documentType={}.", indexName, documentType);

        List<String> allIndices = getAliases(indexName);

        allIndices.forEach((index) -> {
            // Prepare a bulk request builder
            Bulk.Builder bulkBuilder = new Bulk.Builder();
            // For each document prepare an update request and add it to the bulk request builder
            documentMap.forEach((id, jsonString) -> {
                BulkableAction updateIndex = new Index.Builder(jsonString).index(index).type(documentType).id(id).build();
                bulkBuilder.addAction(updateIndex);
            });

            // Execute the bulk update request
            JestResult jestResult = jestClientHelper.execute(bulkBuilder.build());

            // If there are failures log them
            if (!jestResult.isSucceeded())
            {
                LOGGER.error("Bulk response error = {}", jestResult.getErrorMessage());
            }
        });
    }

    @Override
    public Settings getIndexSettings(String indexName)
    {
        GetSettings getSettings = new GetSettings.Builder().addIndex(indexName).build();
        JestResult result = jestClientHelper.execute(getSettings);
        Assert.isTrue(result.isSucceeded(), result.getErrorMessage());
        JsonObject json = result.getJsonObject().getAsJsonObject(indexName).getAsJsonObject("settings");
        return Settings.builder().loadFromSource(json.toString()).build();
    }

    @Override
    public DocsStats getIndexStats(String indexName)
    {
        Action getStats = new Stats.Builder().addIndex(indexName).build();
        JestResult jestResult = jestClientHelper.execute(getStats);
        Assert.isTrue(jestResult.isSucceeded(), jestResult.getErrorMessage());
        JsonObject statsJson = jestResult.getJsonObject().getAsJsonObject("indices").getAsJsonObject(indexName).getAsJsonObject("primaries");
        JsonObject docsJson = statsJson.getAsJsonObject("docs");
        return new DocsStats(docsJson.get("count").getAsLong(), docsJson.get("deleted").getAsLong());
    }

    private List<String> getAliases(String aliasName)
    {
        GetAliases getAliases = new GetAliases.Builder().build();
        JestResult jestResult = jestClientHelper.execute(getAliases);
        Assert.isTrue(jestResult.isSucceeded(), jestResult.getErrorMessage());
        List<String> indexNameList =
            jestResult.getJsonObject().entrySet().stream().filter(e -> e.getKey().startsWith(aliasName)).map(Map.Entry::getKey).collect(Collectors.toList());
        return indexNameList;
    }
}
