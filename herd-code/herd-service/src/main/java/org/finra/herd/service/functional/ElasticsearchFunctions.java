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

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


/**
 * This class contains functions that can be used to work with an Elasticsearch index.
 */
@Component
public class ElasticsearchFunctions implements SearchFunctions
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchFunctions.class);

    /**
     * The transport client is a connection to the elasticsearch index
     */
    @Autowired
    private TransportClient transportClient;

    /**
     * The index function will take as arguments indexName, documentType, id, json and add the document to the index.
     */
    private final QuadConsumer<String, String, String, String> indexFunction = (indexName, documentType, id, json) -> {
        final IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex(indexName, documentType, id);
        indexRequestBuilder.setSource(json);
        indexRequestBuilder.execute().actionGet();
    };

    /**
     * The validate function will take as arguments indexName, documentType, id, json and validate the document against the index.
     */
    private final QuadConsumer<String, String, String, String> validateFunction = (indexName, documentType, id, json) -> {
        LOGGER.info("Validating Elasticsearch document, indexName={}, documentType={}, id={}.", indexName, documentType, id);

        // Get the document from the index
        final GetRequestBuilder getRequestBuilder = transportClient.prepareGet(indexName, documentType, id);
        final GetResponse getResponse = getRequestBuilder.execute().actionGet();

        // Retrieve the JSON string from the get response
        final String jsonStringFromIndex = getResponse.getSourceAsString();

        // If the document does not exist in the index add the document to the index
        if (StringUtils.isEmpty(jsonStringFromIndex))
        {
            LOGGER.warn("Document does not exist in the index, adding the document to the index.");
            final IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex(indexName, documentType, id);
            indexRequestBuilder.setSource(json);
            indexRequestBuilder.execute().actionGet();
        }
        // Else if the JSON does not match the JSON from the index update the index
        else if (!json.equals(jsonStringFromIndex))
        {
            LOGGER.warn("Document does not match the document in the index, updating the document in the index.");
            final UpdateRequestBuilder updateRequestBuilder = transportClient.prepareUpdate(indexName, documentType, id);
            updateRequestBuilder.setDoc(json);
            updateRequestBuilder.execute().actionGet();
        }
    };

    /**
     * The isValid function will take as arguments indexName, documentType, id, json and validate the document against the index and return true if the document
     * is valid and false otherwise.
     */
    private final QuadPredicate<String, String, String, String> isValidFunction = (indexName, documentType, id, json) -> {
        // Get the document from the index
        final GetResponse getResponse = transportClient.prepareGet(indexName, documentType, id).execute().actionGet();

        // Retrieve the JSON string from the get response
        final String jsonStringFromIndex = getResponse.getSourceAsString();

        // Return true if the json from the index is not null or empty and the json from the index matches the object from the database
        return StringUtils.isNotEmpty(jsonStringFromIndex) && jsonStringFromIndex.equals(json);
    };

    /**
     * The index exists predicate will take as an argument the index name and will return tree if the index exists and false otherwise.
     */
    private final Predicate<String> indexExistsFunction = indexName -> {
        final IndicesExistsResponse indicesExistsResponse = transportClient.admin().indices().prepareExists(indexName).execute().actionGet();
        return indicesExistsResponse.isExists();
    };

    /**
     * The delete index function will take as an argument the index name and will delete the index.
     */
    private final Consumer<String> deleteIndexFunction = indexName -> {
        LOGGER.info("Deleting Elasticsearch index, indexName={}.", indexName);
        final DeleteIndexRequestBuilder deleteIndexRequestBuilder = transportClient.admin().indices().prepareDelete(indexName);
        deleteIndexRequestBuilder.execute().actionGet();
    };

    /**
     * The create index function will take as arguments the index name, document type, and mapping and will create a new index.
     */
    private final TriConsumer<String, String, String> createIndexFunction = (indexName, documentType, mapping) -> {
        LOGGER.info("Creating Elasticsearch index, indexName={}, documentType={}.", indexName, documentType);
        final CreateIndexRequestBuilder createIndexRequestBuilder = transportClient.admin().indices().prepareCreate(indexName);
        createIndexRequestBuilder.addMapping(documentType, mapping);
        createIndexRequestBuilder.execute().actionGet();
    };

    /**
     * The delete document by id function will delete a document in the index by the document id.
     */
    private final TriConsumer<String, String, String> deleteDocumentByIdFunction = (indexName, documentType, id) -> {
        LOGGER.info("Deleting Elasticsearch document from index, indexName={}, documentType={}, id={}.", indexName, documentType, id);
        final DeleteRequestBuilder deleteRequestBuilder = transportClient.prepareDelete(indexName, documentType, id);
        deleteRequestBuilder.execute().actionGet();
    };

    /**
     * The number of types in index function will take as arguments the index name and the document type and will return the number of documents in the index.
     */
    private final BiFunction<String, String, Long> numberOfTypesInIndexFunction = (indexName, documentType) -> {
        final SearchResponse searchResponse = transportClient.prepareSearch(indexName).setTypes(documentType).execute().actionGet();
        return searchResponse.getHits().getTotalHits();
    };

    /**
     * The ids in index function will take as arguments the index name and the document type and will return a list of all the ids in the index.
     */
    private final BiFunction<String, String, List<String>> idsInIndexFunction = (indexName, documentType) -> {
        List<String> idList = new ArrayList<>();
        final SearchResponse searchResponse = transportClient.prepareSearch().setQuery(matchAllQuery()).execute().actionGet();
        for (SearchHit searchHit : searchResponse.getHits())
        {
            idList.add(searchHit.id());
        }
        return idList;
    };

    @Override
    public QuadConsumer<String, String, String, String> getIndexFunction()
    {
        return indexFunction;
    }

    @Override
    public QuadConsumer<String, String, String, String> getValidateFunction()
    {
        return validateFunction;
    }

    @Override
    public QuadPredicate<String, String, String, String> getIsValidFunction()
    {
        return isValidFunction;
    }

    @Override
    public Predicate<String> getIndexExistsFunction()
    {
        return indexExistsFunction;
    }

    @Override
    public Consumer<String> getDeleteIndexFunction()
    {
        return deleteIndexFunction;
    }

    @Override
    public TriConsumer<String, String, String> getCreateIndexFunction()
    {
        return createIndexFunction;
    }

    @Override
    public TriConsumer<String, String, String> getDeleteDocumentByIdFunction()
    {
        return deleteDocumentByIdFunction;
    }

    @Override
    public BiFunction<String, String, Long> getNumberOfTypesInIndexFunction()
    {
        return numberOfTypesInIndexFunction;
    }

    @Override
    public BiFunction<String, String, List<String>> getIdsInIndexFunction()
    {
        return idsInIndexFunction;
    }
}
