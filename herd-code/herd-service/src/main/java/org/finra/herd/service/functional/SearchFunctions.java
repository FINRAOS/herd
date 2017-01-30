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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.jpa.TagEntity;

/**
 * SearchFunctions interface used to provide search index functionality
 */
public interface SearchFunctions
{
    /**
     * The index function will take as arguments indexName, documentType, id, json and add the document to the index.
     */
    QuadConsumer<String, String, String, String> getIndexFunction();

    /**
     * The validate function will take as arguments indexName, documentType, id, json and validate the document against the index.
     */
    QuadConsumer<String, String, String, String> getValidateFunction();

    /**
     * The isValid function will take as arguments indexName, documentType, id, json and validate the document against the index and return true if the document
     * is valid and false otherwise.
     */
    QuadPredicate<String, String, String, String> getIsValidFunction();

    /**
     * The index exists predicate will take as an argument the index name and will return tree if the index exists and false otherwise.
     */
    Predicate<String> getIndexExistsFunction();

    /**
     * The delete index function will take as an argument the index name and will delete the index.
     */
    Consumer<String> getDeleteIndexFunction();

    /**
     * The create index documents function will take as arguments the index name, document type, and a map of new documents. The document map key is the
     * document id, and the value is the document as a JSON string.
     */
    TriConsumer<String, String, Map<String, String>> getCreateIndexDocumentsFunction();

    /**
     * The create index function will take as arguments the index name, document type, and mapping and will create a new index.
     */
    TriConsumer<String, String, String> getCreateIndexFunction();

    /**
     * The delete document by id function will delete a document in the index by the document id.
     */
    TriConsumer<String, String, String> getDeleteDocumentByIdFunction();

    /**
     * The delete index documents function will delete a list of document in the index by a list of document ids.
     */
    TriConsumer<String, String, List<Integer>> getDeleteIndexDocumentsFunction();

    /**
     * The number of types in index function will take as arguments the index name and the document type and will return the number of documents in the index.
     */
    BiFunction<String, String, Long> getNumberOfTypesInIndexFunction();

    /**
     * The ids in index function will take as arguments the index name and the document type and will return a list of all the ids in the index.
     */
    BiFunction<String, String, List<String>> getIdsInIndexFunction();

    /**
     * The find all business object definitions function will return all business object definition entities in the search index.
     */
    TriFunction<String, String, Set<String>, ElasticsearchResponseDto> getFindAllBusinessObjectDefinitionsFunction();

    /**
     * The search business object definitions by tags function will take a list of tag entities and return a list of business object definition entities. The
     * function will search the search index based on tag code and tag type code.
     */
    QuadFunction<String, String, List<TagEntity>, Set<String>, ElasticsearchResponseDto> getSearchBusinessObjectDefinitionsByTagsFunction();

    /**
     * The update index documents function will take as arguments the index name, document type, and a map of documents to update. The document map key is the
     * document id, and the value is the document as a JSON string.
     */
    TriConsumer<String, String, Map<String, String>> getUpdateIndexDocumentsFunction();
}
