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


import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.DocsStats;

public interface IndexFunctionsDao extends BaseJpaDao
{
    /**
     * The index function will take as arguments indexName, documentType, id, json and add the document to the index.
     *
     * @param indexName index name
     * @param id id
     * @param json json
     */
    void createIndexDocument(String indexName, String id, String json);

    /**
     * The number of types in index function will take as arguments the index name and will return the number of documents in the index.
     *
     * @param indexName index name
     *
     * @return number of types in index
     */
    long getNumberOfTypesInIndex(String indexName);

    /**
     * The isValid function will take as arguments indexName, id, json and validate the document against the index and return true if the document
     * is valid and false otherwise.
     *
     * @param indexName index name
     * @param id  id
     * @param json json
     *
     * @return is valid
     */
    boolean isValidDocumentIndex(String indexName, String id, String json);

    /**
     * The create index documents function will take as arguments the index name, and a map of new documents. The document map key is the
     * document id, and the value is the document as a JSON string.
     *
     * @param indexName index name
     * @param documentMap document map
     */
    void createIndexDocuments(String indexName, Map<String, String> documentMap);

    /**
     * The index exists predicate will take as an argument the index name and will return tree if the index exists and false otherwise.
     *
     * @param indexName index name
     *
     * @return is index exist
     */
    boolean isIndexExists(String indexName);

    /**
     * The delete index function will take as an argument the index name and will delete the index.
     *
     * @param indexName indexName
     */
    void deleteIndex(String indexName);

    /**
     * The validate function will take as arguments indexName, id, json and validate the document against the index.
     *
     * @param indexName index name
     * @param id id
     * @param json json
     */
    void validateDocumentIndex(String indexName, String id, String json);

     /**
      * The create index function will take as arguments the index name, and mapping and will create a new index.
      * @param indexName index name
      * @param mapping mapping
      * @param settings setting
      * @param alias alias
     */
     void createIndex(String indexName, String mapping, String settings, String alias);

    /**
     * The delete document by id function will delete a document in the index by the document id.
     *
     * @param indexName index name
     * @param id id
     */
    void deleteDocumentById(String indexName, String id);

    /**
     * The delete index documents function will delete a list of document in the index by a list of document ids.
     *
     * @param indexName the name of the index where the document exists.
     * @param ids the ids of the documents to delete.
     */
    void deleteIndexDocuments(String indexName, List<Long> ids);

    /**
     * The ids in index function will take as arguments the index name and will return a list of all the ids in the index.
     *
     * @param indexName index name
     *
     * @return list of ids
     */
    List<String> getIdsInIndex(String indexName);

    /**
     * The update index documents function will take as arguments the index name and a map of documents to update. The document map key is the
     * document id, and the value is the document as a JSON string.
     *
     * @param indexName index name
     * @param documentMap document map
     */
    void updateIndexDocuments(String indexName, Map<String, String> documentMap);

    /**
     * Get the index settings
     *
     * @param indexName index name
     *
     * @return index settings
     */
    Settings getIndexSettings(String indexName);

    /**
     * Get index statistics
     *
     * @param indexName index name
     *
     * @return index stats
     */
    DocsStats getIndexStats(String indexName);
}
