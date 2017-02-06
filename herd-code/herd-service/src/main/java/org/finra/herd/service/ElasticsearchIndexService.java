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
package org.finra.herd.service;

import org.finra.herd.model.api.xml.ElasticsearchIndex;
import org.finra.herd.model.api.xml.ElasticsearchIndexCreateRequest;
import org.finra.herd.model.api.xml.ElasticsearchIndexKey;
import org.finra.herd.model.api.xml.ElasticsearchIndexKeys;

/**
 * The Elasticsearch index service.
 */
public interface ElasticsearchIndexService
{
    /**
     * Creates a new Elasticsearch index.
     *
     * @param request the information needed to create an Elasticsearch index
     *
     * @return the newly created Elasticsearch index
     */
    public ElasticsearchIndex createElasticsearchIndex(ElasticsearchIndexCreateRequest request);

    /**
     * Deletes an existing Elasticsearch index for the specified key.
     *
     * @param elasticsearchIndexKey the Elasticsearch index key
     *
     * @return the Elasticsearch index that was deleted
     */
    public ElasticsearchIndex deleteElasticsearchIndex(ElasticsearchIndexKey elasticsearchIndexKey);

    /**
     * Gets an existing Elasticsearch index for the specified key.
     *
     * @param elasticsearchIndexKey the Elasticsearch index key
     *
     * @return the retrieved Elasticsearch index
     */
    public ElasticsearchIndex getElasticsearchIndex(ElasticsearchIndexKey elasticsearchIndexKey);

    /**
     * Gets a list of Elasticsearch index keys for all Elasticsearch indexes defined in the system.
     *
     * @return the Elasticsearch index keys
     */
    public ElasticsearchIndexKeys getElasticsearchIndexes();
}
