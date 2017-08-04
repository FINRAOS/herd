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
import java.util.Set;

import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.dto.SearchFilterType;
import org.finra.herd.model.jpa.TagEntity;


public interface BusinessObjectDefinitionIndexSearchDao
{
    /**
     * Returns business object definition records indexed in the Elasticsearch that match the specified tags.
     *
     * @param indexName the index name
     * @param documentType the document type
     * @param nestedTagEntityMaps the
     * @param facetFieldsList the set of facet fields
     *
     * @return the response from the Elasticsearch
     */
    public ElasticsearchResponseDto searchBusinessObjectDefinitionsByTags(String indexName, String documentType,
        List<Map<SearchFilterType, List<TagEntity>>> nestedTagEntityMaps, Set<String> facetFieldsList);

    /**
     * Returns all business object definition records indexed in the Elasticsearch.
     *
     * @param indexName the index name
     * @param documentType the document type
     * @param facetFieldsList the set of facet fields
     *
     * @return the response from the Elasticsearch
     */
    public ElasticsearchResponseDto findAllBusinessObjectDefinitions(String indexName, String documentType, Set<String> facetFieldsList);
}
