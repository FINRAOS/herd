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

import org.finra.herd.model.api.xml.SearchIndex;
import org.finra.herd.model.api.xml.SearchIndexCreateRequest;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.api.xml.SearchIndexKeys;

/**
 * The search index service.
 */
public interface SearchIndexService
{
    /**
     * Creates a new search index.
     *
     * @param request the information needed to create a search index
     *
     * @return the newly created search index
     */
    public SearchIndex createSearchIndex(SearchIndexCreateRequest request);

    /**
     * Deletes an existing search index for the specified key.
     *
     * @param searchIndexKey the search index key
     *
     * @return the search index that was deleted
     */
    public SearchIndex deleteSearchIndex(SearchIndexKey searchIndexKey);

    /**
     * Gets an existing search index for the specified key.
     *
     * @param searchIndexKey the search index key
     *
     * @return the retrieved search index
     */
    public SearchIndex getSearchIndex(SearchIndexKey searchIndexKey);

    /**
     * Gets a list of search index keys for all search indexes defined in the system.
     *
     * @return the search index keys
     */
    public SearchIndexKeys getSearchIndexes();
}
