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
package org.finra.herd.rest;

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.SearchIndex;
import org.finra.herd.model.api.xml.SearchIndexCreateRequest;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.api.xml.SearchIndexKeys;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.SearchIndexService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles search index requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Search Index")
public class SearchIndexRestController extends HerdBaseController
{
    public static final String SEARCH_INDEXES_URI_PREFIX = "/searchIndexes";

    @Autowired
    private SearchIndexService searchIndexService;

    /**
     * Creates a new search index.
     *
     * @param request the information needed to create a search index
     *
     * @return the newly created search index
     */
    @RequestMapping(value = SEARCH_INDEXES_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_SEARCH_INDEXES_POST)
    public SearchIndex createSearchIndexes(@RequestBody SearchIndexCreateRequest request)
    {
        return searchIndexService.createSearchIndex(request);
    }

    /**
     * Deletes an existing search index by its name.
     *
     * @param searchIndexName the search index name
     *
     * @return the search index that got deleted
     */
    @RequestMapping(value = SEARCH_INDEXES_URI_PREFIX + "/{searchIndexName}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_SEARCH_INDEXES_DELETE)
    public SearchIndex deleteSearchIndex(@PathVariable("searchIndexName") String searchIndexName)
    {
        return searchIndexService.deleteSearchIndex(new SearchIndexKey(searchIndexName));
    }

    /**
     * Gets an existing search index by its name.
     *
     * @param searchIndexName the name of the search index
     *
     * @return the retrieved search index
     */
    @RequestMapping(value = SEARCH_INDEXES_URI_PREFIX + "/{searchIndexName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SEARCH_INDEXES_GET)
    public SearchIndex getSearchIndex(@PathVariable("searchIndexName") String searchIndexName)
    {
        return searchIndexService.getSearchIndex(new SearchIndexKey(searchIndexName));
    }

    /**
     * Gets a list of search index keys for all search indexes defined in the system.
     *
     * @return the list of search index keys
     */
    @RequestMapping(value = SEARCH_INDEXES_URI_PREFIX, method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SEARCH_INDEXES_ALL_GET)
    public SearchIndexKeys getSearchIndexes()
    {
        return searchIndexService.getSearchIndexes();
    }
}
