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

import org.finra.herd.model.api.xml.ElasticsearchIndex;
import org.finra.herd.model.api.xml.ElasticsearchIndexCreateRequest;
import org.finra.herd.model.api.xml.ElasticsearchIndexKey;
import org.finra.herd.model.api.xml.ElasticsearchIndexKeys;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.ElasticsearchIndexService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles Elasticsearch index requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Elasticsearch Index")
public class ElasticsearchIndexRestController extends HerdBaseController
{
    public static final String ELASTICSEARCH_INDEXES_URI_PREFIX = "/elasticsearchIndexes";

    @Autowired
    private ElasticsearchIndexService elasticsearchIndexService;

    /**
     * Creates a new Elasticsearch index.
     *
     * @param request the information needed to create an Elasticsearch index
     *
     * @return the newly created Elasticsearch index
     */
    @RequestMapping(value = ELASTICSEARCH_INDEXES_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_ELASTICSEARCH_INDEXES_POST)
    public ElasticsearchIndex createElasticsearchIndexes(@RequestBody ElasticsearchIndexCreateRequest request)
    {
        return elasticsearchIndexService.createElasticsearchIndex(request);
    }

    /**
     * Deletes an existing Elasticsearch index by its name.
     *
     * @param elasticsearchIndexName the Elasticsearch index name
     *
     * @return the Elasticsearch index that got deleted
     */
    @RequestMapping(value = ELASTICSEARCH_INDEXES_URI_PREFIX + "/{elasticsearchIndexName}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_ELASTICSEARCH_INDEXES_DELETE)
    public ElasticsearchIndex deleteElasticsearchIndex(@PathVariable("elasticsearchIndexName") String elasticsearchIndexName)
    {
        return elasticsearchIndexService.deleteElasticsearchIndex(new ElasticsearchIndexKey(elasticsearchIndexName));
    }

    /**
     * Gets an existing Elasticsearch index by its name.
     *
     * @param elasticsearchIndexName the name of the Elasticsearch index
     *
     * @return the retrieved Elasticsearch index
     */
    @RequestMapping(value = ELASTICSEARCH_INDEXES_URI_PREFIX + "/{elasticsearchIndexName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_ELASTICSEARCH_INDEXES_GET)
    public ElasticsearchIndex getElasticsearchIndex(@PathVariable("elasticsearchIndexName") String elasticsearchIndexName)
    {
        return elasticsearchIndexService.getElasticsearchIndex(new ElasticsearchIndexKey(elasticsearchIndexName));
    }

    /**
     * Gets a list of Elasticsearch index keys for all Elasticsearch indexes defined in the system.
     *
     * @return the list of Elasticsearch index keys
     */
    @RequestMapping(value = ELASTICSEARCH_INDEXES_URI_PREFIX, method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_ELASTICSEARCH_INDEXES_ALL_GET)
    public ElasticsearchIndexKeys getElasticsearchIndexes()
    {
        return elasticsearchIndexService.getElasticsearchIndexes();
    }
}
