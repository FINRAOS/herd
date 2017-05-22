package org.finra.herd.dao.impl;

import static org.finra.herd.dao.helper.ElasticsearchUriBuilder.ElasticsearchFunction.SEARCH;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.RequestMethod;

import org.finra.herd.dao.SearchDao;
import org.finra.herd.dao.helper.ElasticsearchRestClientHelper;
import org.finra.herd.dao.helper.ElasticsearchUriBuilder;

/**
 * SearchDaoImpl contains methods to allow searching of a search index.
 */
@Repository
public class SearchDaoImpl implements SearchDao
{
    /**
     * The Elasticsearch rest client object used to connect to Elasticsearch to make process REST requests
     * This object will be dependency injected by Spring
     */
    private final ElasticsearchRestClientHelper elasticsearchRestClientHelper;

    /**
     * The SearchDaoImpl constructor used to autowire the elasticsearchRestClientHelper.
     *
     * @param elasticsearchRestClientHelper a helper class to connect to Elasticsearch to make process REST requests
     */
    @Autowired
    public SearchDaoImpl(ElasticsearchRestClientHelper elasticsearchRestClientHelper) {
        this.elasticsearchRestClientHelper = elasticsearchRestClientHelper;
    }

    @Override
    public String search(String index, String query) {
        // Build the endpoint string for the request
        final String endpoint = new ElasticsearchUriBuilder(index).elasticsearchFunction(SEARCH).toUriString();

        // Search the index
        return elasticsearchRestClientHelper.performRequest(RequestMethod.GET.name(), endpoint, Collections.emptyMap(), query);
    }
}