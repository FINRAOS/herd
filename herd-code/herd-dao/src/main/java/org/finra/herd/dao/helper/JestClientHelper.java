package org.finra.herd.dao.helper;

import java.io.IOException;

import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.JestClientFactory;

/**
 * JestClientHelper
 */
@Component
public class JestClientHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JestClientHelper.class);

    @Autowired
    private JestClientFactory jestClientFactory;

    /**
     * Method to use the JEST client to search against Elasticsearch.
     *
     * @param search JEST Search object
     *
     * @return a search result
     */
    public SearchResult searchExecute(final Search search)
    {
        final SearchResult searchResult;
        try
        {
            searchResult = jestClientFactory.getJestClient().execute(search);
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Failed to execute JEST client search.", ioException);
            throw new RuntimeException(ioException);
        }

        return searchResult;
    }

}
