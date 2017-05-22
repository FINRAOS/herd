package org.finra.herd.dao;

/**
 * SearchDao is the interface for the search data access object implementation class.
 */
public interface SearchDao {
    /**
     * Method to used search an index.
     *
     * @param index the index to search
     * @param query the document JSON
     *
     * @return the response from the search call
     */
    String search(final String index, final String query);
}