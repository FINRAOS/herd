package org.finra.herd.dao.helper;

import static org.finra.herd.dao.helper.ElasticsearchUriBuilder.ElasticsearchFunction.NO_FUNCTION;

/**
 * ElasticsearchUriBuilder will build an Elasticsearch URI from the index, document type, document id, and Elasticsearch commands used.
 */
public final class ElasticsearchUriBuilder {
    /**
     * An enum to hold valid Elasticsearch functions
     */
    public enum ElasticsearchFunction {
        COUNT(preSlash(_COUNT)),
        MAPPING(preSlash(_MAPPING)),
        NO_FUNCTION(""),
        SEARCH(preSlash(_SEARCH));

        /**
         * The string name for the Elasticsearch function.
         */
        private String name;

        /**
         * The constructor for the enum values.
         *
         * @param name the name of the Elasticsearch function.
         */
        ElasticsearchFunction(String name) {
            this.name = name;
        }

        /**
         * Getter for the Elasticsearch function name.
         *
         * @return the name string
         */
        public String getName() {
            return name;
        }
    }

    /**
     * String used to obtain the count of documents for an Elasticsearch index
     */
    private static final String _COUNT = "_count";

    /**
     * String used to obtain the mapping for an Elasticsearch index
     */
    private static final String _MAPPING = "_mapping";

    /**
     * String used to search an Elasticsearch index
     */
    private static final String _SEARCH = "_search";

    /**
     * String used for pretty printing the result from Elasticsearch
     * The question mark version is used starting a parameter list
     */
    private static final String PRETTY = "?pretty";

    /**
     * Slash string
     */
    private static final String SLASH = "/";

    /**
     * The Elasticsearch document type, default to an empty string.
     */
    private String documentType = "";

    /**
     * The Elasticsearch function enumeration, default to no function.
     */
    private ElasticsearchFunction elasticsearchFunction = NO_FUNCTION;

    /**
     * The Elasticsearch id, default to an empty string.
     */
    private String id = "";

    /**
     * The Elasticsearch index, this is a required variable.
     */
    private final String index;

    /**
     * The pretty print string, default to an empty string.
     */
    private String pretty = "";


    /**
     * The constructor for the Elasticsearch URI builder, the index string is required.
     *
     * @param index the index string.
     */
    public ElasticsearchUriBuilder(final String index) {
        this.index = preSlash(index);
    }

    /**
     * Method to add the document type to this builder.
     *
     * @param documentType the document type string
     *
     * @return this builder
     */
    public ElasticsearchUriBuilder documentType(final String documentType) {
        this.documentType = preSlash(documentType);
        return this;
    }

    /**
     * Method to add the id to this builder.
     *
     * @param id the id string
     *
     * @return this builder
     */
    public ElasticsearchUriBuilder id(final String id) {
        this.id = preSlash(id);
        return this;
    }

    /**
     * Method to add an Elasticsearch function to this builder.
     *
     * @param elasticsearchFunction the Elasticsearch function.
     *
     * @return this builder.
     */
    public ElasticsearchUriBuilder elasticsearchFunction(final ElasticsearchFunction elasticsearchFunction) {
        this.elasticsearchFunction = elasticsearchFunction;
        return this;
    }

    /**
     * If isPretty is true then add ?pretty to the end of the URI.
     *
     * @param isPretty the pretty boolean flag
     *
     * @return the ElasticsearchUriBuilder
     */
    public ElasticsearchUriBuilder isPretty(final boolean isPretty) {
        // If the pretty boolean is true than set the pretty string, otherwise leave it empty.
        pretty = isPretty ? preSlash(PRETTY) : "";

        return this;
    }

    /**
     * Method to return the URI String.
     *
     * @return the URI String.
     */
    public String toUriString() {
        return index + elasticsearchFunction.getName() + documentType + id + pretty;
    }

    /**
     * Private static method to prepend the "/" to the start of the path string.
     *
     * @param path the path string
     *
     * @return the path string with Slash added to the start.
     */
    private static String preSlash(final String path) {
        return SLASH + path;
    }
}