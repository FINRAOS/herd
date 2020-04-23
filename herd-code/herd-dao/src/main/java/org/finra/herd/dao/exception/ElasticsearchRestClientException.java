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
package org.finra.herd.dao.exception;

/**
 * ElasticsearchRestClientException used when the Elasticsearch REST client fails
 */
public class ElasticsearchRestClientException extends RuntimeException
{
    /**
     * Default constructor
     */
    public ElasticsearchRestClientException()
    {
        // Default constructor
        super();
    }

    /**
     * Constructor that accepts a message
     *
     * @param message the exception message
     */
    public ElasticsearchRestClientException(String message)
    {
        // Pass the message to the Exception class
        super(message);
    }

    /**
     * Constructor that accepts an exception
     *
     * @param exception the exception
     */
    public ElasticsearchRestClientException(Exception exception)
    {
        // Pass the exception to the Exception class
        super(exception);
    }
}
