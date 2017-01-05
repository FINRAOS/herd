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
package org.finra.herd.service.functional;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * SearchFunctions interface used to provide search index functionality
 */
public interface SearchFunctions
{
    /**
     * The index function will take as arguments indexName, documentType, id, json and add the document to the index.
     */
    QuadConsumer<String, String, String, String> getIndexFunction();

    /**
     * The validate function will take as arguments indexName, documentType, id, json and validate the document against the index.
     */
    QuadConsumer<String, String, String, String> getValidateFunction();

    /**
     * The isValid function will take as arguments indexName, documentType, id, json and validate the document against the index and return true if the document
     * is valid and false otherwise.
     */
    QuadPredicate<String, String, String, String> getIsValidFunction();

    /**
     * The index exists predicate will take as an argument the index name and will return tree if the index exists and false otherwise.
     */
    Predicate<String> getIndexExistsFunction();

    /**
     * The delete index function will take as an argument the index name and will delete the index.
     */
    Consumer<String> getDeleteIndexFunction();

    /**
     * The create index function will take as arguments the index name, document type, and mapping and will create a new index.
     */
    TriConsumer<String, String, String> getCreateIndexFunction();

    /**
     * The delete document by id function will delete a document in the index by the document id.
     */
    TriConsumer<String, String, String> getDeleteDocumentByIdFunction();

    /**
     * The number of types in index function will take as arguments the index name and the document type and will return the number of documents in the index.
     */
    BiFunction<String, String, Long> getNumberOfTypesInIndexFunction();

    /**
     * The ids in index function will take as arguments the index name and the document type and will return a list of all the ids in the index.
     */
    BiFunction<String, String, List<String>> getIdsInIndexFunction();
}
