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

import java.util.Set;

import org.finra.herd.model.api.xml.IndexSearchRequest;
import org.finra.herd.model.api.xml.IndexSearchResponse;

/**
 * IndexSearchService
 */
public interface IndexSearchService
{
    /**
     * The index indexSearch method will take as parameters an index indexSearch request that contains a indexSearch term string, and a set of field strings.
     * It will perform an index indexSearch on the indexSearch term and return the fields specified in the index indexSearch response.
     *
     * @param request the index indexSearch request that contains a indexSearch term string
     * @param fields the set of fields that are to be returned in the index indexSearch response
     *
     * @return an index indexSearch response object containing the total index indexSearch results and index indexSearch results
     */
    IndexSearchResponse indexSearch(final IndexSearchRequest request, final Set<String> fields);
}
