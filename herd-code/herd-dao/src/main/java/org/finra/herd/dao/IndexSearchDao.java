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
package org.finra.herd.dao;

import java.util.Set;

import org.finra.herd.model.api.xml.IndexSearchRequest;
import org.finra.herd.model.api.xml.IndexSearchResponse;

public interface IndexSearchDao
{
    /**
     * The index search method will accept an index search request as a parameter and will return an index search result. The result will be an index search
     * based on the search term contained in the index search request.
     *
     * @param request the index search request containing the search term
     * @param fields the set of fields that are to be returned in the index indexSearch response
     * @param match the set of match fields that are to be searched upon in the index search
     * @param bdefActiveIndex the name of the active index for business object definitions
     * @param tagActiveIndex the name os the active index for tags
     *
     * @return the index search response containing the search results
     */
    IndexSearchResponse indexSearch(final IndexSearchRequest request, final Set<String> fields, final Set<String> match, final String bdefActiveIndex,
        final String tagActiveIndex);
}
