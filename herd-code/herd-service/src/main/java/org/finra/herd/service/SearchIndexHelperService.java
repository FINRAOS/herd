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

import java.util.concurrent.Future;

import org.finra.herd.model.api.xml.SearchIndexKey;

/**
 * The helper service class for the search index service.
 */
public interface SearchIndexHelperService
{
    /**
     * Indexes all business object definitions defined in the system.
     *
     * @param searchIndexKey the key of the search index
     *
     * @return result of an asynchronous computation
     */
    public Future<Void> indexAllBusinessObjectDefinitions(SearchIndexKey searchIndexKey);

    /**
     * Indexes all tags defined in the system.
     *
     * @param searchIndexKey the key of the search index
     *
     * @return result of an asynchronous computation
     */
    public Future<Void> indexAllTags(SearchIndexKey searchIndexKey);
}
