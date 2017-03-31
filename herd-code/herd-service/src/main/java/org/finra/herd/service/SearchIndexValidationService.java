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

import org.finra.herd.model.api.xml.SearchIndexValidation;
import org.finra.herd.model.api.xml.SearchIndexValidationCreateRequest;

/**
 * The search index validation service.
 */
public interface SearchIndexValidationService
{
    /**
     * Validates the specified search index.
     *
     * @param request the information needed to validate a search index
     *
     * @return the validation response of the search index
     */
    public SearchIndexValidation createSearchIndexValidation(SearchIndexValidationCreateRequest request);

}
