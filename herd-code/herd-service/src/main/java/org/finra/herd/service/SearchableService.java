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

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.util.Assert;

/**
 * Service interface to support search operations.
 */
public interface SearchableService
{
    /**
     * Returns valid search response fields.
     *
     * @return the set of valid search response fields
     */
    public Set<String> getValidSearchResponseFields();

    /**
     * Validates a set of search response fields. This method also trims and lowers the fields.
     *
     * @param fields the search response fields to be validated
     */
    default void validateSearchResponseFields(Set<String> fields)
    {
        // Create a local copy of the fields set so that we can stream it to modify the fields set
        Set<String> localCopy = new HashSet<>(fields);

        // Clear the fields set
        fields.clear();

        // Add to the fields set field the strings both trimmed and lower cased and filter out empty and null strings
        localCopy.stream().filter(StringUtils::isNotBlank).map(String::trim).map(String::toLowerCase).forEachOrdered(fields::add);

        // Validate the field names
        fields.forEach(
            field -> Assert.isTrue(getValidSearchResponseFields().contains(field), String.format("Search response field \"%s\" is not supported.", field)));
    }
}
