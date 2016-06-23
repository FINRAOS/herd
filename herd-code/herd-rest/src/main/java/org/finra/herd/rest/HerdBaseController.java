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
package org.finra.herd.rest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.access.annotation.Secured;

/*
 * Base class for all herd controllers.
 */
// This ensures that any rest endpoint will be not be accessible unless annotated with proper function. 
@Secured("FN_NOT_ALLOWED")
public abstract class HerdBaseController
{
    protected HerdBaseController()
    {
        // Prevent classes from instantiating except sub-classes.
    }

    /**
     * Gets the given delimited field values as a list. If delimited field values is null, returns an empty list.
     *
     * @param delimitedFieldValues Delimited field values
     *
     * @return List of values
     */
    protected List<String> getList(DelimitedFieldValues delimitedFieldValues)
    {
        return delimitedFieldValues == null ? new ArrayList<>() : delimitedFieldValues.getValues();
    }

    /**
     * Validates that the query string parameters aren't duplicated for a list of expected parameters.
     *
     * @param parameterMap the query string parameter map.
     * @param parametersToCheck the query string parameters to check.
     *
     * @throws IllegalArgumentException if any duplicates were found.
     */
    public void validateNoDuplicateQueryStringParams(Map<String, String[]> parameterMap, String... parametersToCheck) throws IllegalArgumentException
    {
        List<String> parametersToCheckList = Arrays.asList(parametersToCheck);
        for (Map.Entry<String, String[]> mapEntry : parameterMap.entrySet())
        {
            if ((parametersToCheckList.contains(mapEntry.getKey())) && (mapEntry.getValue().length != 1))
            {
                throw new IllegalArgumentException("Found " + mapEntry.getValue().length + " occurrences of query string parameter \"" + mapEntry.getKey() +
                    "\", but 1 expected. Values found: \"" + StringUtils.join(mapEntry.getValue(), ", ") + "\".");
            }
        }
    }
}
