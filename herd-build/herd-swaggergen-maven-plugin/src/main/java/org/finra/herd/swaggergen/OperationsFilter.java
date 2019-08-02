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
package org.finra.herd.swaggergen;

import io.swagger.core.filter.AbstractSpecFilter;
import io.swagger.model.ApiDescription;
import io.swagger.models.Operation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Yaml filter to avoid all non GET operations for the resource
 * <p>
 * If allowedOperations list provided - only operations within this list will appear in final yaml spec
 **/
public class OperationsFilter extends AbstractSpecFilter
{
    private List<String> allowedOperations;

    /**
     * Constructs the model class finder.
     *
     * @param allowedOperations the list of operations to be included in the resulting yaml spec.
     */
    public OperationsFilter(String[] allowedOperations)
    {
        if (allowedOperations != null)
        {
            this.allowedOperations = Arrays.asList(allowedOperations);
        }
    }

    @Override
    public boolean isOperationAllowed(
            Operation operation,
            ApiDescription api,
            Map<String, List<String>> params,
            Map<String, String> cookies,
            Map<String, List<String>> headers)
    {
        return allowedOperations == null ||
                allowedOperations.size() == 0 ||
                allowedOperations.stream().anyMatch(op -> operation.getOperationId().equalsIgnoreCase(op))
                ;
    }
}
