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
 * SearchFunctions
 */
public interface SearchFunctions
{
    QuadConsumer<String, String, String, String> getIndexFunction();

    QuadConsumer<String, String, String, String> getValidateFunction();

    QuadPredicate<String, String, String, String> getIsValidFunction();

    Predicate<String> getIndexExistsFunction();

    Consumer<String> getDeleteIndexFunction();

    TriConsumer<String, String, String> getCreateIndexFunction();

    TriConsumer<String, String, String> getDeleteDocumentByIdFunction();

    BiFunction<String, String, Long> getNumberOfTypesInIndexFunction();

    BiFunction<String, String, List<String>> getIdsInIndexFunction();
}
