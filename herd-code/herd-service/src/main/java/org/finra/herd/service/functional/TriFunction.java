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


/**
 * Represents a function that accepts three arguments and produces a result.
 *
 * @param <A> the type of the first argument to the function
 * @param <B> the type of the second argument to the function
 * @param <C> the type of the third argument to the function
 * @param <R> the type of the result of the function
 */
@FunctionalInterface
public interface TriFunction<A, B, C, R>
{
    /**
     * Applies this function to the given arguments.
     *
     * @param first the first argument to the operation
     * @param second the second argument to the operation
     * @param third the third argument to the operation
     * @return the function result
     */
    R apply(A first, B second, C third);
}