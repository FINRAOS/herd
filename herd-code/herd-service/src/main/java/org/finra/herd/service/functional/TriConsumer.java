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


import java.io.IOException;

/**
 * Represents an operation that accepts four input arguments and returns no result. This is a functional interface whose functional method is
 * accept(java.lang.Object, java.lang.Object, java.lang.Object).
 *
 * @param <A> the type of the first argument to the consumer
 * @param <B> the type of the second argument to the consumer
 * @param <C> the type of the third argument to the consumer
 */
@FunctionalInterface
public interface TriConsumer<A, B, C>
{
    /**
     * Performs this operation on the given arguments.
     *
     * @param first the first argument to the operation
     * @param second the second argument to the operation
     * @param third the third argument to the operation
     *
     * @throws IOException an IO exception
     */
    void accept(A first, B second, C third) throws IOException;
}