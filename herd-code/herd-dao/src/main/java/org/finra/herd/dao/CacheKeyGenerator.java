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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.cache.interceptor.KeyGenerator;

/*
 * Custom key generator that includes the class, method name and all parameters.
 */
public class CacheKeyGenerator implements KeyGenerator
{
    @Override
    public Object generate(final Object target, final Method method, final Object... params)
    {
        final List<Object> keys = new ArrayList<>();
        keys.add(method.getDeclaringClass().getName());
        keys.add(method.getName());
        Collections.addAll(keys, params);
        return keys;
    }
}
