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
package org.finra.dm.dao.helper;

import java.util.List;

import org.springframework.stereotype.Component;

/**
 * Helper for collections.
 * TODO add getCollectionSize(), a null-safe size() operation
 * TODO add getFirstElement(), a null-safe iterator().next() operation
 */
@Component
public class DmCollectionHelper
{
    /**
     * Null-safe and index-safe list get. The method returns null if the list is null or the index is out of bounds. Returns element at index otherwise.
     * 
     * @param <T> the type of list element
     * @param list {@link List} of T
     * @param index index of element
     * @return element or null
     */
    public <T> T safeGet(List<T> list, int index)
    {
        if (list != null && index < list.size())
        {
            return list.get(index);
        }
        return null;
    }
}
