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
package org.finra.herd.core;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Least recently used (LRU) cache backed by a linked hash map.
 */
public class LruCache<K, V> extends LinkedHashMap<K, V>
{
    /**
     * The number of entries at which an entry will be removed from the end of the linked hash map.
     */
    private int maxEntries;

    /**
     * The default access order boolean.
     * Access order of true orders the entries in the linked hash map by most recent access first.
     * Access order of false will order entries in the linked hash map in insertion order.
     */
    private static final boolean DEFAULT_ACCESS_ORDER = true;

    /**
     * The default initial capacity - MUST be a power of two.
     */
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The load factor is a measure of how full the hash table is allowed to get before its capacity is automatically increased.
     * As a general rule, the default load factor (.75) offers a good trade-off between time and space costs.
     */
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * Constructor for the LRU cache.
     *
     * @param maxEntries the maximum number of cache entries before an entry will be removed.
     */
    public LruCache(int maxEntries)
    {
        super(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_ACCESS_ORDER);
        this.maxEntries = maxEntries;
    }

    /**
     * Returns true if this cache should remove its eldest entry.
     *
     * @param eldest The least recently accessed entry in the map.
     *
     * @return True if the eldest entry should be removed
     * from the map. False if it should be retained.
     */
    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest)
    {
        return this.size() > maxEntries;
    }
}