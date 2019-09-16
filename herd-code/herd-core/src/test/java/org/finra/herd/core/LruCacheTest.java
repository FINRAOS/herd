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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

/**
 * Test for the {@link LruCache} class
 */
public class LruCacheTest extends AbstractCoreTest
{
    private static final int MAXIMUM_CACHE_SIZE = 10;

    private static final int LEAST_RECENTLY_USED_CACHE_ENTRY = 5;

    /**
     * Unit test to test the least recently used (LRU) cache.
     * This test will fill the LRU cache, access all entries except one, then add another entry.
     * The least recently used entry will be removed when the maximum cache size is exceeded.
     */
    @Test
    public void testLruCache()
    {
        // Initialize the cache
        Map<String, String> lruCache = Collections.synchronizedMap(new LruCache<>(MAXIMUM_CACHE_SIZE));


        // Add entities to the cache
        for (int i = 0; i < MAXIMUM_CACHE_SIZE; i++)
        {
            lruCache.put("KEY" + i, "VALUE" + i);
        }

        assertThat("LRU Cache not the correct size.", lruCache.size(), is(equalTo(MAXIMUM_CACHE_SIZE)));

        // Access all entries except LEAST_RECENTLY_USED_CACHE_ENTRY
        for (int i = 0; i < MAXIMUM_CACHE_SIZE; i++)
        {
            if (i != LEAST_RECENTLY_USED_CACHE_ENTRY)
            {
                String value = lruCache.get("KEY" + i);
                assertThat("Value is not correct.", value, is(equalTo("VALUE" + i)));
            }
        }

        // Add another entry to the cache
        // This will increase the cache size such that an entry in the cache must be deleted.
        // The least recently used (LRU) entry should be removed during this put operation.
        lruCache.put("KEY" + MAXIMUM_CACHE_SIZE, "VALUE" + MAXIMUM_CACHE_SIZE);

        assertThat("LRU Cache not the correct size.", lruCache.size(), is(equalTo(MAXIMUM_CACHE_SIZE)));

        // Access all entries and verify that the least recently used entry has been removed.
        for (int i = 0; i <= MAXIMUM_CACHE_SIZE; i++)
        {
            if (i != LEAST_RECENTLY_USED_CACHE_ENTRY)
            {
                String value = lruCache.get("KEY" + i);
                assertThat("Value is not correct.", value, is(equalTo("VALUE" + i)));
            }
            else
            {
                String leastRecentlyUsedValue = lruCache.get("KEY" + LEAST_RECENTLY_USED_CACHE_ENTRY);
                assertThat("Least recently used entry should be removed.", leastRecentlyUsedValue, is(nullValue()));
            }
        }
    }


}
