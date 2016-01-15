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
package org.finra.herd.dao.helper;

import java.util.Arrays;

import org.finra.herd.dao.AbstractDaoTest;
import org.junit.Assert;
import org.junit.Test;

public class HerdCollectionHelperTest extends AbstractDaoTest
{
    @Test
    public void testSafeGetNotNullElement()
    {
        String result = herdCollectionHelper.safeGet(Arrays.asList("A", "B", "C"), 2);
        Assert.assertEquals("result", "C", result);
    }

    @Test
    public void testSafeGetNullList()
    {
        String result = herdCollectionHelper.safeGet(null, 2);
        Assert.assertNull("result is not null", result);
    }

    @Test
    public void testSafeGetOutOfBounds()
    {
        String result = herdCollectionHelper.safeGet(Arrays.asList("A", "B", "C"), 3);
        Assert.assertNull("result is not null", result);
    }
}
