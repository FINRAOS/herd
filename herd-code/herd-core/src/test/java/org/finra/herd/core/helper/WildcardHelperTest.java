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
package org.finra.herd.core.helper;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class WildcardHelperTest
{
    private WildcardHelper wildcardHelper;

    @Before
    public void before()
    {
        wildcardHelper = new WildcardHelper();
    }

    @Test
    public void testMatchesAssertReturnTrueWhenMatches()
    {
        assertTrue(wildcardHelper.matches("ab", "*b"));
    }

    @Test
    public void testMatchesAssertReturnFalseWhenDoesNotMatch()
    {
        assertFalse(wildcardHelper.matches("ab", "*c"));
    }

    @Test
    public void testMatchesAssertDoesStringEqualWhenExpressionDoesNotStartWithWildcard()
    {
        assertTrue(wildcardHelper.matches("a*", "a*"));
        assertFalse(wildcardHelper.matches("ab", "a*"));
    }

    @Test
    public void testMatchesAssertDoesStringEqualWhenExpressionDoesNotContainWildcard()
    {
        assertFalse(wildcardHelper.matches("a*", "ab"));
        assertTrue(wildcardHelper.matches("ab", "ab"));
    }

    @Test
    public void testMatchesAssertWildcardOnlyMatchesAll()
    {
        assertTrue(wildcardHelper.matches("ab", "*"));
        assertTrue(wildcardHelper.matches("bc", "*"));
        assertTrue(wildcardHelper.matches("", "*"));
    }

    /**
     * Asserts that when the expression contains multiple consecutive wildcards as a prefix, only the first wildcard character would be considered a wildcard.
     * The subsequence wildcard characters are taken as-is.
     */
    @Test
    public void testMatchesAssertConsecutiveWildcardsOnlyMatchFirstWildcard()
    {
        assertFalse(wildcardHelper.matches("ab", "**"));
        assertTrue(wildcardHelper.matches("ab*", "**"));
    }
}
