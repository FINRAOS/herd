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

import static org.junit.Assert.assertEquals;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

/**
 * Test driver for the {@link HerdStringUtils} class
 */
public class HerdStringUtilsTest
{
    @Test
    public void testGetShortDescription()
    {
        String longString = RandomStringUtils.randomAlphabetic(500);
        String result = HerdStringUtils.getShortDescription(longString, 10);

        assertEquals(result.length(), 10);
    }

    @Test
    public void testStripHtmlDirtyFragment()
    {
        String result = HerdStringUtils.stripHtml("<li>fragment with <b>html</b></li>");
        assertEquals("fragment with html", result);
    }

    @Test
    public void testStripHtmlCleanFragment()
    {
        String result = HerdStringUtils.stripHtml("fragment\nwith no html");
        assertEquals("fragment\nwith no html", result);
    }

    @Test
    public void testStripHtmlDirtyFragmentWithPartialTags()
    {
        String result = HerdStringUtils.stripHtml("fragment<li><b> with no</b> html<l");
        assertEquals("fragment with no html", result);
    }

    @Test
    public void testStripHtmlDirtyFragmentWithWhitelist()
    {
        String result = HerdStringUtils.stripHtml("fragment<li><b> with <hlt>no</hlt></b> html</li>", "<hlt>");
        assertEquals("fragment with <hlt>no</hlt> html", result);
    }

    @Test
    public void testStripHtmlDirtyFragmentWithWhitelistWithStyle()
    {
        String result = HerdStringUtils.stripHtml("fragment<li><b> with <hlt class=\"highlight\">no</hlt></b> html</li>", "<hlt>");
        assertEquals("fragment with <hlt class=\"highlight\">no</hlt> html", result);
    }

    @Test
    public void testStripHtmlDirtyFragmentWithMultipleWhitelistTags()
    {
        String result = HerdStringUtils.stripHtml("fragment<li><b> with <hlt>no</hlt></b> html</li>", "<hlt>", "<b>");
        assertEquals("fragment<b> with <hlt>no</hlt></b> html", result);
    }
}
