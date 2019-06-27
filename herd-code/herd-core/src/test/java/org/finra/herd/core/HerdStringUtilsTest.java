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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test driver for the {@link HerdStringUtils} class
 */
public class HerdStringUtilsTest extends AbstractCoreTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final String CSV_INJECTION_ERROR_MSG = "One or more schema column fields start with a prohibited character.";

    @Test
    public void testDecodeBase64()
    {
        // Test decode using hard coded values.
        assertEquals("UT_SomeText", HerdStringUtils.decodeBase64("VVRfU29tZVRleHQ="));

        // Test decode using random string and encoder.
        String encodedText = StringUtils.toEncodedString(Base64.getEncoder().encode(STRING_VALUE.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        assertEquals(STRING_VALUE, HerdStringUtils.decodeBase64(encodedText));
    }

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
    public void testStripHtmlDirtyFragmentWithEscapedHtml()
    {
        String result = HerdStringUtils.stripHtml("&lt;li&gt;fragment with escaped &lt;b&gt;html&lt;/b&gt;&lt;/li&gt;");
        assertEquals("fragment with escaped html", result);
    }

    @Test
    public void testStripHtmlDirtyFragmentWithEscapedHtmlAndUnrecognizedEntity()
    {
        String result = HerdStringUtils.stripHtml("&lt;li&gt;fragment with escaped &lt;b&gt;html&lt;/b&gt;&lt;/li&gt; & unrecognized entity &zzz;x");
        assertEquals("fragment with escaped html &amp; unrecognized entity &amp;zzz;x", result);
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

    @Test
    public void testCheckCsvInjectionStartsWithEqualsToCharacter()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(is(CSV_INJECTION_ERROR_MSG));

        HerdStringUtils.checkCsvInjection("=abc", CSV_INJECTION_ERROR_MSG);
    }

    @Test
    public void testCheckCsvInjectionStartsWithPlusCharacter()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(is(CSV_INJECTION_ERROR_MSG));

        HerdStringUtils.checkCsvInjection("+abc", CSV_INJECTION_ERROR_MSG);
    }

    @Test
    public void testCheckCsvInjectionStartsWithAtCharacter()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(is(CSV_INJECTION_ERROR_MSG));

        HerdStringUtils.checkCsvInjection("@abc", CSV_INJECTION_ERROR_MSG);
    }

    @Test
    public void testCheckCsvInjectionStartsWithMinusCharacter()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(is(CSV_INJECTION_ERROR_MSG));

        HerdStringUtils.checkCsvInjection("-abc", CSV_INJECTION_ERROR_MSG);
    }

    @Test
    public void testCheckCsvInjectionValidEmptyCharacter()
    {
        HerdStringUtils.checkCsvInjection("", CSV_INJECTION_ERROR_MSG);
    }

    @Test
    public void testCheckCsvInjectionValidNull()
    {
        HerdStringUtils.checkCsvInjection(null, CSV_INJECTION_ERROR_MSG);
    }

    @Test
    public void testCheckCsvInjectionValidStartsWithNormalCharacter()
    {
        HerdStringUtils.checkCsvInjection("hello", CSV_INJECTION_ERROR_MSG);
    }

    @Test
    public void testCheckCsvInjectionValidPlusCharacterInMiddle()
    {
        HerdStringUtils.checkCsvInjection("abc+def", CSV_INJECTION_ERROR_MSG);
    }

    @Test
    public void testCheckCsvInjectionValidStartsWithBlank()
    {
        HerdStringUtils.checkCsvInjection(" bc+def", CSV_INJECTION_ERROR_MSG);
    }
}
