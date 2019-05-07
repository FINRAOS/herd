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

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Entities;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Whitelist;

/**
 * HerdStringUtils
 */
public class HerdStringUtils
{
    // Regex to check for CSV Injection. Any text that starts with "+", "=", "@", or "-" will be vulnerable to CSV Injection attack.
    private static final String CSV_INJECTION_REGEX = "^[+=@-].*";

    /**
     * Decodes and return the base64 encoded string.
     *
     * @param base64EncodedText the base64 encoded string
     *
     * @return the decoded string
     */
    public static String decodeBase64(String base64EncodedText)
    {
        return StringUtils.toEncodedString(Base64.getDecoder().decode(base64EncodedText.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    /**
     * Truncates the description field to a configurable value thereby producing a 'short description'
     *
     * @param description the specified description
     * @param shortDescMaxLength the short description maximum length
     *
     * @return truncated (short) description
     */
    public static String getShortDescription(String description, Integer shortDescMaxLength)
    {
        // Parse out only html tags, truncate and return
        // Do a partial HTML parse just in case there are some elements that don't have ending tags or the like
        String toParse = description != null ? description : "";
        return StringUtils.left(Jsoup.parseBodyFragment(toParse).body().text(), shortDescMaxLength);
    }

    /**
     * Strips HTML tags from a given input String, allows some tags to be retained via a whitelist
     *
     * @param fragment the specified String
     * @param whitelistTags the specified whitelist tags
     *
     * @return cleaned String with allowed tags
     */
    public static String stripHtml(String fragment, String... whitelistTags)
    {

        // Parse out html tags except those from a given list of whitelist tags
        Document dirty = Jsoup.parseBodyFragment(fragment);

        Whitelist whitelist = new Whitelist();

        for (String whitelistTag : whitelistTags)
        {
            // Get the actual tag name from the whitelist tag
            // this is vulnerable in general to complex tags but will suffice for our simple needs
            whitelistTag = StringUtils.removePattern(whitelistTag, "[^\\{IsAlphabetic}]");

            // Add all specified tags to the whitelist while preserving inline css
            whitelist.addTags(whitelistTag).addAttributes(whitelistTag, "class");
        }

        Cleaner cleaner = new Cleaner(whitelist);
        Document clean = cleaner.clean(dirty);
        // Set character encoding to UTF-8 and make sure no line-breaks are added
        clean.outputSettings().escapeMode(Entities.EscapeMode.base).charset(StandardCharsets.UTF_8).prettyPrint(false);

        // return 'cleaned' html body
        return clean.body().html();
    }

    /**
     * Check if the text is vulnerable to CSV Injection attack.
     *
     * @param text the text
     */
    public static void checkCsvInjection(String text)
    {
        if (StringUtils.isNotEmpty(text) && text.matches(CSV_INJECTION_REGEX))
        {
            throw new IllegalArgumentException("One or more schema column fields start with a prohibited character");
        }
    }
}
