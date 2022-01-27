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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
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
    // Hidden text content
    public static final String HIDDEN_TEXT = "hidden";
    // Regex to check json password pattern, password wrapped in " or "\", like "jdbc.password":"password1"
    private static Pattern REGEX_JSON_PASSWORD =
        Pattern.compile("(\\\\?\"?password\\\\?\":\\\\?\")[\\w\\p{Punct}&&[^&]]*?(\\\\?\")", Pattern.CASE_INSENSITIVE);
    // Regex to check json password pattern, like "name": "password", "value": "password1"
    private static Pattern REGEX_JSON_PASSWORD2 =
        Pattern.compile("(\"name\": \".*?password\", \"value\": \")[\\w\\p{Punct}&&[^&]]*?\"", Pattern.CASE_INSENSITIVE);
    // Regex to check xml password pattern, like "<password>password1</password>
    private static Pattern REGEX_XML_PASSWORD = Pattern.compile("(<.*?password>)[\\w\\p{Punct}&&[^&]]*?<", Pattern.CASE_INSENSITIVE);

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
     * This method will minimize a file path by removing the first appearance of the directory path string.
     *
     * @param filePath the file path to minimize
     * @param directoryPath the directory path to remove
     *
     * @return the minimized file path
     */
    public static String getMinimizedFilePath(final String filePath, final String directoryPath)
    {
        // If not already there, append slash to directory path, since it represents a directory.
        String directoryPathWithTrailingSlash = StringUtils.appendIfMissing(directoryPath, "/");

        // If the file path starts with the directory path then minimize the file path.
        return filePath.startsWith(directoryPathWithTrailingSlash) ? filePath.substring(directoryPathWithTrailingSlash.length()) : filePath;
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
        return StringUtils.left(stripHtml(toParse), shortDescMaxLength);
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
        // Unescape HTML.
        String unEscapedFragment = StringEscapeUtils.unescapeHtml4(fragment);

        // Parse out html tags except those from a given list of whitelist tags
        Document dirty = Jsoup.parseBodyFragment(unEscapedFragment);

        Whitelist whitelist = new Whitelist();

        for (String whitelistTag : whitelistTags)
        {
            // Get the actual tag name from the whitelist tag
            // this is vulnerable in general to complex tags but will suffice for our simple needs
            whitelistTag = RegExUtils.removePattern(whitelistTag, "[^\\{IsAlphabetic}]");

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
     * @param errorMessage the error message in the exception when CVS Injection check fails
     */
    public static void checkCsvInjection(String text, String errorMessage)
    {
        if (StringUtils.isNotEmpty(text) && text.matches(CSV_INJECTION_REGEX))
        {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * Convert a <code>String</code> to an <code>Integer</code>, returning a default value if the string is blank.</p>
     *
     * @param stringValue the string to convert, may be null
     * @param defaultValue the default value
     *
     * @return the Integer represented by the string, or the default if the string is blank
     */
    public static Integer convertStringToInteger(final String stringValue, final Integer defaultValue)
    {
        if (StringUtils.isNotBlank(stringValue))
        {
            try
            {
                return Integer.parseInt(stringValue);
            }
            catch (final NumberFormatException e)
            {
                throw new IllegalArgumentException(String.format("Failed to convert \"%s\" value to %s.", stringValue, Integer.class.getName()), e);
            }
        }
        else
        {
            return defaultValue;
        }
    }

    /**
     * Sanitize log text by replacing the password
     *
     * @param loggingText logging text
     * @return sanitized text
     */
    public static String sanitizeLogText(String loggingText)
    {
        String sanitizedText = (loggingText != null) ? loggingText : "";
        sanitizedText = sanitizedText.replaceAll("&quot;", "\"");
        sanitizedText = REGEX_JSON_PASSWORD.matcher(sanitizedText).replaceAll("$1" + HIDDEN_TEXT + "$2");
        sanitizedText = REGEX_JSON_PASSWORD2.matcher(sanitizedText).replaceAll("$1" + HIDDEN_TEXT + "\"");
        sanitizedText = REGEX_XML_PASSWORD.matcher(sanitizedText).replaceAll("$1" + HIDDEN_TEXT + "<");

        return sanitizedText;
    }

    /**
     * Determines if a String conforms to Herd's versioning scheme
     *
     * @param versionString The given input string
     * @return boolean true if the input conforms, false otherwise
     */
    public static boolean verifyHerdVersionConformingString(String versionString)
    {
        if (StringUtils.isEmpty(versionString))
        {
            return false;
        }

        Pattern semverPattern = Pattern.compile("\\d+\\.\\d+\\.\\d+(?:-SNAPSHOT)?");
        Matcher semverPatternMatcher = semverPattern.matcher(versionString);

        return semverPatternMatcher.matches();
    }

    /**
     * Get first level prefix from the relative path. The path is assumed not to start from the "/" character.
     *
     * @param prefix the given prefix
     *
     * @return if exists, first level prefix including trailing slash, path value as it was specified otherwise
     */
    public static String getFirstLevelPrefix(String prefix)
    {
        String firstLevelPrefix;

        int firstOccurrenceOfSlash = StringUtils.indexOf(prefix, "/");

        if (firstOccurrenceOfSlash != StringUtils.INDEX_NOT_FOUND)
        {
            firstLevelPrefix = prefix.substring(0, firstOccurrenceOfSlash + 1);
        }
        else
        {
            firstLevelPrefix = prefix;
        }

        return firstLevelPrefix;
    }
}
