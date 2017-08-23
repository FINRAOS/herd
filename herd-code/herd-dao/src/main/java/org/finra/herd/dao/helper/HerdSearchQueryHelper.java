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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.IndexSearchRequest;

/**
 * Advanced query parsing for Herd using a simplistic DSL- this makes it easy for advanced users to fine-tune their search results.
 * <p>
 * <b>Negation</b>: User prefixes individual search terms with the '-' sign and the search engine excludes hits which have searchable
 * field(s) which contain one or more of those search terms.
 */

@Component
public class HerdSearchQueryHelper
{
    private Matcher getNegationTermMatcher(final IndexSearchRequest indexSearchRequest)
    {
        // Matches words with a leading space immediately followed by a '-' sign
        // eg. 'foo -bar' will match on the ' -bar'
        final Pattern regex = Pattern.compile("\\p{Space}(-)\\p{Alnum}+\\b");
        return regex.matcher(indexSearchRequest.getSearchTerm());
    }

    /**
     * Determines if negation terms are present in the search phrase.
     *
     * @param indexSearchRequest the {@link IndexSearchRequest} as specified
     *
     * @return true if present
     */
    public boolean determineNegationTermsPresent(final IndexSearchRequest indexSearchRequest)
    {
        final Matcher regexMatcher = getNegationTermMatcher(indexSearchRequest);
        return regexMatcher.find();
    }

    /**
     * Helper to extract negation terms from a given search query.
     *
     * @param indexSearchRequest the {@link IndexSearchRequest} as specified
     *
     * @return A {@link List} of type {@link String}
     */
    public List<String> extractNegationTerms(final IndexSearchRequest indexSearchRequest)
    {
        final Matcher regexMatcher = getNegationTermMatcher(indexSearchRequest);

        final List<String> negationTerms = new ArrayList<>();

        while (regexMatcher.find())
        {
            // extract all matches and return only the words
            negationTerms.add(regexMatcher.group().replaceAll("-", "").trim());
        }

        return negationTerms;
    }

    /**
     * Helper to clean the search phrase of negation terms.
     *
     * @param indexSearchRequest the {@link IndexSearchRequest} as specified
     *
     * @return A {@link String} without the negation terms
     */
    public String extractSearchPhrase(final IndexSearchRequest indexSearchRequest)
    {
        final Matcher regexMatcher = getNegationTermMatcher(indexSearchRequest);

        String searchPhrase = indexSearchRequest.getSearchTerm();

        while (regexMatcher.find())
        {
            // extract all matches and remove them from the original search phrase
            searchPhrase = searchPhrase.replace(regexMatcher.group(), StringUtils.EMPTY);
        }

        return searchPhrase.trim();
    }

}
