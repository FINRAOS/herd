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

import java.util.Objects;

import org.springframework.stereotype.Component;

/**
 * A helper for wildcard expressions.
 */
@Component
public class WildcardHelper
{
    /**
     * The token to use as a wildcard character.
     */
    public static final String WILDCARD_TOKEN = "*";

    /**
     * Returns whether the given string matches the given expression. The expression may contain a wildcard character, but it must appear as the first character
     * of the expression for it to be interpreted as such. If a wildcard character appears anywhere else, or the wildcard character does not exist, the
     * expression must match the string as-is for this method to return true.
     * 
     * @param string The string to match
     * @param expression The wildcard expression
     * @return True if the string matches the expression, false otherwise
     */
    public boolean matches(String string, String expression)
    {
        if (expression.startsWith(WILDCARD_TOKEN))
        {
            return string.endsWith(expression.substring(WILDCARD_TOKEN.length()));
        }
        return Objects.equals(expression, string);
    }
}
