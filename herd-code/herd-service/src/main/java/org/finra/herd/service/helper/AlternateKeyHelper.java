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
package org.finra.herd.service.helper;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/**
 * A helper class for alternate key related code.
 */
@Component
public class AlternateKeyHelper
{
    /**
     * Validates and returns a trimmed alternate key parameter value.
     *
     * @param parameterName the alternate key parameter name
     * @param parameterValue the alternate key parameter value
     *
     * @return the trimmed alternate key parameter value
     * @throws IllegalArgumentException if alternate key parameter is missing or not valid
     */
    public String validateStringParameter(String parameterName, String parameterValue) throws IllegalArgumentException
    {
        return validateStringParameter("A", parameterName, parameterValue);
    }

    /**
     * Validates and returns a trimmed alternate key parameter value.
     *
     * @param indefiniteArticle the indefinite article to use with the specified parameter name
     * @param parameterName the alternate key parameter name
     * @param parameterValue the alternate key parameter value
     *
     * @return the trimmed alternate key parameter value
     * @throws IllegalArgumentException if alternate key parameter is missing or not valid
     */
    public String validateStringParameter(String indefiniteArticle, String parameterName, String parameterValue) throws IllegalArgumentException
    {
        Assert.hasText(parameterValue, String.format("%s %s must be specified.", indefiniteArticle, parameterName));
        Assert.doesNotContain(parameterValue, "/", String.format("%s can not contain a forward slash character.", StringUtils.capitalize(parameterName)));
        return parameterValue.trim();
    }
}
