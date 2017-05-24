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

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.AbstractDaoTest;

/**
 * This class tests functionality within the UrlHelper class.
 */
public class UrlHelperTest extends AbstractDaoTest
{
    @Autowired
    private UrlHelper urlHelper;

    @Test
    public void testParseJsonObjectFromUrl()
    {
        // Try to read JSON object from an invalid URL.
        try
        {
            urlHelper.parseJsonObjectFromUrl(I_DO_NOT_EXIST);
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Failed to read JSON from the URL: url=\"%s\"", I_DO_NOT_EXIST), e.getMessage());
        }
    }
}
