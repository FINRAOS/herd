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

import static org.junit.Assert.fail;

import org.junit.Assert;
import org.junit.Test;

import org.finra.herd.model.api.xml.IndexSearchResultTypeKey;
import org.finra.herd.service.AbstractServiceTest;

public class IndexSearchResultTypeHelperTest extends AbstractServiceTest
{
    @Test
    public void testValidIndexSearchResultTypeKey()
    {
        final String validTypeBdef = "bdef";
        final String validTypeTag = "tag";

        // Create a key with result type as bdef
        IndexSearchResultTypeKey indexSearchResultTypeKeyBdef = new IndexSearchResultTypeKey(validTypeBdef);
        indexSearchResultTypeHelper.validateIndexSearchResultTypeKey(indexSearchResultTypeKeyBdef);

        // Create a key with result type as tag
        IndexSearchResultTypeKey indexSearchResultTypeKeyTag = new IndexSearchResultTypeKey(validTypeTag);
        indexSearchResultTypeHelper.validateIndexSearchResultTypeKey(indexSearchResultTypeKeyTag);

        // Test passes if no exceptions were thrown
    }

    @Test
    public void testNullIndexSearchResultTypeKey()
    {
        try
        {
            indexSearchResultTypeHelper.validateIndexSearchResultTypeKey(null);
            fail();
        }
        catch (Exception e)
        {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("An index search result type key must be specified.", e.getMessage());
        }
    }

    @Test
    public void testInvalidIndexSearchResultTypeKey()
    {
        final String invalidType = "invalidType";

        // Create a key with result type as bdef
        IndexSearchResultTypeKey indexSearchResultTypeKeyBdef = new IndexSearchResultTypeKey(invalidType);

        try
        {
            indexSearchResultTypeHelper.validateIndexSearchResultTypeKey(indexSearchResultTypeKeyBdef);
            fail();
        }
        catch (Exception e)
        {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
            Assert.assertEquals(String.format("Invalid index search result type: \"%s\"", invalidType), e.getMessage());
        }

    }

}
