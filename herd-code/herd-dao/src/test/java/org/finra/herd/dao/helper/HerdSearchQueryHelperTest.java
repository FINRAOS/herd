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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.model.api.xml.IndexSearchRequest;

public class HerdSearchQueryHelperTest extends AbstractDaoTest
{
    @Autowired
    private HerdSearchQueryHelper herdSearchQueryHelper;

    @Test
    public void determineNegationTermsPresent() throws Exception
    {
        IndexSearchRequest indexSearchRequest = new IndexSearchRequest();

        indexSearchRequest.setSearchTerm(" -foo");
        Assert.assertTrue(herdSearchQueryHelper.determineNegationTermsPresent(indexSearchRequest));

        indexSearchRequest.setSearchTerm("foo -bar foobar");
        Assert.assertTrue(herdSearchQueryHelper.determineNegationTermsPresent(indexSearchRequest));

        indexSearchRequest.setSearchTerm(" ");
        Assert.assertFalse(herdSearchQueryHelper.determineNegationTermsPresent(indexSearchRequest));

        indexSearchRequest.setSearchTerm("-bar");
        Assert.assertFalse(herdSearchQueryHelper.determineNegationTermsPresent(indexSearchRequest));

        indexSearchRequest.setSearchTerm("foo bar");
        Assert.assertFalse(herdSearchQueryHelper.determineNegationTermsPresent(indexSearchRequest));

        indexSearchRequest.setSearchTerm("foo-bar");
        Assert.assertFalse(herdSearchQueryHelper.determineNegationTermsPresent(indexSearchRequest));
    }

    @Test
    public void extractNegationTerms() throws Exception
    {
        IndexSearchRequest indexSearchRequest = new IndexSearchRequest();

        indexSearchRequest.setSearchTerm("foo -foo bar -bar foobar");
        Assert.assertEquals(Arrays.asList("foo", "bar"), herdSearchQueryHelper.extractNegationTerms(indexSearchRequest));

        indexSearchRequest.setSearchTerm("foo -foo bar -bar foobar");
        Assert.assertEquals(Arrays.asList("foo", "bar"), herdSearchQueryHelper.extractNegationTerms(indexSearchRequest));
    }

    @Test
    public void extractSearchPhrase() throws Exception
    {
        IndexSearchRequest indexSearchRequest = new IndexSearchRequest();

        indexSearchRequest.setSearchTerm("foo -foo bar -bar foobar");
        Assert.assertEquals("foo bar foobar", herdSearchQueryHelper.extractSearchPhrase(indexSearchRequest));
    }

}
