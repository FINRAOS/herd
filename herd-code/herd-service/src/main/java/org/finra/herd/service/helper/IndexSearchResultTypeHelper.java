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

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.IndexSearchResultTypeKey;

@Component
public class IndexSearchResultTypeHelper
{
    private final AlternateKeyHelper alternateKeyHelper;

    private static final String TAG = "tag";

    private static final String BUS_OBJCT_DFNTN = "bdef";

    @Autowired
    public IndexSearchResultTypeHelper(AlternateKeyHelper alternateKeyHelper)
    {
        this.alternateKeyHelper = alternateKeyHelper;
    }

    /**
     * Validates an index search result type key. This method also trims the key parameters.
     *
     * @param resultTypeKey the specified index search result type key
     */
    public void validateIndexSearchResultTypeKey(IndexSearchResultTypeKey resultTypeKey)
    {
        Assert.notNull(resultTypeKey, "An index search result type key must be specified.");
        resultTypeKey
            .setIndexSearchResultType(alternateKeyHelper.validateStringParameter("An", "index search result type", resultTypeKey.getIndexSearchResultType()));

        Assert.isTrue(getValidResultTypes().contains(resultTypeKey.getIndexSearchResultType().toLowerCase()),
            String.format("Invalid index search result type: \"%s\"", resultTypeKey.getIndexSearchResultType()));
    }

    private List getValidResultTypes()
    {
        return ImmutableList.of(TAG, BUS_OBJCT_DFNTN);
    }
}
