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
package org.finra.herd.dao.mock;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

/**
 * Mock Bucket implementation used for testing.
 */
public class MockBucket implements Terms.Bucket
{
    @Override
    public Object getKey()
    {
        return null;
    }

    @Override
    public String getKeyAsString()
    {
        return null;
    }

    @Override
    public long getDocCount()
    {
        return 0;
    }

    @Override
    public Aggregations getAggregations()
    {
        return null;
    }

    @Override
    public Number getKeyAsNumber()
    {
        return null;
    }

    @Override
    public long getDocCountError()
    {
        return 0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params)
    {
        return null;
    }
}