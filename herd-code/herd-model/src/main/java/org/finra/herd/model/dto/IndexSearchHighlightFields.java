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
package org.finra.herd.model.dto;

import java.util.List;

/**
 * Models the json config value for highlight fields
 */
public class IndexSearchHighlightFields
{
    private List<IndexSearchHighlightField> highlightFields;

    /**
     * Fully-initializing value constructor
     *
     * @param highlightFields the list of highlight fields configurations
     */
    public IndexSearchHighlightFields(List<IndexSearchHighlightField> highlightFields)
    {
        this.highlightFields = highlightFields;
    }

    public List<IndexSearchHighlightField> getHighlightFields()
    {
        return highlightFields;
    }

    public void setHighlightFields(List<IndexSearchHighlightField> highlightFields)
    {
        this.highlightFields = highlightFields;
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (object == null || getClass() != object.getClass())
        {
            return false;
        }

        IndexSearchHighlightFields that = (IndexSearchHighlightFields) object;

        return highlightFields != null ? highlightFields.equals(that.highlightFields) : that.highlightFields == null;
    }

    @Override
    public int hashCode()
    {
        return highlightFields != null ? highlightFields.hashCode() : 0;
    }
}
