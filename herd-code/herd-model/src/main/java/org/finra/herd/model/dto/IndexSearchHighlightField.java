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
 * Models the json config value an individual highlight field
 */
public class IndexSearchHighlightField
{
    private String fieldName;
    private Integer fragmentSize;
    private List<String> matchedFields;
    private Integer numOfFragments;

    /**
     * Default constructor
     */
    public IndexSearchHighlightField()
    {

    }

    /**
     * Fully-initializing value constructor
     *
     * @param fieldName the field name to highlight
     * @param fragmentSize the desired fragment size
     * @param matchedFields the list of matched_fields
     * @param numOfFragments the desired number of fragments
     */
    public IndexSearchHighlightField(String fieldName, Integer fragmentSize, List<String> matchedFields, Integer numOfFragments)
    {
        this.fieldName = fieldName;
        this.fragmentSize = fragmentSize;
        this.matchedFields = matchedFields;
        this.numOfFragments = numOfFragments;
    }

    public String getFieldName()
    {
        return fieldName;
    }

    public void setFieldName(String fieldName)
    {
        this.fieldName = fieldName;
    }

    public Integer getFragmentSize()
    {
        return fragmentSize;
    }

    public void setFragmentSize(Integer fragmentSize)
    {
        this.fragmentSize = fragmentSize;
    }

    public List<String> getMatchedFields()
    {
        return matchedFields;
    }

    public void setMatchedFields(List<String> matchedFields)
    {
        this.matchedFields = matchedFields;
    }

    public Integer getNumOfFragments()
    {
        return numOfFragments;
    }

    public void setNumOfFragments(Integer numOfFragments)
    {
        this.numOfFragments = numOfFragments;
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

        IndexSearchHighlightField that = (IndexSearchHighlightField) object;

        if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null)
        {
            return false;
        }
        if (fragmentSize != null ? !fragmentSize.equals(that.fragmentSize) : that.fragmentSize != null)
        {
            return false;
        }
        if (matchedFields != null ? !matchedFields.equals(that.matchedFields) : that.matchedFields != null)
        {
            return false;
        }
        return numOfFragments != null ? numOfFragments.equals(that.numOfFragments) : that.numOfFragments == null;
    }

    @Override
    public int hashCode()
    {
        int result = fieldName != null ? fieldName.hashCode() : 0;
        result = 31 * result + (fragmentSize != null ? fragmentSize.hashCode() : 0);
        result = 31 * result + (matchedFields != null ? matchedFields.hashCode() : 0);
        result = 31 * result + (numOfFragments != null ? numOfFragments.hashCode() : 0);
        return result;
    }
}
