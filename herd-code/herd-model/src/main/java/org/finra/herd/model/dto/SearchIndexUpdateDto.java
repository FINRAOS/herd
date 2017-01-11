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
 * Data transfer object used to notify a message listener that a document in the elastic search index needs to be modified.
 */

public class SearchIndexUpdateDto
{
    /**
     * String used to signal a create of a new index document.
     */
    public static final String SEARCH_INDEX_UPDATE_TYPE_CREATE = "CREATE";

    /**
     * String used to signal a update of an index document.
     */
    public static final String SEARCH_INDEX_UPDATE_TYPE_UPDATE = "UPDATE";

    /**
     * String used to signal a delete of an index document.
     */
    public static final String SEARCH_INDEX_UPDATE_TYPE_DELETE = "DELETE";

    /**
     * A list of business object definition ids that will be modified in the index.
     */
    private List<Integer> businessObjectDefinitionIds;

    /**
     * The type of modification that will be processed. INSERT, UPDATE, DELETE
     */
    private String modificationType;

    /**
     * Default constructor required for JSON object mapping
     */
    public SearchIndexUpdateDto()
    {
        // Empty constructor
    }

    public SearchIndexUpdateDto(List<Integer> businessObjectDefinitionIds, String modificationType)
    {
        this.businessObjectDefinitionIds = businessObjectDefinitionIds;
        this.modificationType = modificationType;
    }

    public List<Integer> getBusinessObjectDefinitionIds()
    {
        return businessObjectDefinitionIds;
    }

    public void setBusinessObjectDefinitionIds(List<Integer> businessObjectDefinitionIds)
    {
        this.businessObjectDefinitionIds = businessObjectDefinitionIds;
    }

    public String getModificationType()
    {
        return modificationType;
    }

    public void setModificationType(String modificationType)
    {
        this.modificationType = modificationType;
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

        SearchIndexUpdateDto that = (SearchIndexUpdateDto) object;

        if (businessObjectDefinitionIds != null ? !businessObjectDefinitionIds.equals(that.businessObjectDefinitionIds) :
            that.businessObjectDefinitionIds != null)
        {
            return false;
        }
        return modificationType != null ? modificationType.equals(that.modificationType) : that.modificationType == null;

    }

    @Override
    public int hashCode()
    {
        int result = businessObjectDefinitionIds != null ? businessObjectDefinitionIds.hashCode() : 0;
        result = 31 * result + (modificationType != null ? modificationType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "SearchIndexUpdateDto{" +
            "businessObjectDefinitionIds=" + businessObjectDefinitionIds +
            ", modificationType='" + modificationType + '\'' +
            '}';
    }
}
