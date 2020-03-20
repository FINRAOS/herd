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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data transfer object used to notify a message listener that a document in the elastic search index needs to be modified.
 */
public class SearchIndexUpdateDto
{
    /**
     * Logger for the SearchIndexUpdateDto
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchIndexUpdateDto.class);

    /**
     * String used to signal a create of a new index document.
     */
    public static final String MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE = "BDEF";

    /**
     * String used to signal a create of a new index document.
     */
    public static final String MESSAGE_TYPE_TAG_UPDATE = "TAG";

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
     * A list of business object definition ids that will be modified in the business object definition index.
     */
    private List<Long> businessObjectDefinitionIds;

    /**
     * The type of modification that will be processed. BUSINESS_OBJECT_DEFINITION, TAG
     */
    private String messageType;

    /**
     * The type of modification that will be processed. INSERT, UPDATE, DELETE
     */
    private String modificationType;

    /**
     * A list of tag ids that will be modified in the tag index
     */
    private List<Long> tagIds;

    /**
     * Default constructor required for JSON object mapping
     */
    public SearchIndexUpdateDto()
    {
        // Empty constructor
    }

    /**
     * The constructor for the search index update data transfer object
     * @param messageType the message type that this dto represents
     * @param ids a list of ids to modify
     * @param modificationType the type of modification
     */
    public SearchIndexUpdateDto(String messageType, List<Long> ids, String modificationType)
    {
        this.messageType = messageType;

        switch (messageType)
        {
            case MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE:
                businessObjectDefinitionIds = ids;
                break;
            case MESSAGE_TYPE_TAG_UPDATE:
                tagIds = ids;
                break;
            default:
                LOGGER.error("Unknown message type.");
                break;
        }

        this.modificationType = modificationType;
    }

    public List<Long> getBusinessObjectDefinitionIds()
    {
        return businessObjectDefinitionIds;
    }

    public void setBusinessObjectDefinitionIds(List<Long> businessObjectDefinitionIds)
    {
        this.businessObjectDefinitionIds = businessObjectDefinitionIds;
    }

    public String getMessageType()
    {
        return messageType;
    }

    public void setMessageType(String messageType)
    {
        this.messageType = messageType;
    }

    public String getModificationType()
    {
        return modificationType;
    }

    public void setModificationType(String modificationType)
    {
        this.modificationType = modificationType;
    }

    public List<Long> getTagIds()
    {
        return tagIds;
    }

    public void setTagIds(List<Long> tagIds)
    {
        this.tagIds = tagIds;
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
        if (messageType != null ? !messageType.equals(that.messageType) : that.messageType != null)
        {
            return false;
        }
        if (modificationType != null ? !modificationType.equals(that.modificationType) : that.modificationType != null)
        {
            return false;
        }
        return tagIds != null ? tagIds.equals(that.tagIds) : that.tagIds == null;

    }

    @Override
    public int hashCode()
    {
        int result = businessObjectDefinitionIds != null ? businessObjectDefinitionIds.hashCode() : 0;
        result = 31 * result + (messageType != null ? messageType.hashCode() : 0);
        result = 31 * result + (modificationType != null ? modificationType.hashCode() : 0);
        result = 31 * result + (tagIds != null ? tagIds.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "SearchIndexUpdateDto{" +
            "businessObjectDefinitionIds=" + businessObjectDefinitionIds +
            ", messageType='" + messageType + '\'' +
            ", modificationType='" + modificationType + '\'' +
            ", tagIds=" + tagIds +
            '}';
    }
}
