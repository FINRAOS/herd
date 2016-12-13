package org.finra.herd.model.dto;

import java.util.List;

/**
 * Data transfer object used to notify a message listener that a document in the
 * elastic search index needs to be modified.
 *
 */

public class ElasticsearchIndexReplicationDto
{
    /**
     * A list of business object definition ids that will be modified in the index.
     */
    private List<Integer> BusinessObjectDefinitionIds;

    /**
     * The type of modification that will be processed.
     * INSERT, UPDATE, DELETE
     */
    private String modificationType;

    /**
     * Default constructor required for JSON object mapping
     */
    public ElasticsearchIndexReplicationDto() { }

    public ElasticsearchIndexReplicationDto(List<Integer> businessObjectDefinitionIds, String modificationType)
    {
        BusinessObjectDefinitionIds = businessObjectDefinitionIds;
        this.modificationType = modificationType;
    }

    public List<Integer> getBusinessObjectDefinitionIds()
    {
        return BusinessObjectDefinitionIds;
    }

    public void setBusinessObjectDefinitionIds(List<Integer> businessObjectDefinitionIds)
    {
        BusinessObjectDefinitionIds = businessObjectDefinitionIds;
    }

    public String getModificationType()
    {
        return modificationType;
    }

    public void setModificationType(String modificationType)
    {
        this.modificationType = modificationType;
    }
}
