package org.finra.herd.dao;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.jpa.TagEntity;


public interface BusinessObjectDefinitionIndexSearchDao
{
    public ElasticsearchResponseDto searchBusinessObjectDefinitionsByTags(String indexName, String documentType,
        List<Map<SearchFilterType, List<TagEntity>>> nestedTagEntityMaps, Set<String> facetFieldsList);
    
    public ElasticsearchResponseDto findAllBusinessObjectDefinitions(String indexName, String documentType, Set<String> facetFieldsList);
}
