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
 *  Holds the response object retrieved from elastic search index
 */
public class ElasticsearchResponseDto
{
    private List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtos;

    private List<TagTypeIndexSearchResponsedto> tagTypeIndexSearchResponsedtos;

    /**
     * Empty constructor
     */
    public ElasticsearchResponseDto()
    {
        //Empty constructor
    }

    /**
     * @param businessObjectDefinitionIndexSearchResponseDtos the list of business object definition index search responses
     * @param tagTypeIndexSearchResponsedtos the list of tag types and their associated tags
     */
    public ElasticsearchResponseDto(List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtos,
        List<TagTypeIndexSearchResponsedto> tagTypeIndexSearchResponsedtos)
    {
        this.businessObjectDefinitionIndexSearchResponseDtos = businessObjectDefinitionIndexSearchResponseDtos;
        this.tagTypeIndexSearchResponsedtos = tagTypeIndexSearchResponsedtos;
    }

    public List<TagTypeIndexSearchResponsedto> getTagTypeIndexSearchResponsedtos()
    {
        return tagTypeIndexSearchResponsedtos;
    }

    public void setTagTypeIndexSearchResponsedtos(List<TagTypeIndexSearchResponsedto> tagTypeIndexSearchResponsedtos)
    {
        this.tagTypeIndexSearchResponsedtos = tagTypeIndexSearchResponsedtos;
    }

    public List<BusinessObjectDefinitionIndexSearchResponseDto> getBusinessObjectDefinitionIndexSearchResponseDtos()
    {
        return businessObjectDefinitionIndexSearchResponseDtos;
    }

    public void setBusinessObjectDefinitionIndexSearchResponseDtos(
        List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtos)
    {
        this.businessObjectDefinitionIndexSearchResponseDtos = businessObjectDefinitionIndexSearchResponseDtos;
    }

    @Override
    public String toString()
    {
        return "ElasticsearchResponseDto{" +
            "businessObjectDefinitionIndexSearchResponseDtos=" + businessObjectDefinitionIndexSearchResponseDtos +
            ", tagTypeIndexSearchResponsedtos=" + tagTypeIndexSearchResponsedtos +
            '}';
    }

}
