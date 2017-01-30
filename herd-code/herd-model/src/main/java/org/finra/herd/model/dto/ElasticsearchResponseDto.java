package org.finra.herd.model.dto;

import java.util.List;

/**
 * Created by k26425 on 1/23/2017.
 */
public class ElasticsearchResponseDto
{
    private List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtos;

    private List<TagTypeIndexSearchResponsedto> tagTypeIndexSearchResponsedtos;


    public ElasticsearchResponseDto()
    {

    }

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
