package org.finra.herd.model.dto;

import java.util.List;

/**
 * Created by k26425 on 1/26/2017.
 */
public class TagTypeIndexSearchResponsedto
{
    private String code;

    private String displayName;

    private long count;

    private static String facetType = "TagType";

    private List<TagIndexSearchResponseDto> tagIndexSearchResponseDtos;

    public TagTypeIndexSearchResponsedto()
    {

    }

    public TagTypeIndexSearchResponsedto(String code, long count, List<TagIndexSearchResponseDto> tagIndexSearchResponseDtos)
    {
        this.code = code;
        this.count = count;
        this.tagIndexSearchResponseDtos = tagIndexSearchResponseDtos;
    }


    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
    }

    public long getCount()
    {
        return count;
    }

    public void setCount(long count)
    {
        this.count = count;
    }

    public String getDisplayName()
    {
        return displayName;
    }

    public void setDisplayName(String displayName)
    {
        this.displayName = displayName;
    }

    public List<TagIndexSearchResponseDto> getTagIndexSearchResponseDtos()
    {
        return tagIndexSearchResponseDtos;
    }

    public void setTagIndexSearchResponseDtos(List<TagIndexSearchResponseDto> tagIndexSearchResponseDtos)
    {
        this.tagIndexSearchResponseDtos = tagIndexSearchResponseDtos;
    }

    public static String getFacetType()
    {
        return facetType;
    }

    @Override
    public String toString()
    {
        return "TagTypeIndexSearchResponsedto{" +
            "code='" + code + '\'' +
            ", displayName='" + displayName + '\'' +
            ", count=" + count +
            ", tagIndexSearchResponseDtos=" + tagIndexSearchResponseDtos +
            '}';
    }
}
