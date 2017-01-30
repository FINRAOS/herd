package org.finra.herd.model.dto;

/**
 * Created by k26425 on 1/26/2017.
 */
public class TagIndexSearchResponseDto
{
    private String tagCode;

    private String tagDisplayName;

    private long count;

    private static String facetType = "Tag";

    public TagIndexSearchResponseDto()
    {

    }

    public TagIndexSearchResponseDto(String tagCode, long count)
    {
        this.tagCode = tagCode;
        this.count = count;
    }

    public String getTagCode()
    {
        return tagCode;
    }

    public void setTagCode(String tagCode)
    {
        this.tagCode = tagCode;
    }


    public long getCount()
    {
        return count;
    }

    public void setCount(long count)
    {
        this.count = count;
    }

    public String getTagDisplayName()
    {
        return tagDisplayName;
    }

    public void setTagDisplayName(String tagDisplayName)
    {
        this.tagDisplayName = tagDisplayName;
    }

    public static String getFacetType()
    {
        return facetType;
    }


    @Override
    public String toString()
    {
        return "TagIndexSearchResponseDto{" +
            " tagCode='" + tagCode + '\'' +
            ", count=" + count +
            '}';
    }
}
