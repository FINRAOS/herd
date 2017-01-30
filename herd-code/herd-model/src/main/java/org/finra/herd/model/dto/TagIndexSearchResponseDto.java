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

/**
 * Holds the tag facet response
 */
public class TagIndexSearchResponseDto
{
    private String tagCode;

    private String tagDisplayName;

    private long count;

    private static String facetType = "Tag";

    /**
     * Empty constructor
     */
    public TagIndexSearchResponseDto()
    {
        //Empty constructor
    }

    /**
     * Constructor for DTO.
     *
     * @param tagCode the tag code string
     * @param count the tag count long
     */
    public TagIndexSearchResponseDto(String tagCode, long count)
    {
        this.tagCode = tagCode;
        this.count = count;
    }

    /**
     * Constructor for DTO.
     *
     * @param tagCode the tag code string
     * @param count the tag count long
     * @param tagDisplayName the tag display name string
     */
    public TagIndexSearchResponseDto(String tagCode, long count, String tagDisplayName)
    {
        this.tagCode = tagCode;
        this.count = count;
        this.tagDisplayName = tagDisplayName;
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
