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
 * Holds the tag type facet response
 */
public class TagTypeIndexSearchResponseDto
{
    private String code;

    private String displayName;

    private long count;

    private static final String FACET_TYPE = "TagType";

    private List<TagIndexSearchResponseDto> tagIndexSearchResponseDtos;

    /**
     * Empty constructor
     */
    public TagTypeIndexSearchResponseDto()
    {
        //Empty constructor
    }

    /**
     * Constructor for DTO.
     *
     * @param code the tag type code string
     * @param count the tag type count long
     * @param tagIndexSearchResponseDtos the list of tags
     */
    public TagTypeIndexSearchResponseDto(String code, long count, List<TagIndexSearchResponseDto> tagIndexSearchResponseDtos)
    {
        this.code = code;
        this.count = count;
        this.tagIndexSearchResponseDtos = tagIndexSearchResponseDtos;
    }

    /**
     * Constructor for DTO.
     *
     * @param code the tag type code string
     * @param count the tag type count long
     * @param tagIndexSearchResponseDtos the list of tags
     * @param displayName the tag display name string
     */
    public TagTypeIndexSearchResponseDto(String code, long count, List<TagIndexSearchResponseDto> tagIndexSearchResponseDtos, String displayName)
    {
        this.code = code;
        this.count = count;
        this.tagIndexSearchResponseDtos = tagIndexSearchResponseDtos;
        this.displayName = displayName;
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
        return FACET_TYPE;
    }

    @Override
    public String toString()
    {
        return "TagTypeIndexSearchResponseDto{" +
            "code='" + code + '\'' +
            ", displayName='" + displayName + '\'' +
            ", count=" + count +
            ", tagIndexSearchResponseDtos=" + tagIndexSearchResponseDtos +
            '}';
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

        TagTypeIndexSearchResponseDto that = (TagTypeIndexSearchResponseDto) object;

        if (count != that.count)
        {
            return false;
        }
        if (code != null ? !code.equals(that.code) : that.code != null)
        {
            return false;
        }
        if (displayName != null ? !displayName.equals(that.displayName) : that.displayName != null)
        {
            return false;
        }
        return tagIndexSearchResponseDtos != null ? tagIndexSearchResponseDtos.equals(that.tagIndexSearchResponseDtos) :
            that.tagIndexSearchResponseDtos == null;
    }

    @Override
    public int hashCode()
    {
        int result = code != null ? code.hashCode() : 0;
        result = 31 * result + (displayName != null ? displayName.hashCode() : 0);
        result = 31 * result + (int) (count ^ (count >>> 32));
        result = 31 * result + (tagIndexSearchResponseDtos != null ? tagIndexSearchResponseDtos.hashCode() : 0);
        return result;
    }
}
