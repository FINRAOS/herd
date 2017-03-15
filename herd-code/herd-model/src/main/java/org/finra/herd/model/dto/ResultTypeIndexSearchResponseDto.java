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
 * Result Type Index Search  Response Dto
 */
public class ResultTypeIndexSearchResponseDto
{
    private String resultTypeCode;

    private String resultTypeDisplayName;

    private long count;

    private static final String FACET_TYPE = "ResultType";

    /**
     * Empty constructor
     */
    public ResultTypeIndexSearchResponseDto()
    {
        //Empty constructor
    }

    /**
     * Constructor for DTO.
     *
     * @param resultTypeCode the result type code string
     * @param count the tag count long
     */
    public ResultTypeIndexSearchResponseDto(String resultTypeCode, long count)
    {
        this.resultTypeCode = resultTypeCode;
        this.count = count;
    }

    /**
     * Constructor for DTO.
     *
     * @param resultTypeCode the result code string
     * @param count the tag count long
     * @param resultTypeDisplayName the display name string
     */
    public ResultTypeIndexSearchResponseDto(String resultTypeCode, long count, String resultTypeDisplayName)
    {
        this.resultTypeCode = resultTypeCode;
        this.count = count;
        this.resultTypeDisplayName = resultTypeDisplayName;
    }

    public String getResultTypeCode()
    {
        return resultTypeCode;
    }

    public void setResultTypeCode(String resultTypeCode)
    {
        this.resultTypeCode = resultTypeCode;
    }


    public long getCount()
    {
        return count;
    }

    public void setCount(long count)
    {
        this.count = count;
    }

    public String getResultTypeDisplayName()
    {
        return resultTypeDisplayName;
    }

    public void setResultTypeDisplayName(String resultTypeDisplayName)
    {
        this.resultTypeDisplayName = resultTypeDisplayName;
    }

    public static String getFacetType()
    {
        return FACET_TYPE;
    }


    @Override
    public String toString()
    {
        return "ResultTypeIndexSearchResponseDto{" +
            " resultTypeCode='" + resultTypeCode + '\'' +
            ", count=" + count +
            '}';
    }
}
