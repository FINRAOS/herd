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
package org.finra.herd.rest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.access.annotation.Secured;

import org.finra.herd.model.dto.PagingInfoDto;

/*
 * Base class for all herd controllers.
 */
// This ensures that any rest endpoint will be not be accessible unless annotated with proper function.
@Secured("FN_NOT_ALLOWED")
public abstract class HerdBaseController
{
    /**
     * The HTTP header for the maximum number of results that will be returned on any page of data. The "pageSize" query parameter should not be greater than
     * this value or an HTTP status of 400 (Bad Request) error would be returned.
     */
    static final String HTTP_HEADER_PAGING_MAX_RESULTS_PER_PAGE = "Paging-MaxResultsPerPage";

    /**
     * The HTTP header for the total number of pages that exist assuming a page size limit and the total records returned in the query.
     */
    static final String HTTP_HEADER_PAGING_PAGE_COUNT = "Paging-PageCount";

    /**
     * The HTTP header for the current page number being returned. For the first page, this value would be "1".
     */
    static final String HTTP_HEADER_PAGING_PAGE_NUM = "Paging-PageNum";

    /**
     * The HTTP header for the current page size limit. This is based on what is specified in the request "pageSize" query parameter.
     */
    static final String HTTP_HEADER_PAGING_PAGE_SIZE = "Paging-PageSize";

    /**
     * The HTTP header for the total number of records returned on the current page. This could be less than the "pageSize" query parameter on the last page of
     * data.
     */
    static final String HTTP_HEADER_PAGING_TOTAL_RECORDS_ON_PAGE = "Paging-TotalRecordsOnPage";

    /**
     * The HTTP header for the total number of records that would be returned across all pages. This is basically a "select count" query.
     */
    static final String HTTP_HEADER_PAGING_TOTAL_RECORD_COUNT = "Paging-TotalRecordCount";

    protected HerdBaseController()
    {
        // Prevent classes from instantiating except sub-classes.
    }

    /**
     * Gets the given delimited field values as a list. If delimited field values is null, returns an empty list.
     *
     * @param delimitedFieldValues Delimited field values
     *
     * @return List of values
     */
    protected List<String> getList(DelimitedFieldValues delimitedFieldValues)
    {
        return delimitedFieldValues == null ? new ArrayList<>() : delimitedFieldValues.getValues();
    }

    /**
     * Validates that the query string parameters aren't duplicated for a list of expected parameters.
     *
     * @param parameterMap the query string parameter map.
     * @param parametersToCheck the query string parameters to check.
     *
     * @throws IllegalArgumentException if any duplicates were found.
     */
    public void validateNoDuplicateQueryStringParams(Map<String, String[]> parameterMap, String... parametersToCheck) throws IllegalArgumentException
    {
        List<String> parametersToCheckList = Arrays.asList(parametersToCheck);
        for (Map.Entry<String, String[]> mapEntry : parameterMap.entrySet())
        {
            if ((parametersToCheckList.contains(mapEntry.getKey())) && (mapEntry.getValue().length != 1))
            {
                throw new IllegalArgumentException("Found " + mapEntry.getValue().length + " occurrences of query string parameter \"" + mapEntry.getKey() +
                    "\", but 1 expected. Values found: \"" + StringUtils.join(mapEntry.getValue(), ", ") + "\".");
            }
        }
    }

    /**
     * Sets HTTP headers to HTTP servlet response per specified paging information.
     *
     * @param httpServletResponse the HTTP servlet response
     * @param pagingInfo the paging information DTO
     */
    protected void addPagingHttpHeaders(HttpServletResponse httpServletResponse, PagingInfoDto pagingInfo)
    {
        httpServletResponse.setHeader(HTTP_HEADER_PAGING_PAGE_NUM, String.valueOf(pagingInfo.getPageNum()));
        httpServletResponse.setHeader(HTTP_HEADER_PAGING_PAGE_SIZE, String.valueOf(pagingInfo.getPageSize()));
        httpServletResponse.setHeader(HTTP_HEADER_PAGING_PAGE_COUNT, String.valueOf(pagingInfo.getPageCount()));
        httpServletResponse.setHeader(HTTP_HEADER_PAGING_TOTAL_RECORDS_ON_PAGE, String.valueOf(pagingInfo.getTotalRecordsOnPage()));
        httpServletResponse.setHeader(HTTP_HEADER_PAGING_TOTAL_RECORD_COUNT, String.valueOf(pagingInfo.getTotalRecordCount()));
        httpServletResponse.setHeader(HTTP_HEADER_PAGING_MAX_RESULTS_PER_PAGE, String.valueOf(pagingInfo.getMaxResultsPerPage()));
    }
}
