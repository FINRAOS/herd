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
package org.finra.herd.service.helper;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.api.xml.AttributeValueFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.dto.ConfigurationValue;


/*
 * a helper class Business Object Data Search 
 */
@Component
public class BusinessObjectDataSearchHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    protected ConfigurationHelper configurationHelper;

    /**
     * validate business object search request
     *
     * @param request business object DATA search request
     *
     * @throws IllegalArgumentException when business object data search request is not valid
     */
    public void validateBusinesObjectDataSearchRequest(BusinessObjectDataSearchRequest request) throws IllegalArgumentException
    {
        Assert.notNull(request, "A Business Object Data SearchRequest must be specified");
        List<BusinessObjectDataSearchFilter> businessObjectDataSearchFilters = request.getBusinessObjectDataSearchFilters();
        Assert.isTrue(businessObjectDataSearchFilters != null, "Business Object Data Search Filters must be specified");
        Assert.isTrue(businessObjectDataSearchFilters.size() == 1, "Business Object Data Search Filters can only have one filter");
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = request.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys();

        Assert.isTrue(businessObjectDataSearchKeys != null, "A BusinessObject Search Key must be specified");

        Assert.isTrue(businessObjectDataSearchKeys.size() == 1, "A BusinessObject Search Key can only have one");

        for (BusinessObjectDataSearchKey key : businessObjectDataSearchKeys)
        {
            validateBusinessObjectDataKey(key);
        }
    }

    /**
     * Validate the business object search request pageNum parameter.
     *
     * @param pageNum the page number parameter. Page numbers are one-based - that is the first page number is one.
     *
     * @return the validated page number
     */
    public Integer validateBusinessObjectDataSearchRequestPageNumParameter(Integer pageNum)
    {
        int firstPage = 1;

        // If the pageNum is null set the pageNum parameter to the default first page
        if (pageNum == null)
        {
            pageNum = firstPage;
        }
        // Check if pageNum is less than one
        else if (pageNum < 1)
        {
            throw new IllegalArgumentException("A pageNum greater than 0 must be specified.");
        }

        return pageNum;
    }

    /**
     * Validate the business object search request pageSize parameter.
     *
     * @param pageSize the page size parameter. From one to maximum page size.
     *
     * @return the validated pageSize
     */
    public Integer validateBusinessObjectDataSearchRequestPageSizeParameter(Integer pageSize)
    {
        int maxPageSize = configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_MAX_PAGE_SIZE, Integer.class);

        // If the pageSize is null set the pageSize to the maxPageSize default
        if (pageSize == null)
        {
            pageSize = maxPageSize;
        }
        // Check for pageSize less than one
        else if (pageSize < 1)
        {
            throw new IllegalArgumentException("A pageSize greater than 0 must be specified.");
        }
        // Check the pageSize larger than max page size
        else if (pageSize > maxPageSize)
        {
            throw new IllegalArgumentException("A pageSize less than " + maxPageSize + " must be specified.");
        }

        return pageSize;
    }

    /**
     * validate business search key
     *
     * @param key business object data search key
     *
     * @throws IllegalArgumentException when business object data search key is not valid
     */
    public void validateBusinessObjectDataKey(BusinessObjectDataSearchKey key) throws IllegalArgumentException
    {
        Assert.notNull(key, "A business object data key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", key.getBusinessObjectDefinitionName()));

        if (key.getBusinessObjectFormatUsage() != null)
        {
            key.setBusinessObjectFormatUsage(alternateKeyHelper.validateStringParameter("business object format usage", key.getBusinessObjectFormatUsage()));
        }

        if (key.getBusinessObjectFormatFileType() != null)
        {
            key.setBusinessObjectFormatFileType(
                alternateKeyHelper.validateStringParameter("business object format file type", key.getBusinessObjectFormatFileType()));
        }

        List<PartitionValueFilter> partitionValueFilters = key.getPartitionValueFilters();
        if (partitionValueFilters != null && !partitionValueFilters.isEmpty())
        {
            businessObjectDataHelper.validatePartitionValueFilters(partitionValueFilters, null, false);

            //TODO For now, only support partition values or ranges filter
            for (PartitionValueFilter partitionValueFilter : partitionValueFilters)
            {
                List<String> partitionValues = partitionValueFilter.getPartitionValues();
                PartitionValueRange partitionValueRange = partitionValueFilter.getPartitionValueRange();
                //The partition values array should not be empty and partition vale range start and end value should not be empty
                //as it is done above at businessObjectDataHelper.validatePartitionValueFilters
                if ((partitionValues == null) && (partitionValueRange == null))
                {
                    throw new IllegalArgumentException("Only partition values or partition range are supported in partition value filters.");
                }
            }
        }

        List<AttributeValueFilter> attributeValueFilters = key.getAttributeValueFilters();
        if (attributeValueFilters != null && !attributeValueFilters.isEmpty())
        {
            for (AttributeValueFilter attributeValueFilter : attributeValueFilters)
            {
                String attributeName = attributeValueFilter.getAttributeName();
                String attributeValue = attributeValueFilter.getAttributeValue();
                if (attributeName != null)
                {
                    attributeName = attributeName.trim();
                    attributeValueFilter.setAttributeName(attributeName);
                }
                if (StringUtils.isEmpty(attributeName) && StringUtils.isEmpty(attributeValue))
                {
                    throw new IllegalArgumentException("Either attribute name or value filter must exist.");
                }
            }
        }
    }
}
