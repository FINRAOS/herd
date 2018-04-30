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

import org.apache.commons.collections4.CollectionUtils;
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

@Component
public class BusinessObjectDataSearchHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * Validates a business object data search request.
     *
     * @param businessObjectDataSearchRequest the business object data search request
     */
    public void validateBusinessObjectDataSearchRequest(BusinessObjectDataSearchRequest businessObjectDataSearchRequest)
    {
        Assert.notNull(businessObjectDataSearchRequest, "A business object data search request must be specified.");

        List<BusinessObjectDataSearchFilter> businessObjectDataSearchFilters = businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters();

        Assert.isTrue(CollectionUtils.isNotEmpty(businessObjectDataSearchFilters), "A business object data search filter must be specified.");

        Assert.isTrue(businessObjectDataSearchFilters.size() == 1, "A list of business object data search filters can only have one element.");

        BusinessObjectDataSearchFilter businessObjectDataSearchFilter = businessObjectDataSearchFilters.get(0);
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = businessObjectDataSearchFilter.getBusinessObjectDataSearchKeys();

        Assert.isTrue(CollectionUtils.isNotEmpty(businessObjectDataSearchKeys), "A business object data search key must be specified.");

        Assert.isTrue(businessObjectDataSearchKeys.size() == 1, "A list of business object data search keys can only have one element.");

        validateBusinessObjectDataSearchKey(businessObjectDataSearchKeys.get(0));
    }

    /**
     * Validates paging parameter value per specified criteria.
     *
     * @param parameterName the name of the parameter
     * @param parameterValue the parameter value, may be null
     * @param defaultParameterValue the default parameter value
     * @param maxParameterValue the maximum allowed parameter value
     *
     * @return the validated parameter value
     */
    public Integer validatePagingParameter(String parameterName, Integer parameterValue, Integer defaultParameterValue, Integer maxParameterValue)
    {
        if (parameterValue == null)
        {
            parameterValue = defaultParameterValue;
        }
        else if (parameterValue < 1)
        {
            throw new IllegalArgumentException(String.format("A %s greater than 0 must be specified.", parameterName));
        }
        else if (parameterValue > maxParameterValue)
        {
            throw new IllegalArgumentException(String.format("A %s less than %d must be specified.", parameterName, maxParameterValue));
        }

        return parameterValue;
    }

    /**
     * Validates a business object data search key.
     *
     * @param businessObjectDataSearchKey the business object data search key
     */
    void validateBusinessObjectDataSearchKey(BusinessObjectDataSearchKey businessObjectDataSearchKey)
    {
        Assert.notNull(businessObjectDataSearchKey, "A business object data search key must be specified.");

        businessObjectDataSearchKey.setNamespace(alternateKeyHelper.validateStringParameter("namespace", businessObjectDataSearchKey.getNamespace()));
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", businessObjectDataSearchKey.getBusinessObjectDefinitionName()));

        if (businessObjectDataSearchKey.getBusinessObjectFormatUsage() != null)
        {
            businessObjectDataSearchKey.setBusinessObjectFormatUsage(businessObjectDataSearchKey.getBusinessObjectFormatUsage().trim());
        }

        if (businessObjectDataSearchKey.getBusinessObjectFormatFileType() != null)
        {
            businessObjectDataSearchKey.setBusinessObjectFormatFileType(businessObjectDataSearchKey.getBusinessObjectFormatFileType().trim());
        }

        // Validate partition value filters, if specified.
        if (CollectionUtils.isNotEmpty(businessObjectDataSearchKey.getPartitionValueFilters()))
        {
            businessObjectDataHelper.validatePartitionValueFilters(businessObjectDataSearchKey.getPartitionValueFilters(), null, false);

            // TODO: For now, we only support partition values or partition range in the filter.
            for (PartitionValueFilter partitionValueFilter : businessObjectDataSearchKey.getPartitionValueFilters())
            {
                List<String> partitionValues = partitionValueFilter.getPartitionValues();
                PartitionValueRange partitionValueRange = partitionValueFilter.getPartitionValueRange();

                // The partition values array should not be empty and partition vale range start and end value should not be empty
                // as it is done above at businessObjectDataHelper.validatePartitionValueFilters().
                if (CollectionUtils.isEmpty(partitionValues) && partitionValueRange == null)
                {
                    throw new IllegalArgumentException("Only partition values or partition range are supported in partition value filter.");
                }
            }
        }

        // Validate registration date range filter, if specified.
        if (businessObjectDataSearchKey.getRegistrationDateRangeFilter() != null)
        {
            businessObjectDataHelper.validateRegistrationDateRangeFilter(businessObjectDataSearchKey.getRegistrationDateRangeFilter());
        }

        // Validate attribute value filters, if specified.
        if (CollectionUtils.isNotEmpty(businessObjectDataSearchKey.getAttributeValueFilters()))
        {
            for (AttributeValueFilter attributeValueFilter : businessObjectDataSearchKey.getAttributeValueFilters())
            {
                if (attributeValueFilter.getAttributeName() != null)
                {
                    attributeValueFilter.setAttributeName(attributeValueFilter.getAttributeName().trim());
                }
                if (StringUtils.isBlank(attributeValueFilter.getAttributeName()) && StringUtils.isEmpty(attributeValueFilter.getAttributeValue()))
                {
                    throw new IllegalArgumentException("Either attribute name or attribute value filter must be specified.");
                }
            }
        }
    }
}
