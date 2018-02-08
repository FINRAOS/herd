package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import org.finra.herd.model.api.xml.AttributeValueFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.LatestBeforePartitionValue;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.service.AbstractServiceTest;

public class BusinessObjectDataSearchHelperTest extends AbstractServiceTest
{
    @Test
    public void testValidateBusinessObjectDataSearchRequestIllegalArgumentExceptions()
    {
        try
        {
            businessObjectDataSearchHelper.validateBusinessObjectDataSearchRequest(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data search request must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataSearchHelper.validateBusinessObjectDataSearchRequest(new BusinessObjectDataSearchRequest());
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data search filter must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataSearchHelper.validateBusinessObjectDataSearchRequest(new BusinessObjectDataSearchRequest(new ArrayList<>()));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data search filter must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataSearchHelper.validateBusinessObjectDataSearchRequest(
                new BusinessObjectDataSearchRequest(Arrays.asList(new BusinessObjectDataSearchFilter(), new BusinessObjectDataSearchFilter())));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A list of business object data search filters can only have one element.", e.getMessage());
        }

        try
        {
            businessObjectDataSearchHelper
                .validateBusinessObjectDataSearchRequest(new BusinessObjectDataSearchRequest(Collections.singletonList(new BusinessObjectDataSearchFilter())));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data search key must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataSearchHelper.validateBusinessObjectDataSearchRequest(
                new BusinessObjectDataSearchRequest(Collections.singletonList(new BusinessObjectDataSearchFilter(new ArrayList<>()))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data search key must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataSearchHelper.validateBusinessObjectDataSearchRequest(new BusinessObjectDataSearchRequest(Collections
                .singletonList(new BusinessObjectDataSearchFilter(Arrays.asList(new BusinessObjectDataSearchKey(), new BusinessObjectDataSearchKey())))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A list of business object data search keys can only have one element.", e.getMessage());
        }
    }

    @Test
    public void testValidateBusinessObjectDataSearchKeyIllegalArgumentExceptions()
    {
        try
        {
            businessObjectDataSearchHelper.validateBusinessObjectDataSearchKey(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data search key must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataSearchHelper.validateBusinessObjectDataSearchKey(
                new BusinessObjectDataSearchKey(BLANK_TEXT, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                    NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataSearchHelper.validateBusinessObjectDataSearchKey(
                new BusinessObjectDataSearchKey(BDEF_NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    NO_PARTITION_VALUE_FILTERS, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataSearchHelper.validateBusinessObjectDataSearchKey(
                new BusinessObjectDataSearchKey(BDEF_NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                    NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataSearchHelper.validateBusinessObjectDataSearchKey(
                new BusinessObjectDataSearchKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Collections.singletonList(
                    new PartitionValueFilter(PARTITION_KEY, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, new LatestBeforePartitionValue(PARTITION_VALUE),
                        NO_LATEST_AFTER_PARTITION_VALUE)), NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Only partition values or partition range are supported in partition value filter.", e.getMessage());
        }

        try
        {
            businessObjectDataSearchHelper.validateBusinessObjectDataSearchKey(
                new BusinessObjectDataSearchKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                    Collections.singletonList(new AttributeValueFilter(BLANK_TEXT, EMPTY_STRING)), NO_FILTER_ON_LATEST_VALID_VERSION,
                    NO_FILTER_ON_RETENTION_EXPIRATION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Either attribute name or attribute value filter must be specified.", e.getMessage());
        }
    }

    @Test
    public void testValidatePagingParameter()
    {
        // Happy path.
        assertEquals(INTEGER_VALUE, businessObjectDataSearchHelper.validatePagingParameter(PARAMETER_NAME, INTEGER_VALUE, null, Integer.MAX_VALUE));

        // Test the default value assignment.
        assertEquals(INTEGER_VALUE_2, businessObjectDataSearchHelper.validatePagingParameter(PARAMETER_NAME, null, INTEGER_VALUE_2, Integer.MAX_VALUE));

        // Try to validate a parameter value that is less than 1.
        try
        {
            businessObjectDataSearchHelper.validatePagingParameter(PARAMETER_NAME, 0, null, Integer.MAX_VALUE);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("A %s greater than 0 must be specified.", PARAMETER_NAME), e.getMessage());
        }

        // Try to validate a parameter value that is greater than the specified max value.
        try
        {
            businessObjectDataSearchHelper.validatePagingParameter(PARAMETER_NAME, Integer.MAX_VALUE, null, Integer.MAX_VALUE - 1);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("A %s less than %d must be specified.", PARAMETER_NAME, Integer.MAX_VALUE - 1), e.getMessage());
        }
    }

    @Test
    public void testValidateBusinessObjectDataSearchKey()
    {
        AttributeValueFilter attributeValueFilter = new AttributeValueFilter();
        attributeValueFilter.setAttributeName(null);
        attributeValueFilter.setAttributeValue(ATTRIBUTE_VALUE);
        businessObjectDataSearchHelper.validateBusinessObjectDataSearchKey(
            new BusinessObjectDataSearchKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                Arrays.asList(attributeValueFilter), NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION));

        attributeValueFilter = new AttributeValueFilter();
        attributeValueFilter.setAttributeName(ATTRIBUTE_NAME);
        attributeValueFilter.setAttributeValue(null);
        businessObjectDataSearchHelper.validateBusinessObjectDataSearchKey(
            new BusinessObjectDataSearchKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                Arrays.asList(attributeValueFilter), NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION));

        attributeValueFilter = new AttributeValueFilter();
        attributeValueFilter.setAttributeName(ATTRIBUTE_NAME);
        attributeValueFilter.setAttributeValue(ATTRIBUTE_VALUE);
        businessObjectDataSearchHelper.validateBusinessObjectDataSearchKey(
            new BusinessObjectDataSearchKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                Arrays.asList(attributeValueFilter), NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION));
    }
}
