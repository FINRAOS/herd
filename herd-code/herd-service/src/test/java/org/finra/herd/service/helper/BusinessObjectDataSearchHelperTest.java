package org.finra.herd.service.helper;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.LatestAfterPartitionValue;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.service.AbstractServiceTest;

public class BusinessObjectDataSearchHelperTest extends AbstractServiceTest
{

    @Test
    public void testValidateBusinessObjectDataSearchRequestFilterNegative()
    {
        BusinessObjectDataSearchRequest request = null;

        handleExpectedExceptionValidateSearchRequest(request, "Null BusinessObjectDataSearchRequest should fail!");

        request = new BusinessObjectDataSearchRequest();
        handleExpectedExceptionValidateSearchRequest(request, "Empty BusinessObjectDataSearchRequest should fail");

        List<BusinessObjectDataSearchFilter> businessObjectDataSearchFilters = new ArrayList<>();
        request = new BusinessObjectDataSearchRequest();
        request.setBusinessObjectDataSearchFilters(businessObjectDataSearchFilters);
        handleExpectedExceptionValidateSearchRequest(request, "No BusinessObjectDataSearchFilter should fail");

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter();
        businessObjectDataSearchFilters.add(filter);
        request = new BusinessObjectDataSearchRequest();
        request.setBusinessObjectDataSearchFilters(businessObjectDataSearchFilters);
        handleExpectedExceptionValidateSearchRequest(request, "One filter with no namespace, defination name should fail!");

        BusinessObjectDataSearchFilter filter2 = new BusinessObjectDataSearchFilter();
        businessObjectDataSearchFilters.add(filter2);
        request = new BusinessObjectDataSearchRequest();
        request.setBusinessObjectDataSearchFilters(businessObjectDataSearchFilters);
        handleExpectedExceptionValidateSearchRequest(request, "two filters should fail!");

    }

    @Test
    public void testValidateBusinessObjectDataSearchRequestFilterTwoKeys()
    {
        // one filter, two keys
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        BusinessObjectDataSearchKey key2 = new BusinessObjectDataSearchKey();
        List<BusinessObjectDataSearchKey> keyList = new ArrayList<>();
        keyList.add(key);
        keyList.add(key2);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(keyList);
        List<BusinessObjectDataSearchFilter> filterList = new ArrayList<>();
        filterList.add(filter);
        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest(filterList);

        handleExpectedExceptionValidateSearchRequest(request, "One fileter with two business data search key should fail");

    }

    @Test
    public void testValidateBusinessObjectDataSearchRequestKeyNegative()
    {
        BusinessObjectDataSearchKey key = null;

        handleExpectedExceptionValidateSearchRequestKey(key, "A business object data key must be specified.");

        key = new BusinessObjectDataSearchKey();
        key.setNamespace(null);
        handleExpectedExceptionValidateSearchRequestKey(key, "A namespace must be specified.");

        key.setNamespace("SOME NAMESPACE");
        key.setBusinessObjectDefinitionName(null);
        handleExpectedExceptionValidateSearchRequestKey(key, "A business object definition name must be specified.");

    }

    @Test
    public void testValidateBusinessObjectDataSearchRequestKeyBadFilter()
    {
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        key.setNamespace("NAMESPACE");
        key.setBusinessObjectDefinitionName("DEF");

        PartitionValueFilter filter = new PartitionValueFilter();
        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        partitionValueFilters.add(filter);
        key.setPartitionValueFilters(partitionValueFilters);
        filter.setLatestAfterPartitionValue(new LatestAfterPartitionValue("NA"));
        filter.setPartitionKey("NA");
        handleExpectedExceptionValidateSearchRequestKey(key, "Only partition values or partition range are supported in partition value filters.");
    }
    
    @Test
    public void testValidateBusinessObjectDataSearchRequestKeyPositiveWithRequiredFieldsOnly()
    {
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        try
        {
            key.setNamespace("NAMESPACE");
            key.setBusinessObjectDefinitionName("DEF");
            businessObjectDataSearchHelper.validateBusinessObjectDataKey(key);
        }
        catch (IllegalArgumentException ex)
        {
            // catch Exception as expected
            fail("No exception should be thrown, but got " + ex.getMessage());
        }
    }

    @Test
    public void testValidateBusinessObjectDataSearchRequestKeyPositive()
    {
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        try
        {
            key.setNamespace("NAMESPACE");
            key.setBusinessObjectDefinitionName("DEF");
            key.setBusinessObjectFormatFileType("BIZ");
            key.setBusinessObjectFormatUsage("PRC");
            key.setBusinessObjectFormatVersion(0);
            businessObjectDataSearchHelper.validateBusinessObjectDataKey(key);
            
            //empty partition value filters
            key.setPartitionValueFilters(new ArrayList<PartitionValueFilter>());
            businessObjectDataSearchHelper.validateBusinessObjectDataKey(key);
        }
        catch (IllegalArgumentException ex)
        {
            // catch Exception as expected
            fail("No exception should be thrown, but got " + ex.getMessage());
        }
    }

    @Test
    public void testValidateBusinessObjectDataSearchRequestPositive()
    {
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace("NAMESPACE");
        key.setBusinessObjectDefinitionName("DEF");
        key.setBusinessObjectFormatUsage("FORMAT");
        key.setBusinessObjectFormatUsage("PRC");
        key.setBusinessObjectFormatVersion(0);
        List<BusinessObjectDataSearchKey> keyList = new ArrayList<>();
        keyList.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(keyList);
        List<BusinessObjectDataSearchFilter> filterList = new ArrayList<>();
        filterList.add(filter);
        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest(filterList);
        request.setBusinessObjectDataSearchFilters(filterList);

        try
        {
            businessObjectDataSearchHelper.validateBusinesObjectDataSearchRequest(request);
        }
        catch (IllegalArgumentException ex)
        {
            // catch Exception as expected
            fail("No exception should be thrown, but got " + ex.getMessage());
        }
    }

    /**
     * call the validation with expectation of validation failure; if now, the assert the exception message
     * 
     * @param request search request
     * @param message message to assert
     */
    private void handleExpectedExceptionValidateSearchRequest(BusinessObjectDataSearchRequest request, String message)
    {
        boolean caughtException = false;
        try
        {
            businessObjectDataSearchHelper.validateBusinesObjectDataSearchRequest(request);
        }
        catch (IllegalArgumentException ex)
        {

            caughtException = true;
        }
        Assert.isTrue(caughtException, message);
    }

    /**
     * handleExpectedExceptionValidateSearchRequestKey
     * 
     * @param key business data search key
     * @param message assert message
     */
    private void handleExpectedExceptionValidateSearchRequestKey(BusinessObjectDataSearchKey key, String message)
    {
        boolean caughtException = false;
        try
        {
            businessObjectDataSearchHelper.validateBusinessObjectDataKey(key);
        }
        catch (IllegalArgumentException ex)
        {
            caughtException = true;
            Assert.isTrue(message.equals(ex.getMessage()));
        }
        Assert.isTrue(caughtException, "exception should be thrown and caught.");
       
    }
}
