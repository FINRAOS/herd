package org.finra.herd.service.helper;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
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

        handleExpectedExceptionValidateSearchRequestKey(key, "null business data search key should fail");

        key = new BusinessObjectDataSearchKey();
        key.setNamespace(null);
        handleExpectedExceptionValidateSearchRequestKey(key, "null namespace in data search key should fail");

        key.setNamespace("SOME NAMESPACE");
        key.setBusinessObjectDefinitionName(null);
        handleExpectedExceptionValidateSearchRequestKey(key, "null business defination name in data search key should fail");

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
            Assert.isTrue(true, "No exception should be thrown, should not be in here. No exception should be thrown");
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
            key.setBusinessObjectFormatUsage("FORMAT");
            key.setBusinessObjectFormatUsage("PRC");
            key.setBusinessObjectFormatVersion(0);
            businessObjectDataSearchHelper.validateBusinessObjectDataKey(key);
        }
        catch (IllegalArgumentException ex)
        {
            // catch Exception as expected
            Assert.isTrue(true, "No exception should be thrown, should not be in here. No exception should be thrown");
        }
    }

    @Test
    public void testValidateBusinessObjectDataSearchRequestPositive()
    {
        List<BusinessObjectDataSearchFilter> businessObjectDataSearchFilters = new ArrayList<>();
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
        request.setBusinessObjectDataSearchFilters(businessObjectDataSearchFilters);

        try
        {
            businessObjectDataSearchHelper.validateBusinesObjectDataSearchRequest(request);
        }
        catch (IllegalArgumentException ex)
        {
            // catch Exception as expected
            Assert.isTrue(true, "No exception should be thrown, should not be in here. No exception should be thrown");
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
        }
        Assert.isTrue(caughtException, message);
    }
}
