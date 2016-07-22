package org.finra.herd.service.helper;

import java.util.ArrayList;
import java.util.List;

import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.service.AbstractServiceTest;
import org.junit.Test;
import org.springframework.util.Assert;

public class BusinessObjectDataSearchHelperTest extends AbstractServiceTest {

	@Test
    public void testValiateBusinessObjectDataSearchRequestFilterNegative()
    {
		BusinessObjectDataSearchRequest request = null;
		boolean caughtException = false;
		try
		{
			businessObjectDataSearchHelper.validBusinesObjectDataSearchRequest(request);
		}
		catch(IllegalArgumentException ex)
		{
			//catch Exception as expected
			caughtException = true;
		}
		
		Assert.isTrue(caughtException);
		caughtException = false;
		
		try
		{
			request = new BusinessObjectDataSearchRequest();
			businessObjectDataSearchHelper.validBusinesObjectDataSearchRequest(request);
		}
		catch(IllegalArgumentException ex)
		{
			//catch Exception as expected
			caughtException = true;
		}
		
		Assert.isTrue(caughtException);
		caughtException = false;
		List<BusinessObjectDataSearchFilter> businessObjectDataSearchFilters = new ArrayList<BusinessObjectDataSearchFilter>();	
		try
		{
			request = new BusinessObjectDataSearchRequest();
			request.setBusinessObjectDataSearchFilters(businessObjectDataSearchFilters);
			businessObjectDataSearchHelper.validBusinesObjectDataSearchRequest(request);
		}
		catch(IllegalArgumentException ex)
		{
			//catch Exception as expected
			caughtException = true;
		}
		
		Assert.isTrue(caughtException);
		caughtException = false;
		
		BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter();
		businessObjectDataSearchFilters.add(filter);
		//one filter, empty business request 
		try
		{
			request = new BusinessObjectDataSearchRequest();
			request.setBusinessObjectDataSearchFilters(businessObjectDataSearchFilters);
			businessObjectDataSearchHelper.validBusinesObjectDataSearchRequest(request);
		}
		catch(IllegalArgumentException ex)
		{
			//catch Exception as expected
			caughtException = true;
		}
	
		Assert.isTrue(caughtException);
		caughtException = false;
		
		BusinessObjectDataSearchFilter filter2 = new BusinessObjectDataSearchFilter();
		businessObjectDataSearchFilters.add(filter2);
		//two filters 
		try
		{
			request = new BusinessObjectDataSearchRequest();
			request.setBusinessObjectDataSearchFilters(businessObjectDataSearchFilters);
			businessObjectDataSearchHelper.validBusinesObjectDataSearchRequest(request);
		}
		catch(IllegalArgumentException ex)
		{
			//catch Exception as expected
			caughtException = true;
	    }
		Assert.isTrue(caughtException);
    }
	
	@Test
    public void testValiateBusinessObjectDataSearchRequestFilterTwoKeys()
	{		
		//one filter, two keys
		BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
		BusinessObjectDataSearchKey key2 = new BusinessObjectDataSearchKey();
		List<BusinessObjectDataSearchKey> keyList = new ArrayList<BusinessObjectDataSearchKey>();
		keyList.add(key);
		keyList.add(key2);
		
		BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(keyList);
		List<BusinessObjectDataSearchFilter> filterList = new ArrayList<BusinessObjectDataSearchFilter>();
		filterList.add(filter);
		BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest(filterList);

		request = new BusinessObjectDataSearchRequest();
		
		boolean caughtException = false;
		try
		{
			businessObjectDataSearchHelper.validBusinesObjectDataSearchRequest(request);
		}
		catch(IllegalArgumentException ex)
		{
			//catch Exception as expected
			caughtException = true;
		}
		Assert.isTrue(caughtException);
	}
	
	@Test
    public void testValiateBusinessObjectDataSearchRequestKeyNegative()
	{
		BusinessObjectDataSearchKey key = null;
		boolean caughtException = false;
		try
		{
			businessObjectDataSearchHelper.validateBusinessObjectDataKey(key);
		}
		catch(IllegalArgumentException ex)
		{
			//catch Exception as expected
			caughtException = true;
		}
		Assert.isTrue(caughtException);
		caughtException = false;
		
		
		key = new BusinessObjectDataSearchKey();
		key.setNamespace(null);
		try
		{
			businessObjectDataSearchHelper.validateBusinessObjectDataKey(key);
		}
		catch(IllegalArgumentException ex)
		{
			//catch Exception as expected
			caughtException = true;
		}
		Assert.isTrue(caughtException);
		caughtException = false;
		
		key.setNamespace("SOME NAMEPLACE");
		key.setBusinessObjectDefinitionName(null);
		
		try
		{
			businessObjectDataSearchHelper.validateBusinessObjectDataKey(key);
		}
		catch(IllegalArgumentException ex)
		{
			//catch Exception as expected
			caughtException = true;
		}	
	}
	
	@Test
    public void testValiateBusinessObjectDataSearchRequestKeyPostiveWithRequiredFieldsOnly()
	{
		BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
		try
		{
			key.setNamespace("NAMESPACE");
			key.setBusinessObjectDefinitionName("DEF");
			businessObjectDataSearchHelper.validateBusinessObjectDataKey(key);
		}
		catch(IllegalArgumentException ex)
		{
			//catch Exception as expected
			Assert.isTrue(true,"No exception should be thrown, should not be in here. No exception should be thrown");
		}
	}
	
	@Test
    public void testValiateBusinessObjectDataSearchRequestKeyPostive()
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
		catch(IllegalArgumentException ex)
		{
			//catch Exception as expected
			Assert.isTrue(true,"No exception should be thrown, should not be in here. No exception should be thrown");
		}
	}
	
	@Test
    public void testValiateBusinessObjectDataSearchRequestPostive()
	{
		List<BusinessObjectDataSearchFilter> businessObjectDataSearchFilters = new ArrayList<BusinessObjectDataSearchFilter>();		
		BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
		key.setNamespace("NAMESPACE");
		key.setBusinessObjectDefinitionName("DEF");
		key.setBusinessObjectFormatUsage("FORMAT");
		key.setBusinessObjectFormatUsage("PRC");
		key.setBusinessObjectFormatVersion(0);
		List<BusinessObjectDataSearchKey> keyList = new ArrayList<BusinessObjectDataSearchKey>();
		keyList.add(key);
		
		BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(keyList);
		List<BusinessObjectDataSearchFilter> filterList = new ArrayList<BusinessObjectDataSearchFilter>();
		filterList.add(filter);
		BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest(filterList);
		request.setBusinessObjectDataSearchFilters(businessObjectDataSearchFilters);
		
		try
		{
			businessObjectDataSearchHelper.validBusinesObjectDataSearchRequest(request);
		}
		catch(IllegalArgumentException ex)
		{
			//catch Exception as expected
			Assert.isTrue(true,"No exception should be thrown, should not be in here. No exception should be thrown");
		}
	}
}
