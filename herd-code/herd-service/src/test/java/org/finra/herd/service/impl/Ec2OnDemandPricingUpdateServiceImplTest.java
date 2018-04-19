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
package org.finra.herd.service.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Sets;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.Ec2OnDemandPricingDao;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.helper.UrlHelper;
import org.finra.herd.model.dto.Ec2OnDemandPricing;
import org.finra.herd.model.dto.Ec2OnDemandPricingKey;
import org.finra.herd.model.jpa.Ec2OnDemandPricingEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests implementation of the service that updates EC2 on-demand pricing.
 */
public class Ec2OnDemandPricingUpdateServiceImplTest extends AbstractServiceTest
{
    @Mock
    private Ec2OnDemandPricingDao ec2OnDemandPricingDao;

    @InjectMocks
    private Ec2OnDemandPricingUpdateServiceImpl ec2OnDemandPricingUpdateServiceImpl;

    @Mock
    private JsonHelper jsonHelper;

    @Mock
    private UrlHelper urlHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testConvertLocationToRegionName()
    {
        // Call the method under test and validate the results.
        assertEquals("us-east-1", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("US East (N. Virginia)"));
        assertEquals("us-east-2", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("US East (Ohio)"));
        assertEquals("us-west-1", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("US West (N. California)"));
        assertEquals("us-west-2", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("US West (Oregon)"));
        assertEquals("ca-central-1", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("Canada (Central)"));
        assertEquals("ap-south-1", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("Asia Pacific (Mumbai)"));
        assertEquals("ap-northeast-2", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("Asia Pacific (Seoul)"));
        assertEquals("ap-southeast-1", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("Asia Pacific (Singapore)"));
        assertEquals("ap-southeast-2", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("Asia Pacific (Sydney)"));
        assertEquals("ap-northeast-1", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("Asia Pacific (Tokyo)"));
        assertEquals("eu-central-1", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("EU (Frankfurt)"));
        assertEquals("eu-west-1", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("EU (Ireland)"));
        assertEquals("eu-west-2", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("EU (London)"));
        assertEquals("sa-east-1", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("South America (Sao Paulo)"));
        assertEquals("us-gov-west-1", ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName("AWS GovCloud (US)"));
        assertEquals(AWS_REGION_NAME, ec2OnDemandPricingUpdateServiceImpl.convertLocationToRegionName(AWS_REGION_NAME));
    }

    @Test
    public void testCreateEc2OnDemandPricingEntry()
    {
        // Positive tests.
        assertEquals(new Ec2OnDemandPricing(new Ec2OnDemandPricingKey(AWS_REGION_NAME, EC2_INSTANCE_TYPE), NO_HOURLY_PRICE, SKU),
            ec2OnDemandPricingUpdateServiceImpl
                .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM,
                    EC2_INSTANCE_TYPE, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, "BoxUsage" + RANDOM_SUFFIX,
                    Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertEquals(new Ec2OnDemandPricing(new Ec2OnDemandPricingKey(AWS_REGION_NAME, EC2_INSTANCE_TYPE), NO_HOURLY_PRICE, SKU),
            ec2OnDemandPricingUpdateServiceImpl
                .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM,
                    EC2_INSTANCE_TYPE, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, RANDOM_SUFFIX + "-BoxUsage" + RANDOM_SUFFIX,
                    Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));

        // Missing required input parameters passed as nulls.
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, null, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM, EC2_INSTANCE_TYPE,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, "BoxUsage" + RANDOM_SUFFIX,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, null, EC2_INSTANCE_TYPE, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY,
                "BoxUsage" + RANDOM_SUFFIX, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM, null,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, "BoxUsage" + RANDOM_SUFFIX,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM, EC2_INSTANCE_TYPE,
                null, "BoxUsage" + RANDOM_SUFFIX, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM, EC2_INSTANCE_TYPE,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, null,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM, EC2_INSTANCE_TYPE,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, "BoxUsage" + RANDOM_SUFFIX, null));

        // Missing required input parameters passed as empty strings.
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, BLANK_TEXT, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM, EC2_INSTANCE_TYPE,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, "BoxUsage" + RANDOM_SUFFIX,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertNull(ec2OnDemandPricingUpdateServiceImpl.createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, BLANK_TEXT, EC2_INSTANCE_TYPE,
            Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, "BoxUsage" + RANDOM_SUFFIX,
            Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM, BLANK_TEXT,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, "BoxUsage" + RANDOM_SUFFIX,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM, EC2_INSTANCE_TYPE,
                BLANK_TEXT, "BoxUsage" + RANDOM_SUFFIX, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM, EC2_INSTANCE_TYPE,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, BLANK_TEXT,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM, EC2_INSTANCE_TYPE,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, "BoxUsage" + RANDOM_SUFFIX, BLANK_TEXT));

        // Invalid input parameters.
        assertNull(ec2OnDemandPricingUpdateServiceImpl.createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, INVALID_VALUE, EC2_INSTANCE_TYPE,
            Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, "BoxUsage" + RANDOM_SUFFIX,
            Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM, EC2_INSTANCE_TYPE,
                INVALID_VALUE, "BoxUsage" + RANDOM_SUFFIX, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM, EC2_INSTANCE_TYPE,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, INVALID_VALUE,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE));
        assertNull(ec2OnDemandPricingUpdateServiceImpl
            .createEc2OnDemandPricingEntry(SKU, AWS_REGION_NAME, Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM, EC2_INSTANCE_TYPE,
                Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY, "BoxUsage" + RANDOM_SUFFIX, INVALID_VALUE));
    }

    @Test
    public void testGetEc2OnDemandPricing()
    {
        // Create mock JSON objects.
        JSONObject jsonObject = mock(JSONObject.class);
        JSONObject products = mock(JSONObject.class);
        when(products.keySet()).thenReturn(Sets.newHashSet(EC2_PRODUCT_KEY));
        JSONObject product = mock(JSONObject.class);
        JSONObject attributes = mock(JSONObject.class);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_LOCATION)).thenReturn(AWS_REGION_NAME);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_OPERATING_SYSTEM))
            .thenReturn(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_INSTANCE_TYPE)).thenReturn(EC2_INSTANCE_TYPE);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_TENANCY))
            .thenReturn(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_USAGE_TYPE)).thenReturn("BoxUsage");
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_PRE_INSTALLED_SOFTWARE))
            .thenReturn(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE);
        JSONObject terms = mock(JSONObject.class);
        JSONObject onDemand = mock(JSONObject.class);
        JSONObject onDemandSkuInformation = mock(JSONObject.class);
        JSONObject pricingWrapper = mock(JSONObject.class);
        JSONObject priceDimensions = mock(JSONObject.class);
        JSONObject innerPricingWrapper = mock(JSONObject.class);
        JSONObject pricePerUnit = mock(JSONObject.class);

        // Mock the external calls.
        when(urlHelper.parseJsonObjectFromUrl(EC2_PRICING_LIST_URL)).thenReturn(jsonObject);
        when(jsonHelper.getKeyValue(jsonObject, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRODUCTS, JSONObject.class)).thenReturn(products);
        when(jsonHelper.getKeyValue(products, EC2_PRODUCT_KEY, JSONObject.class)).thenReturn(product);
        when(jsonHelper.getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_SKU, String.class)).thenReturn(SKU);
        when(jsonHelper.getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_ATTRIBUTES, JSONObject.class)).thenReturn(attributes);
        when(jsonHelper.getKeyValue(jsonObject, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_TERMS, JSONObject.class)).thenReturn(terms);
        when(jsonHelper.getKeyValue(terms, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_ON_DEMAND, JSONObject.class)).thenReturn(onDemand);
        when(jsonHelper.getKeyValue(onDemand, SKU, JSONObject.class)).thenReturn(onDemandSkuInformation);
        when(jsonHelper.getKeyValue(onDemandSkuInformation, SKU + Ec2OnDemandPricingUpdateServiceImpl.JSON_SKU_WRAPPER_SUFFIX, JSONObject.class))
            .thenReturn(pricingWrapper);
        when(jsonHelper.getKeyValue(pricingWrapper, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRICE_DIMENSIONS, JSONObject.class))
            .thenReturn(priceDimensions);
        when(jsonHelper.getKeyValue(priceDimensions,
            SKU + Ec2OnDemandPricingUpdateServiceImpl.JSON_SKU_WRAPPER_SUFFIX + Ec2OnDemandPricingUpdateServiceImpl.JSON_PRICE_DIMENSIONS_WRAPPER_SUFFIX,
            JSONObject.class)).thenReturn(innerPricingWrapper);
        when(jsonHelper.getKeyValue(innerPricingWrapper, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRICE_PER_UNIT, JSONObject.class))
            .thenReturn(pricePerUnit);
        when(jsonHelper.getKeyValue(pricePerUnit, Ec2OnDemandPricingUpdateServiceImpl.JSON_PRICE_PER_UNIT_WRAPPER, String.class))
            .thenReturn(HOURLY_PRICE.toString());

        // Call the method under test.
        List<Ec2OnDemandPricing> result = ec2OnDemandPricingUpdateServiceImpl.getEc2OnDemandPricing(EC2_PRICING_LIST_URL);

        // Verify the external calls.
        verify(urlHelper).parseJsonObjectFromUrl(EC2_PRICING_LIST_URL);
        verify(jsonHelper).getKeyValue(jsonObject, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRODUCTS, JSONObject.class);
        verify(jsonHelper).getKeyValue(products, EC2_PRODUCT_KEY, JSONObject.class);
        verify(jsonHelper).getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_SKU, String.class);
        verify(jsonHelper).getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_ATTRIBUTES, JSONObject.class);
        verify(jsonHelper).getKeyValue(jsonObject, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_TERMS, JSONObject.class);
        verify(jsonHelper).getKeyValue(terms, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_ON_DEMAND, JSONObject.class);
        verify(jsonHelper).getKeyValue(onDemand, SKU, JSONObject.class);
        verify(jsonHelper).getKeyValue(onDemandSkuInformation, SKU + Ec2OnDemandPricingUpdateServiceImpl.JSON_SKU_WRAPPER_SUFFIX, JSONObject.class);
        verify(jsonHelper).getKeyValue(pricingWrapper, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRICE_DIMENSIONS, JSONObject.class);
        verify(jsonHelper).getKeyValue(priceDimensions,
            SKU + Ec2OnDemandPricingUpdateServiceImpl.JSON_SKU_WRAPPER_SUFFIX + Ec2OnDemandPricingUpdateServiceImpl.JSON_PRICE_DIMENSIONS_WRAPPER_SUFFIX,
            JSONObject.class);
        verify(jsonHelper).getKeyValue(innerPricingWrapper, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRICE_PER_UNIT, JSONObject.class);
        verify(jsonHelper).getKeyValue(pricePerUnit, Ec2OnDemandPricingUpdateServiceImpl.JSON_PRICE_PER_UNIT_WRAPPER, String.class);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(Collections.singletonList(new Ec2OnDemandPricing(new Ec2OnDemandPricingKey(AWS_REGION_NAME, EC2_INSTANCE_TYPE), HOURLY_PRICE, SKU)),
            result);
    }

    @Test
    public void testGetEc2OnDemandPricingDuplicateEc2OnDemandPricingKeys()
    {
        // Create mock JSON objects.
        JSONObject jsonObject = mock(JSONObject.class);
        JSONObject products = mock(JSONObject.class);
        when(products.keySet()).thenReturn(Sets.newHashSet(EC2_PRODUCT_KEY, EC2_PRODUCT_KEY_2));
        JSONObject product = mock(JSONObject.class);
        JSONObject attributes = mock(JSONObject.class);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_LOCATION)).thenReturn(AWS_REGION_NAME);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_OPERATING_SYSTEM))
            .thenReturn(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_INSTANCE_TYPE)).thenReturn(EC2_INSTANCE_TYPE);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_TENANCY))
            .thenReturn(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_USAGE_TYPE)).thenReturn("BoxUsage");
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_PRE_INSTALLED_SOFTWARE))
            .thenReturn(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE);

        // Mock the external calls.
        when(urlHelper.parseJsonObjectFromUrl(EC2_PRICING_LIST_URL)).thenReturn(jsonObject);
        when(jsonHelper.getKeyValue(jsonObject, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRODUCTS, JSONObject.class)).thenReturn(products);
        when(jsonHelper.getKeyValue(products, EC2_PRODUCT_KEY, JSONObject.class)).thenReturn(product);
        when(jsonHelper.getKeyValue(products, EC2_PRODUCT_KEY_2, JSONObject.class)).thenReturn(product);
        when(jsonHelper.getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_SKU, String.class)).thenReturn(SKU);
        when(jsonHelper.getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_ATTRIBUTES, JSONObject.class)).thenReturn(attributes);

        // Try to call the method under test.
        try
        {
            ec2OnDemandPricingUpdateServiceImpl.getEc2OnDemandPricing(EC2_PRICING_LIST_URL);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                    .format("Found duplicate EC2 on-demand pricing entry for \"%s\" AWS region and \"%s\" EC2 instance type.", AWS_REGION_NAME, EC2_INSTANCE_TYPE),
                e.getMessage());
        }

        // Verify the external calls.
        verify(urlHelper).parseJsonObjectFromUrl(EC2_PRICING_LIST_URL);
        verify(jsonHelper).getKeyValue(jsonObject, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRODUCTS, JSONObject.class);
        verify(jsonHelper).getKeyValue(products, EC2_PRODUCT_KEY, JSONObject.class);
        verify(jsonHelper).getKeyValue(products, EC2_PRODUCT_KEY_2, JSONObject.class);
        verify(jsonHelper, times(2)).getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_SKU, String.class);
        verify(jsonHelper, times(2)).getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_ATTRIBUTES, JSONObject.class);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetEc2OnDemandPricingNoEc2OnDemandPricingEntriesCreated()
    {
        // Create mock JSON objects.
        JSONObject jsonObject = mock(JSONObject.class);
        JSONObject products = mock(JSONObject.class);
        when(products.keySet()).thenReturn(Sets.newHashSet(EC2_PRODUCT_KEY));
        JSONObject product = mock(JSONObject.class);
        JSONObject attributes = mock(JSONObject.class);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_LOCATION)).thenReturn(null);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_OPERATING_SYSTEM))
            .thenReturn(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_INSTANCE_TYPE)).thenReturn(EC2_INSTANCE_TYPE);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_TENANCY))
            .thenReturn(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_USAGE_TYPE)).thenReturn("BoxUsage");
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_PRE_INSTALLED_SOFTWARE))
            .thenReturn(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE);

        // Mock the external calls.
        when(urlHelper.parseJsonObjectFromUrl(EC2_PRICING_LIST_URL)).thenReturn(jsonObject);
        when(jsonHelper.getKeyValue(jsonObject, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRODUCTS, JSONObject.class)).thenReturn(products);
        when(jsonHelper.getKeyValue(products, EC2_PRODUCT_KEY, JSONObject.class)).thenReturn(product);
        when(jsonHelper.getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_SKU, String.class)).thenReturn(SKU);
        when(jsonHelper.getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_ATTRIBUTES, JSONObject.class)).thenReturn(attributes);

        // Call the method under test.
        List<Ec2OnDemandPricing> result = ec2OnDemandPricingUpdateServiceImpl.getEc2OnDemandPricing(EC2_PRICING_LIST_URL);

        // Verify the external calls.
        verify(urlHelper).parseJsonObjectFromUrl(EC2_PRICING_LIST_URL);
        verify(jsonHelper).getKeyValue(jsonObject, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRODUCTS, JSONObject.class);
        verify(jsonHelper).getKeyValue(products, EC2_PRODUCT_KEY, JSONObject.class);
        verify(jsonHelper).getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_SKU, String.class);
        verify(jsonHelper).getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_ATTRIBUTES, JSONObject.class);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(0, result.size());
    }

    @Test
    public void testGetEc2OnDemandPricingNumberFormatException()
    {
        // Create mock JSON objects.
        JSONObject jsonObject = mock(JSONObject.class);
        JSONObject products = mock(JSONObject.class);
        when(products.keySet()).thenReturn(Sets.newHashSet(EC2_PRODUCT_KEY));
        JSONObject product = mock(JSONObject.class);
        JSONObject attributes = mock(JSONObject.class);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_LOCATION)).thenReturn(AWS_REGION_NAME);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_OPERATING_SYSTEM))
            .thenReturn(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_INSTANCE_TYPE)).thenReturn(EC2_INSTANCE_TYPE);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_TENANCY))
            .thenReturn(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_TENANCY);
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_USAGE_TYPE)).thenReturn("BoxUsage");
        when(attributes.get(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_NAME_PRE_INSTALLED_SOFTWARE))
            .thenReturn(Ec2OnDemandPricingUpdateServiceImpl.JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE);
        JSONObject terms = mock(JSONObject.class);
        JSONObject onDemand = mock(JSONObject.class);
        JSONObject onDemandSkuInformation = mock(JSONObject.class);
        JSONObject pricingWrapper = mock(JSONObject.class);
        JSONObject priceDimensions = mock(JSONObject.class);
        JSONObject innerPricingWrapper = mock(JSONObject.class);
        JSONObject pricePerUnit = mock(JSONObject.class);

        // Mock the external calls.
        when(urlHelper.parseJsonObjectFromUrl(EC2_PRICING_LIST_URL)).thenReturn(jsonObject);
        when(jsonHelper.getKeyValue(jsonObject, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRODUCTS, JSONObject.class)).thenReturn(products);
        when(jsonHelper.getKeyValue(products, EC2_PRODUCT_KEY, JSONObject.class)).thenReturn(product);
        when(jsonHelper.getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_SKU, String.class)).thenReturn(SKU);
        when(jsonHelper.getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_ATTRIBUTES, JSONObject.class)).thenReturn(attributes);
        when(jsonHelper.getKeyValue(jsonObject, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_TERMS, JSONObject.class)).thenReturn(terms);
        when(jsonHelper.getKeyValue(terms, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_ON_DEMAND, JSONObject.class)).thenReturn(onDemand);
        when(jsonHelper.getKeyValue(onDemand, SKU, JSONObject.class)).thenReturn(onDemandSkuInformation);
        when(jsonHelper.getKeyValue(onDemandSkuInformation, SKU + Ec2OnDemandPricingUpdateServiceImpl.JSON_SKU_WRAPPER_SUFFIX, JSONObject.class))
            .thenReturn(pricingWrapper);
        when(jsonHelper.getKeyValue(pricingWrapper, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRICE_DIMENSIONS, JSONObject.class))
            .thenReturn(priceDimensions);
        when(jsonHelper.getKeyValue(priceDimensions,
            SKU + Ec2OnDemandPricingUpdateServiceImpl.JSON_SKU_WRAPPER_SUFFIX + Ec2OnDemandPricingUpdateServiceImpl.JSON_PRICE_DIMENSIONS_WRAPPER_SUFFIX,
            JSONObject.class)).thenReturn(innerPricingWrapper);
        when(jsonHelper.getKeyValue(innerPricingWrapper, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRICE_PER_UNIT, JSONObject.class))
            .thenReturn(pricePerUnit);
        when(jsonHelper.getKeyValue(pricePerUnit, Ec2OnDemandPricingUpdateServiceImpl.JSON_PRICE_PER_UNIT_WRAPPER, String.class)).thenReturn(STRING_VALUE);

        // Try to call the method under test.
        try
        {
            ec2OnDemandPricingUpdateServiceImpl.getEc2OnDemandPricing(EC2_PRICING_LIST_URL);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Failed to convert \"%s\" value to %s.", STRING_VALUE, BigDecimal.class.getName()), e.getMessage());
        }

        // Verify the external calls.
        verify(urlHelper).parseJsonObjectFromUrl(EC2_PRICING_LIST_URL);
        verify(jsonHelper).getKeyValue(jsonObject, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRODUCTS, JSONObject.class);
        verify(jsonHelper).getKeyValue(products, EC2_PRODUCT_KEY, JSONObject.class);
        verify(jsonHelper).getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_SKU, String.class);
        verify(jsonHelper).getKeyValue(product, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_ATTRIBUTES, JSONObject.class);
        verify(jsonHelper).getKeyValue(jsonObject, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_TERMS, JSONObject.class);
        verify(jsonHelper).getKeyValue(terms, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_ON_DEMAND, JSONObject.class);
        verify(jsonHelper).getKeyValue(onDemand, SKU, JSONObject.class);
        verify(jsonHelper).getKeyValue(onDemandSkuInformation, SKU + Ec2OnDemandPricingUpdateServiceImpl.JSON_SKU_WRAPPER_SUFFIX, JSONObject.class);
        verify(jsonHelper).getKeyValue(pricingWrapper, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRICE_DIMENSIONS, JSONObject.class);
        verify(jsonHelper).getKeyValue(priceDimensions,
            SKU + Ec2OnDemandPricingUpdateServiceImpl.JSON_SKU_WRAPPER_SUFFIX + Ec2OnDemandPricingUpdateServiceImpl.JSON_PRICE_DIMENSIONS_WRAPPER_SUFFIX,
            JSONObject.class);
        verify(jsonHelper).getKeyValue(innerPricingWrapper, Ec2OnDemandPricingUpdateServiceImpl.JSON_KEY_NAME_PRICE_PER_UNIT, JSONObject.class);
        verify(jsonHelper).getKeyValue(pricePerUnit, Ec2OnDemandPricingUpdateServiceImpl.JSON_PRICE_PER_UNIT_WRAPPER, String.class);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateEc2OnDemandPricing()
    {
        // Create a list of EC2 on-demand price keys.
        List<Ec2OnDemandPricingKey> ec2OnDemandPricingKeys = Arrays
            .asList(new Ec2OnDemandPricingKey(AWS_REGION_NAME, EC2_INSTANCE_TYPE), new Ec2OnDemandPricingKey(AWS_REGION_NAME_2, EC2_INSTANCE_TYPE_2),
                new Ec2OnDemandPricingKey(AWS_REGION_NAME_3, EC2_INSTANCE_TYPE_3), new Ec2OnDemandPricingKey(AWS_REGION_NAME_4, EC2_INSTANCE_TYPE_4));

        // Create a list of EC2 on-demand pricing entities before the update.
        List<Ec2OnDemandPricingEntity> ec2OnDemandPricingEntities =
            Arrays.asList(new Ec2OnDemandPricingEntity(), new Ec2OnDemandPricingEntity(), new Ec2OnDemandPricingEntity());
        ec2OnDemandPricingEntities.get(0).setRegionName(AWS_REGION_NAME);
        ec2OnDemandPricingEntities.get(0).setInstanceType(EC2_INSTANCE_TYPE);
        ec2OnDemandPricingEntities.get(0).setHourlyPrice(HOURLY_PRICE);
        ec2OnDemandPricingEntities.get(1).setRegionName(AWS_REGION_NAME_2);
        ec2OnDemandPricingEntities.get(1).setInstanceType(EC2_INSTANCE_TYPE_2);
        ec2OnDemandPricingEntities.get(1).setHourlyPrice(HOURLY_PRICE_2);
        ec2OnDemandPricingEntities.get(2).setRegionName(AWS_REGION_NAME_3);
        ec2OnDemandPricingEntities.get(2).setInstanceType(EC2_INSTANCE_TYPE_3);
        ec2OnDemandPricingEntities.get(2).setHourlyPrice(HOURLY_PRICE_3);

        // Create a list of EC2 on-demand price entries.
        List<Ec2OnDemandPricing> ec2OnDemandPricingEntries = Arrays.asList(new Ec2OnDemandPricing(ec2OnDemandPricingKeys.get(1), HOURLY_PRICE_2, NO_SKU),
            new Ec2OnDemandPricing(ec2OnDemandPricingKeys.get(2), HOURLY_PRICE_5, NO_SKU),
            new Ec2OnDemandPricing(ec2OnDemandPricingKeys.get(3), HOURLY_PRICE_4, NO_SKU));

        // Mock the external calls.
        when(ec2OnDemandPricingDao.getEc2OnDemandPricingEntities()).thenReturn(ec2OnDemandPricingEntities);

        // Call the method under test.
        ec2OnDemandPricingUpdateServiceImpl.updateEc2OnDemandPricing(ec2OnDemandPricingEntries);

        // Verify the external calls.
        verify(ec2OnDemandPricingDao).getEc2OnDemandPricingEntities();
        verify(ec2OnDemandPricingDao, times(2)).saveAndRefresh(any(Ec2OnDemandPricingEntity.class));
        verify(ec2OnDemandPricingDao).delete(ec2OnDemandPricingEntities.get(0));
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(HOURLY_PRICE_2, ec2OnDemandPricingEntities.get(1).getHourlyPrice());
        assertEquals(HOURLY_PRICE_5, ec2OnDemandPricingEntities.get(2).getHourlyPrice());
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(ec2OnDemandPricingDao, jsonHelper, urlHelper);
    }
}
