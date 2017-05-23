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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.Ec2OnDemandPricingDao;
import org.finra.herd.dao.helper.JsonHelper;
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

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
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
        verifyNoMoreInteractions(ec2OnDemandPricingDao, jsonHelper);
    }
}
