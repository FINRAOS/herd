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
package org.finra.herd.dao.helper;

import static org.finra.herd.dao.impl.MockEc2OperationsImpl.AVAILABILITY_ZONE_1;
import static org.finra.herd.dao.impl.MockEc2OperationsImpl.AVAILABILITY_ZONE_2;
import static org.finra.herd.dao.impl.MockEc2OperationsImpl.AVAILABILITY_ZONE_3;
import static org.finra.herd.dao.impl.MockEc2OperationsImpl.AVAILABILITY_ZONE_4;
import static org.finra.herd.dao.impl.MockEc2OperationsImpl.INSTANCE_TYPE_1;
import static org.finra.herd.dao.impl.MockEc2OperationsImpl.INSTANCE_TYPE_2;
import static org.finra.herd.dao.impl.MockEc2OperationsImpl.INSTANCE_TYPE_3;
import static org.finra.herd.dao.impl.MockEc2OperationsImpl.INSTANCE_TYPE_4;
import static org.finra.herd.dao.impl.MockEc2OperationsImpl.SUBNET_1;
import static org.finra.herd.dao.impl.MockEc2OperationsImpl.SUBNET_2;
import static org.finra.herd.dao.impl.MockEc2OperationsImpl.SUBNET_3;
import static org.finra.herd.dao.impl.MockEc2OperationsImpl.SUBNET_4;
import static org.finra.herd.dao.impl.MockEc2OperationsImpl.SUBNET_5;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.impl.MockEc2OperationsImpl;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.InstanceDefinition;
import org.finra.herd.model.api.xml.InstanceDefinitions;
import org.finra.herd.model.api.xml.MasterInstanceDefinition;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.Ec2PriceDto;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.dto.EmrClusterPriceDto;
import org.finra.herd.model.dto.EmrVpcPricingState;

/**
 * Test cases for EMR pricing algorithm.
 */
public class EmrPricingHelperTest extends AbstractDaoTest
{
    private static final BigDecimal ONE_POINT_ONE = new BigDecimal("1.1");

    private static final BigDecimal ONE_POINT_ONE_ONE = new BigDecimal("1.11");

    private static final BigDecimal ONE_UNIT = new BigDecimal("0.00001");

    private static final BigDecimal FIVE_UNIT = new BigDecimal("0.00005");

    private static final BigDecimal TEN_PERCENT = new BigDecimal("0.1");

    private static final BigDecimal ON_DEMAND = new BigDecimal("1.00");

    private static final BigDecimal ON_DEMAND_LESS_ONE = ON_DEMAND.subtract(ONE_UNIT);

    private static final BigDecimal SPOT_PRICE_LOW = new BigDecimal(MockEc2OperationsImpl.SPOT_PRICE_LOW);

    private static final BigDecimal SPOT_PRICE_LOW_LESS_ONE = SPOT_PRICE_LOW.subtract(ONE_UNIT);

    private static final BigDecimal SPOT_PRICE_LOW_PLUS_ONE = SPOT_PRICE_LOW.add(ONE_UNIT);

    private static final BigDecimal SPOT_PRICE_VERY_HIGH = new BigDecimal(MockEc2OperationsImpl.SPOT_PRICE_VERY_HIGH);

    @Autowired
    private EmrPricingHelper emrPricingHelper;

    @Before
    public void createDatabaseEntities()
    {
        // Create EC2 on-demand pricing entities required for testing.
        ec2OnDemandPricingDaoTestHelper.createEc2OnDemandPricingEntities();
    }

    /**
     * Tests algorithmic case when the max search price is lower than on-demand price and spot price is not available. The update method should throw an error
     * indicating that no subnets satisfied the given criteria.
     */
    @Test
    public void testBestPriceAlgorithmicMaxSearchPriceTooLowAndSpotPriceNotAvailable()
    {
        String subnetId = SUBNET_1;

        // For master instance definition, use instance type that does not have spot price available.
        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_4);
        masterInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND_LESS_ONE);

        InstanceDefinition coreInstanceDefinition = null;

        InstanceDefinition taskInstanceDefinition = null;

        // Try with master failing criteria
        try
        {
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            // Set expected EMR VPC price state.
            EmrVpcPricingState expectedEmrVpcPricingState = new EmrVpcPricingState();
            expectedEmrVpcPricingState.setSubnetAvailableIpAddressCounts(new HashMap<String, Integer>()
            {{
                put(SUBNET_1, 10);
            }});
            expectedEmrVpcPricingState.setSpotPricesPerAvailabilityZone(new HashMap<String, Map<String, BigDecimal>>()
            {{
                put(AVAILABILITY_ZONE_1, new HashMap<>());
            }});
            expectedEmrVpcPricingState.setOnDemandPricesPerAvailabilityZone(new HashMap<String, Map<String, BigDecimal>>()
            {{
                put(AVAILABILITY_ZONE_1, new HashMap<String, BigDecimal>()
                {{
                    put(INSTANCE_TYPE_4, ON_DEMAND);
                }});
            }});

            assertEquals(String.format(
                "There were no subnets which satisfied your best price search criteria. If you explicitly opted to use spot EC2 instances, please confirm " +
                    "that your instance types support spot pricing. Otherwise, try setting the max price or the on-demand threshold to a higher value.%n%s",
                emrVpcPricingStateFormatter.format(expectedEmrVpcPricingState)), e.getMessage());
        }
    }

    /**
     * Tests case where max search price < on-demand, threshold < on-demand. The algorithm should select spot price because max search price is below
     * on-demand.
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 8
     */
    @Test
    public void testBestPriceAlgorithmicSearchBelowOnDemandThresholdBelowOnDemand()
    {
        String subnetId = SUBNET_1;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND_LESS_ONE);
        masterInstanceDefinition.setInstanceOnDemandThreshold(ON_DEMAND_LESS_ONE.subtract(SPOT_PRICE_LOW));

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND_LESS_ONE);
        coreInstanceDefinition.setInstanceOnDemandThreshold(ON_DEMAND_LESS_ONE.subtract(SPOT_PRICE_LOW));

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertEquals("master instance bid price", ON_DEMAND_LESS_ONE,
            emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertEquals("core instance bid price", ON_DEMAND_LESS_ONE, emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());
    }

    /**
     * Test cases where user sets instances to explicitly use spot for master and on-demand for core by setting the instanceSpotPrice property and not
     * specifying criteria, respectively.
     */
    @Test
    public void testBestPriceExplicitSpotAndOnDemand()
    {
        String subnetId = SUBNET_1;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);


        assertBestPriceCriteriaRemoved(emrClusterDefinition);

        assertEquals("master instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertNull("core instance was not on-demand", emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());
    }

    /**
     * Test cases where user sets max search price and the the max search price is simplified as a pass through to spot price.
     */
    @Test
    public void testMaxSearchPriceConvertedToSportPrice()
    {
        String subnetId = SUBNET_1;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceMaxSearchPrice(ONE_POINT_ONE);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceMaxSearchPrice(ONE_POINT_ONE);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);

        assertEquals("master instance bid price", ONE_POINT_ONE, emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertEquals("core instance bid price", ONE_POINT_ONE, emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());
    }

    /**
     * Tests algorithmic case when spot is explicitly requested and spot price is not available. The update method should throw an error indicating that no
     * subnets satisfied the given criteria.
     */
    @Test
    public void testBestPriceExplicitSpotAndSpotPriceNotAvailable()
    {
        String subnetId = SUBNET_1;

        // For master instance definition, use instance type that does not have spot price available.
        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_4);
        masterInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        InstanceDefinition coreInstanceDefinition = null;

        InstanceDefinition taskInstanceDefinition = null;

        // Try with master failing criteria
        try
        {
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            // Set expected EMR VPC price state.
            EmrVpcPricingState expectedEmrVpcPricingState = new EmrVpcPricingState();
            expectedEmrVpcPricingState.setSubnetAvailableIpAddressCounts(new HashMap<String, Integer>()
            {{
                put(SUBNET_1, 10);
            }});
            expectedEmrVpcPricingState.setSpotPricesPerAvailabilityZone(new HashMap<String, Map<String, BigDecimal>>()
            {{
                put(AVAILABILITY_ZONE_1, new HashMap<>());
            }});
            expectedEmrVpcPricingState.setOnDemandPricesPerAvailabilityZone(new HashMap<String, Map<String, BigDecimal>>()
            {{
                put(AVAILABILITY_ZONE_1, new HashMap<String, BigDecimal>()
                {{
                    put(INSTANCE_TYPE_4, ON_DEMAND);
                }});
            }});

            assertEquals(String.format(
                "There were no subnets which satisfied your best price search criteria. If you explicitly opted to use spot EC2 instances, please confirm " +
                    "that your instance types support spot pricing. Otherwise, try setting the max price or the on-demand threshold to a higher value.%n%s",
                emrVpcPricingStateFormatter.format(expectedEmrVpcPricingState)), e.getMessage());
        }
    }

    /**
     * Test case when multiple subnets are specified, but the subnet with AZ with the cheapest price does not have enough IP addresses available, the algorithm
     * should choose the subnet with enough availability, even when it is not the cheapest.
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 10
     */
    @Test
    public void testBestPriceCheapestPriceDoenstHaveEnoughIp()
    {
        String subnetId = SUBNET_1 + "," + SUBNET_3;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(5);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(6);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertEquals("master instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertEquals("core instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());

        assertEquals("selected subnet", SUBNET_3, emrClusterDefinition.getSubnetId());
    }

    /**
     * Tests case where none of the subnets specified has enough IP addresses available. The algorithm should fail with an error.
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 11
     */
    @Test
    public void testBestPriceAllSubnetNotEnoughIp()
    {
        String subnetId = SUBNET_1 + "," + SUBNET_2;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(10);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(11);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        try
        {
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);
            fail("expected ObjectNotFoundException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", ObjectNotFoundException.class, e.getClass());
        }
    }

    /**
     * Tests case where multiple subnets across multiple AZs are specified, and there are price differences. The algorithm should result in selecting the subnet
     * in the AZ with the lowest price.
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 12
     */
    @Test
    public void testBestPricePickBestAz()
    {
        String subnetId = SUBNET_1 + "," + SUBNET_3;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(4);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(5);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertEquals("master instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertEquals("core instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());

        assertEquals("selected subnet", SUBNET_1, emrClusterDefinition.getSubnetId());
    }

    /**
     * Tests case where instance count affect subnet selection. Tests case given - core instance is very expensive in one AZ but cheaper in the other - master
     * instance is very cheap in one AZ but more expensive in the other When - Enough instances are specified such that the AZ with the expensive core is
     * selected
     * <p/>
     * Even though the core is more expensive, the master is cheap enough to warrant the use of the expensive core AZ.
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 13
     */
    @Test
    public void testBestPricePickMultipleInstancesSelectCheaperCore()
    {
        String subnetId = SUBNET_1 + "," + SUBNET_4;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_2);
        coreInstanceDefinition.setInstanceSpotPrice(SPOT_PRICE_VERY_HIGH);

        InstanceDefinition taskInstanceDefinition = new InstanceDefinition();
        taskInstanceDefinition.setInstanceCount(1);
        taskInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        taskInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertEquals("master instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertEquals("core instance bid price", SPOT_PRICE_VERY_HIGH, emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());
        assertEquals("task instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getTaskInstances().getInstanceSpotPrice());

        assertEquals("selected subnet", SUBNET_1, emrClusterDefinition.getSubnetId());
    }

    /**
     * Tests case where instance count affect subnet selection. Tests case given - master instance is very expensive in one AZ but cheaper in the other - core
     * instance is very cheap in one AZ but more expensive in the other When - Enough instances are specified such that both AZ's are equal in total costs Then
     * - The result subnet is arbitrary, but should not error.
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 13
     */
    @Test
    public void testBestPricePickMultipleInstancesAzPricesAreEqual()
    {
        String subnetId = SUBNET_1 + "," + SUBNET_4;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_2);
        masterInstanceDefinition.setInstanceSpotPrice(SPOT_PRICE_VERY_HIGH);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(2);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        InstanceDefinition taskInstanceDefinition = new InstanceDefinition();
        taskInstanceDefinition.setInstanceCount(2);
        taskInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        taskInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertEquals("master instance bid price", SPOT_PRICE_VERY_HIGH,
            emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertEquals("core instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());
        assertEquals("task instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getTaskInstances().getInstanceSpotPrice());

        assertTrue("selected subnet was neither SUBNET_1 or SUBNET_4",
            SUBNET_1.equals(emrClusterDefinition.getSubnetId()) || SUBNET_4.equals(emrClusterDefinition.getSubnetId()));
    }

    /**
     * Tests case where instance count affect subnet selection. Tests case given - master instance is very expensive in one AZ but cheaper in the other - core
     * instance is very cheap in one AZ but more expensive in the other When - Enough instances are specified such that the AZ with the very cheap core is
     * selected.
     * <p/>
     * Even though the master is more expensive, the cores are cheap enough that it overtakes the master's expense.
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 13
     */
    @Test
    public void testBestPricePickMultipleInstancesSelectCheaperCoreAndTask()
    {
        String subnetId = SUBNET_1 + "," + SUBNET_4;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_2);
        masterInstanceDefinition.setInstanceSpotPrice(SPOT_PRICE_VERY_HIGH);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(2);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        InstanceDefinition taskInstanceDefinition = new InstanceDefinition();
        taskInstanceDefinition.setInstanceCount(3);
        taskInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);
        taskInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertEquals("master instance bid price", SPOT_PRICE_VERY_HIGH,
            emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertEquals("core instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());
        assertEquals("task instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getTaskInstances().getInstanceSpotPrice());

        assertEquals("selected subnet", SUBNET_4, emrClusterDefinition.getSubnetId());
    }

    /**
     * Tests case where subnet spans multiple regions, and one of the region has a cheaper price. The on-demand prices are identified by region, therefore this
     * test case tests that the correct on-demand price is selected.
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 14
     */
    @Test
    public void testBestPriceMultipleRegions()
    {
        String subnetId = SUBNET_1 + "," + SUBNET_5;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertNull("master instance was not on-demand", emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertNull("core instance was not on-demand", emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());

        assertEquals("selected subnet", SUBNET_5, emrClusterDefinition.getSubnetId());
    }

    /**
     * Tests case where instance type was not found in the spot list because AWS does not have a spot price for the given instance type in the given AZ. But
     * there is another AZ available for that does have a all spot prices available.
     */
    @Test
    public void testBestPriceSpotInstanceNotFoundBecauseSpotPriceIsNotAvailable()
    {
        String subnetId = SUBNET_1 + "," + SUBNET_5;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_3);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
    }

    /**
     * Tests case where spot price is found but no on-demand price was found for the specified subnet's region and instance type. This is a case where the
     * on-demand configuration table was not properly configured or the user specified invalid instance type.
     */
    @Test
    public void testBestPriceOnDemandNotFound()
    {
        String subnetId = SUBNET_5;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INVALID_VALUE);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_2);

        InstanceDefinition taskInstanceDefinition = null;

        try
        {
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);
            fail("expected ObjectNotFoundException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", ObjectNotFoundException.class, e.getClass());
        }
    }

    /**
     * Tests case where subnet is empty. This should default to searching all subnets.
     */
    @Test
    public void testBestPriceSubnetIsEmpty()
    {
        String subnetId = "";

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);

        assertTrue("subnet was not selected", StringUtils.isNotBlank(emrClusterDefinition.getSubnetId()));
    }

    /**
     * Tests case where subnet list contains - Valid subnet with whitespace padding - Empty string - String with only whitespaces - Trailing and leading commas
     * <p/>
     * We expect that only the valid subnet should be used, after trimming. All other blank values should be ignored.
     */
    @Test
    public void testBestPriceSubnetPermutations()
    {
        String subnetId = ", \n\t\r" + SUBNET_1 + " \n\t\r,, \n\t\r,";

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);

        assertEquals("selected subnet", SUBNET_1, emrClusterDefinition.getSubnetId());
    }

    /**
     * Tests case where at least one user specified subnet does not exist. This is a user error.
     */
    @Test
    public void testBestPriceSubnetNotFound()
    {
        String subnetId = "I_DO_NOT_EXIST," + SUBNET_1;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        try
        {
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);
            fail("expected ObjectNotFoundException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", ObjectNotFoundException.class, e.getClass());
        }
    }

    /**
     * Tests case where the subnet retrieval throws an unexpected amazon error.
     */
    @Test
    public void testBestPriceSubnetError() throws Exception
    {
        String subnetId = "throw.SomeError";

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        try
        {
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);
            fail("expected AmazonServiceException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", AmazonServiceException.class, e.getClass());
        }
    }

    /**
     * Tests case where multiple subnets are specified, but they all belong to the same AZ. In such case, the subnet with the most available IP addresses should
     * be selected.
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 9
     */
    @Test
    public void testBestPriceSameAzMultipleSubnets()
    {
        String subnetId = SUBNET_1 + "," + SUBNET_2;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);

        assertEquals("selected subnet", SUBNET_2, emrClusterDefinition.getSubnetId());
    }

    @Test
    public void testCoreInstanceNullSubnetInMultipleAzAssertSuccess() throws Exception
    {
        String subnetId = SUBNET_1 + "," + SUBNET_3;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = null;

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        // we select the pricing randomly so either one can be chosen
        assertTrue(Arrays.asList(SUBNET_1, SUBNET_3).contains(emrClusterDefinition.getSubnetId()));
    }

    @Test
    public void testCoreInstanceCount0SubnetInMultipleAzAssertSuccess()
    {
        String subnetId = SUBNET_1 + "," + SUBNET_3;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(0);
        coreInstanceDefinition.setInstanceType(INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        // we select the pricing randomly so either one can be chosen
        assertTrue(Arrays.asList(SUBNET_1, SUBNET_3).contains(emrClusterDefinition.getSubnetId()));
    }

    @Test
    public void testGetEmrClusterLowestCoreInstancePriceEmptyPricingList()
    {
        assertNull(emrPricingHelper.getEmrClusterPriceWithLowestCoreInstancePrice(Collections.emptyList()));
    }

    @Test
    public void testGetEmrClusterPricesWithinLowestCoreInstancePriceThresholdSinglePricing() throws Exception
    {
        List<EmrClusterPriceDto> pricingList = Arrays.asList(createSimpleEmrClusterPrice(AVAILABILITY_ZONE_1, BigDecimal.ONE));
        List<EmrClusterPriceDto> lowestCoreInstancePriceClusters =
            emrPricingHelper.getEmrClusterPricesWithinLowestCoreInstancePriceThreshold(pricingList, TEN_PERCENT);

        assertEquals(1, lowestCoreInstancePriceClusters.size());
        assertEquals(AVAILABILITY_ZONE_1, lowestCoreInstancePriceClusters.get(0).getAvailabilityZone());
    }

    @Test
    public void testGetEmrClusterPricesWithinLowestCoreInstancePriceThresholdMultiplePricing() throws Exception
    {
        List<EmrClusterPriceDto> pricingList = Arrays
            .asList(createSimpleEmrClusterPrice(AVAILABILITY_ZONE_1, BigDecimal.ONE), createSimpleEmrClusterPrice(AVAILABILITY_ZONE_2, BigDecimal.TEN),
                createSimpleEmrClusterPrice(AVAILABILITY_ZONE_3, ONE_POINT_ONE), createSimpleEmrClusterPrice(AVAILABILITY_ZONE_4, ONE_POINT_ONE_ONE));

        List<EmrClusterPriceDto> lowestCoreInstancePriceClusters =
            emrPricingHelper.getEmrClusterPricesWithinLowestCoreInstancePriceThreshold(pricingList, TEN_PERCENT);
        assertEquals(2, lowestCoreInstancePriceClusters.size());
        for (EmrClusterPriceDto emrClusterPriceDto : lowestCoreInstancePriceClusters)
        {
            assertTrue(Arrays.asList(AVAILABILITY_ZONE_1, AVAILABILITY_ZONE_3).contains(emrClusterPriceDto.getAvailabilityZone()));
        }
    }

    /**
     * Tests when the threshold is set to zero.
     *
     * @throws Exception
     */
    @Test
    public void testGetEmrClusterPricesWithinLowestCoreInstancePriceZeroThresholdMultiplePricings() throws Exception
    {
        List<EmrClusterPriceDto> pricingList = Arrays
            .asList(createSimpleEmrClusterPrice(AVAILABILITY_ZONE_1, BigDecimal.ONE), createSimpleEmrClusterPrice(AVAILABILITY_ZONE_2, BigDecimal.TEN),
                createSimpleEmrClusterPrice(AVAILABILITY_ZONE_3, BigDecimal.ONE.add(FIVE_UNIT)),
                createSimpleEmrClusterPrice(AVAILABILITY_ZONE_4, BigDecimal.ONE));

        List<EmrClusterPriceDto> lowestCoreInstancePriceClusters =
            emrPricingHelper.getEmrClusterPricesWithinLowestCoreInstancePriceThreshold(pricingList, BigDecimal.ZERO);
        assertEquals(2, lowestCoreInstancePriceClusters.size());
        for (EmrClusterPriceDto emrClusterPriceDto : lowestCoreInstancePriceClusters)
        {
            assertTrue(Arrays.asList(AVAILABILITY_ZONE_1, AVAILABILITY_ZONE_4).contains(emrClusterPriceDto.getAvailabilityZone()));
        }
    }

    /**
     * Tests when one cluster does not have core instance. In this case this cluster will be picked since the price for the cluster is now zero (the lowest)
     *
     * @throws Exception
     */
    @Test
    public void testGetEmrClusterPricesWithinLowestCoreInstancePriceEmptyCoreInstanceMultiplePricings() throws Exception
    {
        List<EmrClusterPriceDto> pricingList =
            Arrays.asList(createSimpleEmrClusterPrice(AVAILABILITY_ZONE_1, BigDecimal.ONE), createSimpleEmrClusterPrice(AVAILABILITY_ZONE_4, null));

        List<EmrClusterPriceDto> lowestCoreInstancePriceClusters =
            emrPricingHelper.getEmrClusterPricesWithinLowestCoreInstancePriceThreshold(pricingList, TEN_PERCENT);
        assertEquals(1, lowestCoreInstancePriceClusters.size());
        for (EmrClusterPriceDto emrClusterPriceDto : lowestCoreInstancePriceClusters)
        {
            assertTrue(Arrays.asList(AVAILABILITY_ZONE_4).contains(emrClusterPriceDto.getAvailabilityZone()));
        }
    }

    private EmrClusterPriceDto createSimpleEmrClusterPrice(final String availabilityZone, final BigDecimal instancePrice)
    {
        EmrClusterPriceDto emrClusterPriceDto = new EmrClusterPriceDto();

        emrClusterPriceDto.setAvailabilityZone(availabilityZone);

        if (instancePrice != null)
        {
            Ec2PriceDto corePrice = new Ec2PriceDto();
            corePrice.setInstanceCount(1);
            corePrice.setInstancePrice(instancePrice);
            emrClusterPriceDto.setCorePrice(corePrice);
        }

        return emrClusterPriceDto;
    }

    /**
     * The definition will have it's best price search criteria information removed after being updated by the algorithm. This method asserts that is the case.
     * The task instance information is optional. Task instances will only be validated if it was given in the original definition.
     *
     * @param emrClusterDefinition - The definition updated by the algorithm
     */
    private void assertBestPriceCriteriaRemoved(EmrClusterDefinition emrClusterDefinition)
    {
        assertNull("master instance max search price was not removed",
            emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceMaxSearchPrice());
        assertNull("master instance on-demand threshold was not removed",
            emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceOnDemandThreshold());

        assertNull("core instance max search price was not removed",
            emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceMaxSearchPrice());
        assertNull("core instance on-demand threshold was not removed",
            emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceOnDemandThreshold());

        if (emrClusterDefinition.getInstanceDefinitions().getTaskInstances() != null)
        {
            assertNull("task instance max search price was not removed",
                emrClusterDefinition.getInstanceDefinitions().getTaskInstances().getInstanceMaxSearchPrice());
            assertNull("task instance on-demand threshold was not removed",
                emrClusterDefinition.getInstanceDefinitions().getTaskInstances().getInstanceOnDemandThreshold());
        }
    }

    /**
     * Creates a new EMR cluster definition using the specified parameters, updates it with best price algorithm, and returns the definition.
     *
     * @param subnetId Subnet ID. Optional.
     * @param masterInstanceDefinition The master instance definition
     * @param coreInstanceDefinition The core instance definition
     * @param taskInstanceDefinition The task instance definition. Optional.
     *
     * @return Updated EMR cluster definition.
     */
    private EmrClusterDefinition updateEmrClusterDefinitionWithBestPrice(String subnetId, MasterInstanceDefinition masterInstanceDefinition,
        InstanceDefinition coreInstanceDefinition, InstanceDefinition taskInstanceDefinition)
    {
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setSubnetId(subnetId);
        InstanceDefinitions instanceDefinitions = new InstanceDefinitions();
        instanceDefinitions.setMasterInstances(masterInstanceDefinition);
        instanceDefinitions.setCoreInstances(coreInstanceDefinition);
        instanceDefinitions.setTaskInstances(taskInstanceDefinition);
        emrClusterDefinition.setInstanceDefinitions(instanceDefinitions);

        emrPricingHelper.updateEmrClusterDefinitionWithBestPrice(new EmrClusterAlternateKeyDto(), emrClusterDefinition,
            new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, NO_HTTP_PROXY_HOST, NO_HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1));

        return emrClusterDefinition;
    }
}
