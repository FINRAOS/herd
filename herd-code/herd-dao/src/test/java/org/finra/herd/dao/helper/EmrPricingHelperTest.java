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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;

import com.amazonaws.AmazonServiceException;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.impl.MockEc2OperationsImpl;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.InstanceDefinition;
import org.finra.herd.model.api.xml.InstanceDefinitions;
import org.finra.herd.model.api.xml.MasterInstanceDefinition;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;

/**
 * Test cases for EMR pricing algorithm.
 */
public class EmrPricingHelperTest extends AbstractDaoTest
{
    private static final BigDecimal ONE_UNIT = new BigDecimal("0.00001");
    private static final BigDecimal ON_DEMAND = BigDecimal.ONE;
    private static final BigDecimal ON_DEMAND_LESS_ONE = ON_DEMAND.subtract(ONE_UNIT);
    private static final BigDecimal SPOT_PRICE_LOW = new BigDecimal(MockEc2OperationsImpl.SPOT_PRICE_LOW);
    private static final BigDecimal SPOT_PRICE_LOW_LESS_ONE = SPOT_PRICE_LOW.subtract(ONE_UNIT);
    private static final BigDecimal SPOT_PRICE_LOW_PLUS_ONE = SPOT_PRICE_LOW.add(ONE_UNIT);
    private static final BigDecimal SPOT_PRICE_VERY_HIGH = new BigDecimal(MockEc2OperationsImpl.SPOT_PRICE_VERY_HIGH);

    @Autowired
    private EmrPricingHelper emrPricingHelper;

    /**
     * Tests algorithmic cases:
     * <p/>
     * Master spot < on-demand and on-demand threshold < on-demand. Therefore master should use spot. Core spot < on-demand and on-demand threshold = on-demand.
     * Therefore core should use on-demand.
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 1, 2
     */
    @Test
    public void testBestPriceAlgorithmicPickSpotAndOnDemand()
    {
        String subnetId = MockEc2OperationsImpl.SUBNET_1;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND);
        masterInstanceDefinition.setInstanceOnDemandThreshold(SPOT_PRICE_LOW_LESS_ONE);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND);
        coreInstanceDefinition.setInstanceOnDemandThreshold(SPOT_PRICE_LOW);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertEquals("master instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertNull("core instance was not on-demand", emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());
    }

    /**
     * Tests algorithmic case when the max search price is lower than both spot and on-demand price. The update method should throw an error indicating that no
     * subnets satisfied the given criteria.
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 3
     */
    @Test
    public void testBestPriceAlgorithmicMaxSearchPriceTooLow()
    {
        String subnetId = MockEc2OperationsImpl.SUBNET_1;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceMaxSearchPrice(SPOT_PRICE_LOW_LESS_ONE);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceMaxSearchPrice(SPOT_PRICE_LOW_LESS_ONE);

        InstanceDefinition taskInstanceDefinition = new InstanceDefinition();
        taskInstanceDefinition.setInstanceCount(1);
        taskInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        taskInstanceDefinition.setInstanceMaxSearchPrice(SPOT_PRICE_LOW_LESS_ONE);

        // Try with both master, core, and task failing criteria
        try
        {
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);
            fail("Expected ObjectNotFoundException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", ObjectNotFoundException.class, e.getClass());
        }

        // Now try with only core and task failing criteria
        masterInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND);

        try
        {
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);
            fail("Expected ObjectNotFoundException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", ObjectNotFoundException.class, e.getClass());
        }

        // Try with only task failing criteria
        coreInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND);

        try
        {
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);
            fail("Expected ObjectNotFoundException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", ObjectNotFoundException.class, e.getClass());
        }
    }

    /**
     * Tests 2 cases: Master spot is less than on-demand, max search price is less than on-demand, threshold is greater than on-demand. - Master should pick
     * spot because even though on-demand is within the threshold, it is above the max Core spot > on-demand, max search price = spot - Core should pick
     * on-demand since on-demand is cheaper
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 4, 5
     */
    @Test
    public void testBestPriceAlgorithmicOnDemandOverMaxAndSpotGreaterThanOnDemand()
    {
        String subnetId = MockEc2OperationsImpl.SUBNET_1;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND_LESS_ONE);
        masterInstanceDefinition.setInstanceOnDemandThreshold(SPOT_PRICE_LOW_PLUS_ONE);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_3);
        coreInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertEquals("master instance bid price", ON_DEMAND_LESS_ONE,
            emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertNull("core instance was not on-demand", emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());
    }

    /**
     * Tests 2 cases: Master instance spot < on-demand, search = on-demand, threshold > on-demand - Master should use on-demand since on-demand is above
     * threshold Core instance spot < on-demand, search < on-demand, threshold < on-demand - Core should use spot since on-demand is above max
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 6, 7
     */
    @Test
    public void testBestPriceAlgorithmicThresholdAboveOnDemandAndSearchBelowOnDemand()
    {
        String subnetId = MockEc2OperationsImpl.SUBNET_1;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND);
        masterInstanceDefinition.setInstanceOnDemandThreshold(ON_DEMAND.subtract(SPOT_PRICE_LOW).add(ONE_UNIT));

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND_LESS_ONE);
        coreInstanceDefinition.setInstanceOnDemandThreshold(ON_DEMAND_LESS_ONE.subtract(SPOT_PRICE_LOW));

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertNull("master instance was not on-demand", emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertEquals("core instance bid price", ON_DEMAND_LESS_ONE, emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());
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
        String subnetId = MockEc2OperationsImpl.SUBNET_1;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND_LESS_ONE);
        masterInstanceDefinition.setInstanceOnDemandThreshold(ON_DEMAND_LESS_ONE.subtract(SPOT_PRICE_LOW));

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
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
        String subnetId = MockEc2OperationsImpl.SUBNET_1;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);


        assertBestPriceCriteriaRemoved(emrClusterDefinition);

        assertEquals("master instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertNull("core instance was not on-demand", emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());
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
        String subnetId = MockEc2OperationsImpl.SUBNET_1 + "," + MockEc2OperationsImpl.SUBNET_3;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(5);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(6);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertEquals("master instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertEquals("core instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());

        assertEquals("selected subnet", MockEc2OperationsImpl.SUBNET_3, emrClusterDefinition.getSubnetId());
    }

    /**
     * Tests case where none of the subnets specified has enough IP addresses available. The algorithm should fail with an error.
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 11
     */
    @Test
    public void testBestPriceAllSubnetNotEnoughIp()
    {
        String subnetId = MockEc2OperationsImpl.SUBNET_1 + "," + MockEc2OperationsImpl.SUBNET_2;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(10);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(11);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

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
        String subnetId = MockEc2OperationsImpl.SUBNET_1 + "," + MockEc2OperationsImpl.SUBNET_3;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(4);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        masterInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(5);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceMaxSearchPrice(ON_DEMAND);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertEquals("master instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertEquals("core instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());

        assertEquals("selected subnet", MockEc2OperationsImpl.SUBNET_1, emrClusterDefinition.getSubnetId());
    }

    /**
     * Tests case where instance count affect subnet selection. Tests case given - master instance is very expensive in one AZ but cheaper in the other - core
     * instance is very cheap in one AZ but more expensive in the other When - Enough instances are specified such that the AZ with the expensive master is
     * selected
     * <p/>
     * Even though the core is more expensive, the master is cheap enough to warrant the use of the expensive core AZ.
     * <p/>
     * Test case reference ClusterSpotPriceAlgorithm 13
     */
    @Test
    public void testBestPricePickMultipleInstancesSelectCheaperMaster()
    {
        String subnetId = MockEc2OperationsImpl.SUBNET_1 + "," + MockEc2OperationsImpl.SUBNET_4;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_2);
        masterInstanceDefinition.setInstanceSpotPrice(SPOT_PRICE_VERY_HIGH);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(2);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        InstanceDefinition taskInstanceDefinition = new InstanceDefinition();
        taskInstanceDefinition.setInstanceCount(1);
        taskInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        taskInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertEquals("master instance bid price", SPOT_PRICE_VERY_HIGH,
                emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertEquals("core instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());
        assertEquals("task instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getTaskInstances().getInstanceSpotPrice());

        assertEquals("selected subnet", MockEc2OperationsImpl.SUBNET_1, emrClusterDefinition.getSubnetId());
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
        String subnetId = MockEc2OperationsImpl.SUBNET_1 + "," + MockEc2OperationsImpl.SUBNET_4;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_2);
        masterInstanceDefinition.setInstanceSpotPrice(SPOT_PRICE_VERY_HIGH);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(2);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        InstanceDefinition taskInstanceDefinition = new InstanceDefinition();
        taskInstanceDefinition.setInstanceCount(2);
        taskInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        taskInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertEquals("master instance bid price", SPOT_PRICE_VERY_HIGH,
                emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertEquals("core instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());
        assertEquals("task instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getTaskInstances().getInstanceSpotPrice());

        assertTrue("selected subnet was neither SUBNET_1 or SUBNET_4", MockEc2OperationsImpl.SUBNET_1.equals(emrClusterDefinition.getSubnetId()) ||
            MockEc2OperationsImpl.SUBNET_4.equals(emrClusterDefinition.getSubnetId()));
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
        String subnetId = MockEc2OperationsImpl.SUBNET_1 + "," + MockEc2OperationsImpl.SUBNET_4;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_2);
        masterInstanceDefinition.setInstanceSpotPrice(SPOT_PRICE_VERY_HIGH);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(2);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        coreInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        InstanceDefinition taskInstanceDefinition = new InstanceDefinition();
        taskInstanceDefinition.setInstanceCount(3);
        taskInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        taskInstanceDefinition.setInstanceSpotPrice(ON_DEMAND);

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertEquals("master instance bid price", SPOT_PRICE_VERY_HIGH,
                emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertEquals("core instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceSpotPrice());
        assertEquals("task instance bid price", ON_DEMAND, emrClusterDefinition.getInstanceDefinitions().getTaskInstances().getInstanceSpotPrice());

        assertEquals("selected subnet", MockEc2OperationsImpl.SUBNET_4, emrClusterDefinition.getSubnetId());
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
        String subnetId = MockEc2OperationsImpl.SUBNET_1 + "," + MockEc2OperationsImpl.SUBNET_5;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);
        assertNull("master instance was not on-demand", emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());
        assertNull("core instance was not on-demand", emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceSpotPrice());

        assertEquals("selected subnet", MockEc2OperationsImpl.SUBNET_5, emrClusterDefinition.getSubnetId());
    }

    /**
     * Tests case where instance type was not found in the spot list. This is a user error because we are using AWS API to look up the values, therefore, we can
     * trust that if AWS does not return the instance type, it does not exist.
     */
    @Test
    public void testBestPriceSpotInstanceNotFound()
    {
        String subnetId = MockEc2OperationsImpl.SUBNET_5;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_3);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_3);

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
     * Tests case where instance type was not found in the spot list because AWS does not have a
     * spot price for the given instance type in the given AZ.
     * But there is another AZ available for that does have a all spot prices available.
     */
    @Test
    public void testBestPriceSpotInstanceNotFoundBecauseSpotPriceIsNotAvailable()
    {
        String subnetId = MockEc2OperationsImpl.SUBNET_1 + "," + MockEc2OperationsImpl.SUBNET_5;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_3);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

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
        String subnetId = MockEc2OperationsImpl.SUBNET_5;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_2);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_2);

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
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

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
        String subnetId = ", \n\t\r" + MockEc2OperationsImpl.SUBNET_1 + " \n\t\r,, \n\t\r,";

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);

        assertEquals("selected subnet", MockEc2OperationsImpl.SUBNET_1, emrClusterDefinition.getSubnetId());
    }

    /**
     * Tests case where at least one user specified subnet does not exist. This is a user error.
     */
    @Test
    public void testBestPriceSubnetNotFound()
    {
        String subnetId = "I_DO_NOT_EXIST," + MockEc2OperationsImpl.SUBNET_1;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

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
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

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
        String subnetId = MockEc2OperationsImpl.SUBNET_1 + "," + MockEc2OperationsImpl.SUBNET_2;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertBestPriceCriteriaRemoved(emrClusterDefinition);

        assertEquals("selected subnet", MockEc2OperationsImpl.SUBNET_2, emrClusterDefinition.getSubnetId());
    }

    @Test
    public void testCoreInstanceNullSubnetInMultipleAzAssertSuccess() throws Exception
    {
        String subnetId = MockEc2OperationsImpl.SUBNET_1 + "," + MockEc2OperationsImpl.SUBNET_3;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = null;

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertEquals(MockEc2OperationsImpl.SUBNET_3, emrClusterDefinition.getSubnetId());
    }

    @Test
    public void testCoreInstanceCount0SubnetInMultipleAzAssertSuccess()
    {
        String subnetId = MockEc2OperationsImpl.SUBNET_1 + "," + MockEc2OperationsImpl.SUBNET_3;

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(0);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);

        InstanceDefinition taskInstanceDefinition = null;

        EmrClusterDefinition emrClusterDefinition =
            updateEmrClusterDefinitionWithBestPrice(subnetId, masterInstanceDefinition, coreInstanceDefinition, taskInstanceDefinition);

        assertEquals(MockEc2OperationsImpl.SUBNET_3, emrClusterDefinition.getSubnetId());
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

        emrPricingHelper.updateEmrClusterDefinitionWithBestPrice(new EmrClusterAlternateKeyDto(), emrClusterDefinition);

        return emrClusterDefinition;
    }
}
