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

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.SpotPrice;
import com.amazonaws.services.ec2.model.Subnet;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.Ec2Dao;
import org.finra.herd.dao.Ec2OnDemandPricingDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.InstanceDefinition;
import org.finra.herd.model.api.xml.MasterInstanceDefinition;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.Ec2PriceDto;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.dto.EmrClusterPriceDto;
import org.finra.herd.model.dto.EmrVpcPricingState;
import org.finra.herd.model.jpa.Ec2OnDemandPricingEntity;

/**
 * Encapsulates logic for calculating the best price for EMR cluster.
 */
@Component
public class EmrPricingHelper extends AwsHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EmrPricingHelper.class);

    @Autowired
    private Ec2Dao ec2Dao;

    @Autowired
    private EmrVpcPricingStateFormatter emrVpcPricingStateFormatter;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private Ec2OnDemandPricingDao ec2OnDemandPricingDao;

    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * Finds the best price for each master and core instances based on the subnets and master and core instance search parameters given in the definition.
     * <p/>
     * The results of the findings are used to update the given definition.
     * <p/>
     * If the instance's instanceSpotPrice is set, the instance definition will keep that value. If the instance's instanceMaxSearchPrice is set, the best price
     * will be found. If the found price is spot, the instanceSpotPrice will be set to the value of instanceMaxSearchPrice. If the found price is on-demand, the
     * instanceSpotPrice will be removed. The definition's subnetId will be set to the particular subnet which the best price is found. The value will always be
     * replaced by a single subnet ID.
     * <p/>
     * The definition's instanceMaxSearchPrice and instanceOnDemandThreshold will be removed by this operation.
     *
     * @param emrClusterAlternateKeyDto EMR cluster alternate key
     * @param emrClusterDefinition The EMR cluster definition with search criteria, and the definition that will be updated
     * @param awsParamsDto the AWS related parameters for access/secret keys and proxy details
     */
    public void updateEmrClusterDefinitionWithBestPrice(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, EmrClusterDefinition emrClusterDefinition,
        AwsParamsDto awsParamsDto)
    {
        EmrVpcPricingState emrVpcPricingState = new EmrVpcPricingState();

        // Get total count of instances this definition will attempt to create
        int totalInstanceCount = getTotalInstanceCount(emrClusterDefinition);

        // Get the subnet information
        List<Subnet> subnets = getSubnets(emrClusterDefinition, awsParamsDto);
        for (Subnet subnet : subnets)
        {
            emrVpcPricingState.getSubnetAvailableIpAddressCounts().put(subnet.getSubnetId(), subnet.getAvailableIpAddressCount());
        }
        // Filter out subnets with not enough available IPs
        removeSubnetsWithAvailableIpsLessThan(subnets, totalInstanceCount);

        if (subnets.isEmpty())
        {
            LOGGER.info(String.format("Insufficient IP availability. namespace=\"%s\" emrClusterDefinitionName=\"%s\" emrClusterName=\"%s\" " +
                    "totalRequestedInstanceCount=%s emrVpcPricingState=%s", emrClusterAlternateKeyDto.getNamespace(),
                emrClusterAlternateKeyDto.getEmrClusterDefinitionName(), emrClusterAlternateKeyDto.getEmrClusterName(), totalInstanceCount,
                jsonHelper.objectToJson(emrVpcPricingState)));
            throw new ObjectNotFoundException(String.format(
                "There are no subnets in the current VPC which have sufficient IP addresses available to run your " +
                    "clusters. Try expanding the list of subnets or try again later. requestedInstanceCount=%s%n%s", totalInstanceCount,
                emrVpcPricingStateFormatter.format(emrVpcPricingState)));
        }

        // Best prices are accumulated in this list
        List<EmrClusterPriceDto> emrClusterPrices = new ArrayList<>();

        InstanceDefinition masterInstanceDefinition = getMasterInstanceDefinition(emrClusterDefinition);
        InstanceDefinition coreInstanceDefinition = getCoreInstanceDefinition(emrClusterDefinition);
        InstanceDefinition taskInstanceDefinition = getTaskInstanceDefinition(emrClusterDefinition);

        Set<String> requestedInstanceTypes = new HashSet<>();

        String masterInstanceType = masterInstanceDefinition.getInstanceType();
        requestedInstanceTypes.add(masterInstanceType);

        if (coreInstanceDefinition != null)
        {
            String coreInstanceType = coreInstanceDefinition.getInstanceType();
            requestedInstanceTypes.add(coreInstanceType);
        }

        if (taskInstanceDefinition != null)
        {
            String taskInstanceType = taskInstanceDefinition.getInstanceType();
            requestedInstanceTypes.add(taskInstanceType);
        }

        // Get AZs for the subnets
        for (AvailabilityZone availabilityZone : getAvailabilityZones(subnets, awsParamsDto))
        {
            // Create a mapping of instance types to prices for more efficient, in-memory lookup
            // This method also validates that the given instance types are real instance types supported by AWS.
            Map<String, BigDecimal> instanceTypeOnDemandPrices = getInstanceTypeOnDemandPrices(availabilityZone, requestedInstanceTypes);

            // Create a mapping of instance types to prices for more efficient, in-memory lookup
            // When AWS does not return any spot price history for an instance type in an availability zone, the algorithm will not use that availability zone
            // when selecting the lowest price.
            Map<String, BigDecimal> instanceTypeSpotPrices = getInstanceTypeSpotPrices(availabilityZone, requestedInstanceTypes, awsParamsDto);

            emrVpcPricingState.getSpotPricesPerAvailabilityZone().put(availabilityZone.getZoneName(), instanceTypeSpotPrices);
            emrVpcPricingState.getOnDemandPricesPerAvailabilityZone().put(availabilityZone.getZoneName(), instanceTypeOnDemandPrices);

            // Get and compare master price
            BigDecimal masterSpotPrice = instanceTypeSpotPrices.get(masterInstanceType);
            BigDecimal masterOnDemandPrice = instanceTypeOnDemandPrices.get(masterInstanceType);
            Ec2PriceDto masterPrice = getBestInstancePrice(masterSpotPrice, masterOnDemandPrice, masterInstanceDefinition);

            // Get and compare core price
            Ec2PriceDto corePrice = null;
            if (coreInstanceDefinition != null)
            {
                String coreInstanceType = coreInstanceDefinition.getInstanceType();
                BigDecimal coreSpotPrice = instanceTypeSpotPrices.get(coreInstanceType);
                BigDecimal coreOnDemandPrice = instanceTypeOnDemandPrices.get(coreInstanceType);
                corePrice = getBestInstancePrice(coreSpotPrice, coreOnDemandPrice, coreInstanceDefinition);
            }

            // Get and compare task price
            Ec2PriceDto taskPrice = null;
            if (taskInstanceDefinition != null)
            {
                String taskInstanceType = taskInstanceDefinition.getInstanceType();
                BigDecimal taskSpotPrice = instanceTypeSpotPrices.get(taskInstanceType);
                BigDecimal taskOnDemandPrice = instanceTypeOnDemandPrices.get(taskInstanceType);
                taskPrice = getBestInstancePrice(taskSpotPrice, taskOnDemandPrice, taskInstanceDefinition);
            }

            // If prices were found
            if (masterPrice != null && (coreInstanceDefinition == null || corePrice != null) && (taskInstanceDefinition == null || taskPrice != null))
            {
                // Add the pricing result to the result list
                emrClusterPrices.add(createEmrClusterPrice(availabilityZone, masterPrice, corePrice, taskPrice));
            }

            // If prices were not found for either master or core, this AZ cannot satisfy the search criteria. Ignore this AZ.
        }

        if (emrClusterPrices.isEmpty())
        {
            LOGGER.info(String.format("No subnets which satisfied the best price search criteria. namespace=\"%s\" emrClusterDefinitionName=\"%s\" " +
                    "emrClusterName=\"%s\" emrVpcPricingState=%s", emrClusterAlternateKeyDto.getNamespace(),
                emrClusterAlternateKeyDto.getEmrClusterDefinitionName(), emrClusterAlternateKeyDto.getEmrClusterName(),
                jsonHelper.objectToJson(emrVpcPricingState)));
            throw new ObjectNotFoundException(String.format(
                "There were no subnets which satisfied your best price search criteria. If you explicitly opted to use spot EC2 instances, please confirm " +
                    "that your instance types support spot pricing. Otherwise, try setting the max price or the on-demand threshold to a higher value.%n%s",
                emrVpcPricingStateFormatter.format(emrVpcPricingState)));
        }

        // Find the best prices from the result list
        EmrClusterPriceDto bestEmrClusterPrice = getEmrClusterPriceWithLowestTotalCost(emrClusterPrices);

        // Find the best subnet among the best AZ's
        Subnet bestEmrClusterSubnet = getBestSubnetForAvailabilityZone(bestEmrClusterPrice.getAvailabilityZone(), subnets);

        // Update the definition with the new calculated values
        updateInstanceDefinitionsWithBestPrice(emrClusterDefinition, bestEmrClusterSubnet, bestEmrClusterPrice);
    }

    /**
     * Returns the total number of requested instances. Returns the sum of master, core, and task instance counts. Task instance is optional.
     *
     * @param emrClusterDefinition the EMR cluster definition containing the instance definitions
     *
     * @return the total instance count
     */
    private int getTotalInstanceCount(EmrClusterDefinition emrClusterDefinition)
    {
        InstanceDefinition masterInstanceDefinition = getMasterInstanceDefinition(emrClusterDefinition);
        InstanceDefinition coreInstanceDefinition = getCoreInstanceDefinition(emrClusterDefinition);
        InstanceDefinition taskInstanceDefinition = getTaskInstanceDefinition(emrClusterDefinition);

        // Get total count of instances this definition will attempt to create
        int totalInstanceCount = masterInstanceDefinition.getInstanceCount();
        if (coreInstanceDefinition != null)
        {
            totalInstanceCount += coreInstanceDefinition.getInstanceCount();
        }
        if (taskInstanceDefinition != null)
        {
            totalInstanceCount += taskInstanceDefinition.getInstanceCount();
        }
        return totalInstanceCount;
    }

    /**
     * Updates the given definition with the given subnet and EMR pricing information.
     * <p/>
     * Sets the subnet with the given subnet ID. Removes any maxSearchPrice and onDemandThreshold that were set. Sets the spotPrice only if the given cluster
     * price is a spot.
     *
     * @param emrClusterDefinition the definition to update
     * @param bestEmrClusterSubnet the subnet to use
     * @param bestEmrClusterPrice the EMR pricing information for each instance
     */
    private void updateInstanceDefinitionsWithBestPrice(EmrClusterDefinition emrClusterDefinition, Subnet bestEmrClusterSubnet,
        EmrClusterPriceDto bestEmrClusterPrice)
    {
        emrClusterDefinition.setSubnetId(bestEmrClusterSubnet.getSubnetId());

        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceMaxSearchPrice(null);
        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceOnDemandThreshold(null);
        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceSpotPrice(getSpotBidPrice(bestEmrClusterPrice.getMasterPrice()));

        if (bestEmrClusterPrice.getCorePrice() != null)
        {
            emrClusterDefinition.getInstanceDefinitions().getCoreInstances().setInstanceMaxSearchPrice(null);
            emrClusterDefinition.getInstanceDefinitions().getCoreInstances().setInstanceOnDemandThreshold(null);
            emrClusterDefinition.getInstanceDefinitions().getCoreInstances().setInstanceSpotPrice(getSpotBidPrice(bestEmrClusterPrice.getCorePrice()));
        }
    }

    /**
     * Returns the bid price based on the given pricing information. Returns the given price's bid price if the pricing is spot. Returns null otherwise.
     *
     * @param ec2Price the EC2 pricing information
     *
     * @return the bid price, or null
     */
    private BigDecimal getSpotBidPrice(Ec2PriceDto ec2Price)
    {
        BigDecimal bidPrice = null;
        if (ec2Price.isSpotPricing())
        {
            bidPrice = ec2Price.getBidPrice();
        }
        return bidPrice;
    }

    /**
     * Chooses the best subnet from the given list of subnets, which belongs to the given availability zone. The "best" subnet is selected by the number of
     * available IP addresses in the subnet. A subnet with more availability is preferred. If multiple subnets have same IP availability, then the result subnet
     * is arbitrarily chosen.
     *
     * @param availabilityZone the availability zone in which the subnet belongs to
     * @param subnets the list of subnet to select from
     *
     * @return the subnet with the most number of available IPs
     */
    private Subnet getBestSubnetForAvailabilityZone(String availabilityZone, List<Subnet> subnets)
    {
        List<Subnet> subnetsInAvailabilityZone = new ArrayList<>();
        for (Subnet subnet : subnets)
        {
            if (subnet.getAvailabilityZone().equals(availabilityZone))
            {
                subnetsInAvailabilityZone.add(subnet);
            }
        }

        return getTop(subnetsInAvailabilityZone, new IpAddressComparator());
    }

    /**
     * An IP address comparator. A static named inner class was created as opposed to an anonymous inner class since it has no dependencies on it's containing
     * class and is therefore more efficient.
     */
    private static class IpAddressComparator implements Comparator<Subnet>, Serializable
    {
        private static final long serialVersionUID = 2005944161800182009L;

        @Override
        public int compare(Subnet o1, Subnet o2)
        {
            return o2.getAvailableIpAddressCount().compareTo(o1.getAvailableIpAddressCount());
        }
    }

    /**
     * Selects the first element after sorting the list using the given comparator. Returns null if the list is empty.
     *
     * @param list the list to select from
     * @param comparator the comparator to use to sort
     *
     * @return the first element after sorting, or null
     */
    private <T> T getTop(List<T> list, Comparator<T> comparator)
    {
        Collections.sort(list, comparator);
        return list.get(0);
    }

    /**
     * Selects the EMR cluster pricing with the lowest total cost. We will select one pricing randomly if there are multiple pricings that meet the
     * lowest total cost criteria.
     * <p>
     * Returns null if the given list is empty
     *
     * @param emrClusterPrices the list of pricing to select from
     *
     * @return the pricing with the lowest total cost
     */
    private EmrClusterPriceDto getEmrClusterPriceWithLowestTotalCost(final List<EmrClusterPriceDto> emrClusterPrices)
    {
        List<EmrClusterPriceDto> emrClusterPricesWithLowestTotalCost = getEmrClusterPricesWithinLowestTotalCostThreshold(emrClusterPrices,
            new BigDecimal(configurationHelper.getProperty(ConfigurationValue.EMR_CLUSTER_LOW_TOTAL_COST_THRESHOLD_DOLLARS)));
        if (!emrClusterPricesWithLowestTotalCost.isEmpty())
        {
            LOGGER.debug(String.format("List of pricing with low total cost: %s", emrClusterPricesWithLowestTotalCost));

            // Pick one randomly from the low total cost pricing list
            return emrClusterPricesWithLowestTotalCost.get(new Random().nextInt(emrClusterPricesWithLowestTotalCost.size()));
        }
        else
        {
            return null;
        }
    }

    /**
     * Finds all the pricings that meet the lowest total cost criteria. The lowest total cost is defined as a range from the lowest total cost of all the
     * pricings, to the lowest total cost plus the threshold value.
     * For example, if the total costs are 0.30, 0.32, 0.34, 0.36, and the threshold value is 0.05, then the low total cost range should be [0.30, 0.35].
     *
     * @param emrClusterPrices the list of pricings to select from
     * @param lowestTotalCostThresholdValue the threshold value that defines the range of low total cost
     *
     * @return the list of pricing that fall in low total cost range
     */
    private List<EmrClusterPriceDto> getEmrClusterPricesWithinLowestTotalCostThreshold(final List<EmrClusterPriceDto> emrClusterPrices,
        final BigDecimal lowestTotalCostThresholdValue)
    {
        // Builds a tree map that has the total cost as the key, and the list of pricing with the same total cost as the value. The tree map is
        // automatically sorted, so it is easy to find the lowest total cost, and total costs within the threshold of lowest total cost.
        TreeMap<BigDecimal, List<EmrClusterPriceDto>> emrClusterPriceMapKeyedByTotalCost = new TreeMap<>();
        for (final EmrClusterPriceDto emrClusterPriceDto : emrClusterPrices)
        {
            final BigDecimal totalCost = getEmrClusterTotalCost(emrClusterPriceDto);
            if (emrClusterPriceMapKeyedByTotalCost.containsKey(totalCost))
            {
                emrClusterPriceMapKeyedByTotalCost.get(totalCost).add(emrClusterPriceDto);
            }
            else
            {
                List<EmrClusterPriceDto> emrClusterPriceList = new ArrayList<>();
                emrClusterPriceList.add(emrClusterPriceDto);
                emrClusterPriceMapKeyedByTotalCost.put(totalCost, emrClusterPriceList);
            }
        }

        // Finds the list of pricings in the range of the lowest total cost
        List<EmrClusterPriceDto> emrClusterPricesWithLowestTotalCost = new ArrayList<>();
        if (!emrClusterPriceMapKeyedByTotalCost.isEmpty())
        {
            // calculate the low total cost range
            final BigDecimal lowestTotalCostLowerBound = emrClusterPriceMapKeyedByTotalCost.firstEntry().getKey();
            final BigDecimal lowestTotalCostUpperBound = lowestTotalCostLowerBound.add(lowestTotalCostThresholdValue);

            for (final Map.Entry<BigDecimal, List<EmrClusterPriceDto>> entry : emrClusterPriceMapKeyedByTotalCost.entrySet())
            {
                final BigDecimal totalCost = entry.getKey();
                // Fall into the low cost range? add it to the list
                if (totalCost.compareTo(lowestTotalCostLowerBound) >= 0 && totalCost.compareTo(lowestTotalCostUpperBound) <= 0)
                {
                    emrClusterPricesWithLowestTotalCost.addAll(entry.getValue());
                }
                else
                {
                    // since the tree map is sorted in ascending order, we do not need to check the rest of entries in the map
                    break;
                }
            }
        }
        return emrClusterPricesWithLowestTotalCost;
    }

    /**
     * Gets the total cost of the given pricing. The total cost is the sum of master, core, and task prices - each multiplied by their instance count. Task
     * price is optional and will be ignored if not specified.
     *
     * @param emrClusterPrice the pricing information
     *
     * @return the total cost
     */

    private BigDecimal getEmrClusterTotalCost(EmrClusterPriceDto emrClusterPrice)
    {
        BigDecimal totalPrice = BigDecimal.ZERO;

        BigDecimal masterPrice = getTotalCost(emrClusterPrice.getMasterPrice());
        totalPrice = totalPrice.add(masterPrice);

        if (emrClusterPrice.getCorePrice() != null)
        {
            BigDecimal corePrice = getTotalCost(emrClusterPrice.getCorePrice());
            totalPrice = totalPrice.add(corePrice);
        }

        if (emrClusterPrice.getTaskPrice() != null)
        {
            BigDecimal taskPrice = getTotalCost(emrClusterPrice.getTaskPrice());
            totalPrice = totalPrice.add(taskPrice);
        }

        return totalPrice;
    }

    /**
     * Updates the given list of subnets to remove subnets with number of available IPs less than the given value.
     *
     * @param subnets the list of subnets
     * @param availableIps the number of available IPs to filter by
     */
    private void removeSubnetsWithAvailableIpsLessThan(List<Subnet> subnets, int availableIps)
    {
        Iterator<Subnet> iterator = subnets.iterator();
        while (iterator.hasNext())
        {
            Subnet subnet = iterator.next();
            if (subnet.getAvailableIpAddressCount() < availableIps)
            {
                iterator.remove();
            }
        }
    }

    /**
     * Creates a new {@link EmrClusterPriceDto} object from the given parameters.
     *
     * @param availabilityZone the AZ
     * @param masterPrice the master instance's price
     * @param corePrice the core instance's price
     * @param taskPrice the task instance's price
     *
     * @return the new {@link EmrClusterPriceDto}
     */
    private EmrClusterPriceDto createEmrClusterPrice(AvailabilityZone availabilityZone, Ec2PriceDto masterPrice, Ec2PriceDto corePrice, Ec2PriceDto taskPrice)
    {
        EmrClusterPriceDto emrClusterPrice = new EmrClusterPriceDto();
        emrClusterPrice.setAvailabilityZone(availabilityZone.getZoneName());
        emrClusterPrice.setMasterPrice(masterPrice);
        emrClusterPrice.setCorePrice(corePrice);
        emrClusterPrice.setTaskPrice(taskPrice);
        return emrClusterPrice;
    }

    /**
     * Returns the pricing information selected based on the given instance definition's search criteria.
     * <p/>
     * If the instance's spotBidPrice is set, returns spot price with spotBidPrice as the bid price. If the instance's maxSearchPrice is set, compares the given
     * spot, on-demand prices, maxSearchPrice, and optionally, onDemandThreshold to return the best result. This may return null if neither spot or on-demand
     * price matched the given criteria. If neither spotBidPrice or maxSearchPrice is set, returns the pricing as the on-demand price.
     *
     * @param spotPrice the current spot price for the instance type
     * @param onDemandPrice the current on-demand price for the instance type
     * @param instanceDefinition the instance definition containing search criteria
     *
     * @return the new {@link Ec2PriceDto} with the pricing information
     */
    private Ec2PriceDto getBestInstancePrice(BigDecimal spotPrice, BigDecimal onDemandPrice, InstanceDefinition instanceDefinition)
    {
        LOGGER.debug("Starting... instanceType=\"{}\" instanceCount={} instanceSpotPrice={} instanceOnDemandPrice={}", instanceDefinition.getInstanceType(),
            instanceDefinition.getInstanceCount(), spotPrice, onDemandPrice);

        BigDecimal spotBidPrice = instanceDefinition.getInstanceSpotPrice();
        BigDecimal maxSearchPrice = instanceDefinition.getInstanceMaxSearchPrice();
        BigDecimal onDemandThreshold = instanceDefinition.getInstanceOnDemandThreshold();

        LOGGER.debug("instanceSpotBidPrice={} instanceMaxSearchPrice={} instanceOnDemandThreshold={}", spotBidPrice, maxSearchPrice, onDemandThreshold);

        Ec2PriceDto bestPrice;

        // spotBidPrice is set. User wants to explicitly use spot pricing
        if (spotBidPrice != null)
        {
            // Check if spot price was actually discovered.
            if (spotPrice != null)
            {
                bestPrice = new Ec2PriceDto();
                bestPrice.setSpotPricing(true);
                bestPrice.setInstancePrice(spotPrice);
                bestPrice.setInstanceCount(instanceDefinition.getInstanceCount());
                bestPrice.setBidPrice(spotBidPrice);
            }
            // If not, error out.
            else
            {
                bestPrice = null;
            }
        }
        // spotBidPrice and maxSearchPrice are not specified. User explicitly wants to use on-demand
        else if (maxSearchPrice == null)
        {
            bestPrice = new Ec2PriceDto();
            bestPrice.setSpotPricing(false);
            bestPrice.setInstanceCount(instanceDefinition.getInstanceCount());
            bestPrice.setInstancePrice(onDemandPrice);
        }
        // maxSearchPrice is set. User wants system to find best price
        else
        {
            // Default to on-demand
            bestPrice = new Ec2PriceDto();
            bestPrice.setSpotPricing(false);
            bestPrice.setInstanceCount(instanceDefinition.getInstanceCount());
            bestPrice.setInstancePrice(onDemandPrice);

            // If spot price is available, use it to compute the best price
            if (spotPrice != null)
            {
                // No on-demand threshold is equivalent to $0.00 threshold
                if (onDemandThreshold == null)
                {
                    onDemandThreshold = BigDecimal.ZERO;
                }

                BigDecimal onDemandThresholdAbsolute = spotPrice.add(onDemandThreshold);

                // Pre-compute some flags for readability
                boolean isSpotBelowMax = spotPrice.compareTo(maxSearchPrice) <= 0;
                boolean isOnDemandBelowMax = onDemandPrice.compareTo(maxSearchPrice) <= 0;
                boolean isSpotBelowOnDemand = spotPrice.compareTo(onDemandPrice) < 0;
                boolean isThresholdBelowOnDemand = onDemandThresholdAbsolute.compareTo(onDemandPrice) < 0;

                // Should I use spot?
                if (isSpotBelowMax && isSpotBelowOnDemand && (isThresholdBelowOnDemand || !isOnDemandBelowMax))
                {
                    bestPrice.setSpotPricing(true);
                    bestPrice.setInstancePrice(spotPrice);
                    bestPrice.setBidPrice(maxSearchPrice);
                }
                // Is there an error?
                else if (!isOnDemandBelowMax)
                {
                    bestPrice = null;
                }
                // Otherwise use on-demand
            }
            // Spot price is not available, so only validate that on-demand price is below max search price
            else
            {
                // Pre-compute some flags for readability
                boolean isOnDemandBelowMax = onDemandPrice.compareTo(maxSearchPrice) <= 0;

                // Error out if on-demand price is below max search price
                if (!isOnDemandBelowMax)
                {
                    bestPrice = null;
                }
                // Otherwise use on-demand
            }
        }

        LOGGER.debug("End. instanceBestPrice={}", jsonHelper.objectToJson(bestPrice));
        return bestPrice;
    }

    /**
     * Returns the core instance definition.
     *
     * @param emrClusterDefinition the EMR cluster definition
     *
     * @return the core instance definition
     */
    private InstanceDefinition getCoreInstanceDefinition(EmrClusterDefinition emrClusterDefinition)
    {
        InstanceDefinition coreInstances = emrClusterDefinition.getInstanceDefinitions().getCoreInstances();
        if (coreInstances != null && coreInstances.getInstanceCount() <= 0)
        {
            coreInstances = null;
        }
        return coreInstances;
    }

    /**
     * Returns the task instance definition. Returns null if no task definition is specified.
     *
     * @param emrClusterDefinition the EMR cluster definition
     *
     * @return the task instance definition, or null
     */
    private InstanceDefinition getTaskInstanceDefinition(EmrClusterDefinition emrClusterDefinition)
    {
        return emrClusterDefinition.getInstanceDefinitions().getTaskInstances();
    }

    /**
     * Returns the master instance definition. Copies the {@link MasterInstanceDefinition} to a {@link InstanceDefinition} to keep the class type consistent
     * with the core instance.
     *
     * @param emrClusterDefinition the EMR cluster definition
     *
     * @return the master instance definition
     */
    private InstanceDefinition getMasterInstanceDefinition(EmrClusterDefinition emrClusterDefinition)
    {
        MasterInstanceDefinition masterInstanceDefinition = emrClusterDefinition.getInstanceDefinitions().getMasterInstances();

        InstanceDefinition instanceDefinition = new InstanceDefinition();
        instanceDefinition.setInstanceType(masterInstanceDefinition.getInstanceType());
        instanceDefinition.setInstanceCount(masterInstanceDefinition.getInstanceCount());
        instanceDefinition.setInstanceSpotPrice(masterInstanceDefinition.getInstanceSpotPrice());
        instanceDefinition.setInstanceMaxSearchPrice(masterInstanceDefinition.getInstanceMaxSearchPrice());
        instanceDefinition.setInstanceOnDemandThreshold(masterInstanceDefinition.getInstanceOnDemandThreshold());
        return instanceDefinition;
    }

    /**
     * Returns a mapping of instance types to on-demand prices for the given AZ and instance types. The on-demand prices are retrieved from database
     * configurations. The on-demand prices are looked up by the AZ's region name. This method also validates that the given instance types are real instance
     * types supported by AWS.
     *
     * @param availabilityZone the availability zone of the on-demand instances
     * @param instanceTypes the sizes of the on-demand instances
     *
     * @return the map of instance type to on-demand price
     * @throws ObjectNotFoundException when any of the instance type was not found in the given region
     */
    private Map<String, BigDecimal> getInstanceTypeOnDemandPrices(AvailabilityZone availabilityZone, Set<String> instanceTypes)
    {
        Map<String, BigDecimal> instanceTypeOnDemandPrices = new HashMap<>();
        for (String instanceType : instanceTypes)
        {
            Ec2OnDemandPricingEntity onDemandPrice = ec2OnDemandPricingDao.getEc2OnDemandPricing(availabilityZone.getRegionName(), instanceType);

            if (onDemandPrice == null)
            {
                throw new ObjectNotFoundException(
                    "On-demand price for region '" + availabilityZone.getRegionName() + "' and instance type '" + instanceType + "' not found.");
            }

            instanceTypeOnDemandPrices.put(instanceType, onDemandPrice.getHourlyPrice());
        }

        return instanceTypeOnDemandPrices;
    }

    /**
     * Returns a mapping of instance types to spot prices for the given AZ and instance types. The spot prices are retrieved from EC2 API.
     * <p/>
     * This method also validates that the given instance types are real instance types supported by AWS.
     *
     * @param availabilityZone the AZ of the spot instances
     * @param instanceTypes the size of the spot instances
     * @param awsParamsDto the AWS related parameters for access/secret keys and proxy details
     *
     * @return the mapping of instance type to spot prices
     * @throws ObjectNotFoundException when any of the instance type does not exist in AWS
     */
    private Map<String, BigDecimal> getInstanceTypeSpotPrices(AvailabilityZone availabilityZone, Set<String> instanceTypes, AwsParamsDto awsParamsDto)
    {
        List<String> productDescriptions = herdStringHelper.getDelimitedConfigurationValue(ConfigurationValue.EMR_SPOT_PRICE_HISTORY_PRODUCT_DESCRIPTIONS);
        List<SpotPrice> spotPrices = ec2Dao.getLatestSpotPrices(availabilityZone.getZoneName(), instanceTypes, productDescriptions, awsParamsDto);

        Map<String, BigDecimal> instanceTypeSpotPrices = new HashMap<>();
        for (SpotPrice spotPrice : spotPrices)
        {
            instanceTypeSpotPrices.put(spotPrice.getInstanceType(), new BigDecimal(spotPrice.getSpotPrice()));
        }

        return instanceTypeSpotPrices;
    }

    /**
     * Returns a list of AZ's which the given list of subnets belong to.
     *
     * @param subnets the list of subnets in the AZ
     * @param awsParamsDto the AWS related parameters for access/secret keys and proxy details
     *
     * @return the list of AZ's
     */
    private List<AvailabilityZone> getAvailabilityZones(List<Subnet> subnets, AwsParamsDto awsParamsDto)
    {
        return ec2Dao.getAvailabilityZonesForSubnetIds(subnets, awsParamsDto);
    }

    /**
     * Returns a list of subnets specified in the definition. The definition specifies a comma-separated list of subnet IDs. This method parses it, looks up the
     * subnet from AWS, and returns the list. If the subnet is not specified or empty, all subnets in the current VPC is returned. This is AWS's default
     * behavior. All subnet IDs will be trimmed, and ignored if empty.
     *
     * @param emrClusterDefinition the definition specifying the subnet IDs
     * @param awsParamsDto the AWS related parameters for access/secret keys and proxy details
     *
     * @return the list of subnets
     */
    private List<Subnet> getSubnets(EmrClusterDefinition emrClusterDefinition, AwsParamsDto awsParamsDto)
    {
        String definitionSubnetId = emrClusterDefinition.getSubnetId();

        Set<String> subnetIds = Collections.emptySet();
        if (StringUtils.isNotBlank(definitionSubnetId))
        {
            subnetIds = herdStringHelper.splitAndTrim(definitionSubnetId, ",");
        }

        return ec2Dao.getSubnets(subnetIds, awsParamsDto);
    }

    /**
     * Returns the total cost per hour to run the requested number of instances for the given price. Returns the instance price multiplied by the number of
     * instances.
     *
     * @param ec2Price the EC2 pricing information
     *
     * @return the USD per hour
     */
    public BigDecimal getTotalCost(Ec2PriceDto ec2Price)
    {
        BigDecimal instancePrice = ec2Price.getInstancePrice();
        Integer instanceCount = ec2Price.getInstanceCount();
        return instancePrice.multiply(new BigDecimal(instanceCount));
    }
}
