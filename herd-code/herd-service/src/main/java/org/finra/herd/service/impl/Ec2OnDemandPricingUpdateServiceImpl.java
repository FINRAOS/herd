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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.Ec2OnDemandPricingDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.helper.UrlHelper;
import org.finra.herd.model.dto.Ec2OnDemandPricing;
import org.finra.herd.model.dto.Ec2OnDemandPricingKey;
import org.finra.herd.model.jpa.Ec2OnDemandPricingEntity;
import org.finra.herd.service.Ec2OnDemandPricingUpdateService;

/**
 * An implementation of the service that updates EC2 on-demand pricing.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class Ec2OnDemandPricingUpdateServiceImpl implements Ec2OnDemandPricingUpdateService
{
    static final String JSON_ATTRIBUTE_NAME_INSTANCE_TYPE = "instanceType";

    static final String JSON_ATTRIBUTE_NAME_LOCATION = "location";

    static final String JSON_ATTRIBUTE_NAME_OPERATING_SYSTEM = "operatingSystem";

    static final String JSON_ATTRIBUTE_NAME_PRE_INSTALLED_SOFTWARE = "preInstalledSw";

    static final String JSON_ATTRIBUTE_NAME_TENANCY = "tenancy";

    static final String JSON_ATTRIBUTE_NAME_USAGE_TYPE = "usagetype";

    static final String JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM = "Linux";

    static final String JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE = "NA";

    static final String JSON_ATTRIBUTE_VALUE_TENANCY = "Shared";

    static final String JSON_KEY_NAME_ATTRIBUTES = "attributes";

    static final String JSON_KEY_NAME_ON_DEMAND = "OnDemand";

    static final String JSON_KEY_NAME_PRICE_DIMENSIONS = "priceDimensions";

    static final String JSON_KEY_NAME_PRICE_PER_UNIT = "pricePerUnit";

    static final String JSON_KEY_NAME_PRODUCTS = "products";

    static final String JSON_KEY_NAME_SKU = "sku";

    static final String JSON_KEY_NAME_TERMS = "terms";

    static final String JSON_PRICE_DIMENSIONS_WRAPPER_SUFFIX = ".6YS6EN2CT7";

    static final String JSON_PRICE_PER_UNIT_WRAPPER = "USD";

    static final String JSON_SKU_WRAPPER_SUFFIX = ".JRTCKXETXF";

    private static final Logger LOGGER = LoggerFactory.getLogger(Ec2OnDemandPricingUpdateServiceImpl.class);

    @Autowired
    private Ec2OnDemandPricingDao ec2OnDemandPricingDao;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private UrlHelper urlHelper;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation executes non-transactionally, suspends the current transaction if one exists.
     */
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public List<Ec2OnDemandPricing> getEc2OnDemandPricing(String ec2PricingListUrl)
    {
        // Create a list of EC2 on-demand pricing entries.
        List<Ec2OnDemandPricing> ec2OnDemandPricingEntries = new ArrayList<>();

        // Get JSON object from the specified URL.
        JSONObject jsonObject = urlHelper.parseJsonObjectFromUrl(ec2PricingListUrl);

        // Get products from the JSON object.
        JSONObject products = jsonHelper.getKeyValue(jsonObject, JSON_KEY_NAME_PRODUCTS, JSONObject.class);

        // Create a set to validate uniqueness of EC2 on-demand pricing keys.
        Set<Ec2OnDemandPricingKey> uniqueEc2OnDemandPricingKeys = new HashSet<>();

        // Process all products.
        for (Object key : products.keySet())
        {
            JSONObject current = jsonHelper.getKeyValue(products, key, JSONObject.class);
            String sku = jsonHelper.getKeyValue(current, JSON_KEY_NAME_SKU, String.class);
            JSONObject attributes = jsonHelper.getKeyValue(current, JSON_KEY_NAME_ATTRIBUTES, JSONObject.class);

            Object location = attributes.get(JSON_ATTRIBUTE_NAME_LOCATION);
            Object operatingSystem = attributes.get(JSON_ATTRIBUTE_NAME_OPERATING_SYSTEM);
            Object instanceType = attributes.get(JSON_ATTRIBUTE_NAME_INSTANCE_TYPE);
            Object tenancy = attributes.get(JSON_ATTRIBUTE_NAME_TENANCY);
            Object usageType = attributes.get(JSON_ATTRIBUTE_NAME_USAGE_TYPE);
            Object preInstalledSoftware = attributes.get(JSON_ATTRIBUTE_NAME_PRE_INSTALLED_SOFTWARE);

            // Validate the parameters and create an EC2 on-demand pricing entry.
            Ec2OnDemandPricing ec2OnDemandPricing =
                createEc2OnDemandPricingEntry(sku, location, operatingSystem, instanceType, tenancy, usageType, preInstalledSoftware);

            // Check if this EC2 on-demand pricing entry got created (the relative parameters passed validation checks).
            if (ec2OnDemandPricing != null)
            {
                // Get the EC2 on-demand pricing key.
                Ec2OnDemandPricingKey ec2OnDemandPricingKey = ec2OnDemandPricing.getEc2OnDemandPricingKey();

                // Validate that this key is unique.
                if (!uniqueEc2OnDemandPricingKeys.add(ec2OnDemandPricingKey))
                {
                    throw new IllegalArgumentException(String
                        .format("Found duplicate EC2 on-demand pricing entry for \"%s\" AWS region and \"%s\" EC2 instance type.",
                            ec2OnDemandPricingKey.getRegionName(), ec2OnDemandPricingKey.getInstanceType()));
                }

                // Add this EC2 on-demand pricing entry to the result list.
                ec2OnDemandPricingEntries.add(ec2OnDemandPricing);
            }
        }

        // Continue the processing only when the result list is not empty.
        if (CollectionUtils.isNotEmpty(ec2OnDemandPricingEntries))
        {
            // Get terms from the JSON object.
            JSONObject terms = jsonHelper.getKeyValue(jsonObject, JSON_KEY_NAME_TERMS, JSONObject.class);

            // Get on-demand information from the terms.
            JSONObject onDemand = jsonHelper.getKeyValue(terms, JSON_KEY_NAME_ON_DEMAND, JSONObject.class);

            // Populate pricing information.
            for (Ec2OnDemandPricing ec2OnDemandPricing : ec2OnDemandPricingEntries)
            {
                String sku = ec2OnDemandPricing.getSku();
                JSONObject current = jsonHelper.getKeyValue(onDemand, sku, JSONObject.class);
                JSONObject pricingWrapper = jsonHelper.getKeyValue(current, sku + JSON_SKU_WRAPPER_SUFFIX, JSONObject.class);
                JSONObject priceDimensions = jsonHelper.getKeyValue(pricingWrapper, JSON_KEY_NAME_PRICE_DIMENSIONS, JSONObject.class);
                JSONObject innerPricingWrapper =
                    jsonHelper.getKeyValue(priceDimensions, sku + JSON_SKU_WRAPPER_SUFFIX + JSON_PRICE_DIMENSIONS_WRAPPER_SUFFIX, JSONObject.class);
                JSONObject pricePerUnit = jsonHelper.getKeyValue(innerPricingWrapper, JSON_KEY_NAME_PRICE_PER_UNIT, JSONObject.class);
                String pricePerUnitValue = jsonHelper.getKeyValue(pricePerUnit, JSON_PRICE_PER_UNIT_WRAPPER, String.class);

                try
                {
                    ec2OnDemandPricing.setPricePerHour(new BigDecimal(pricePerUnitValue));
                }
                catch (NumberFormatException e)
                {
                    throw new IllegalArgumentException(String.format("Failed to convert \"%s\" value to %s.", pricePerUnitValue, BigDecimal.class.getName()),
                        e);
                }
            }
        }

        return ec2OnDemandPricingEntries;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateEc2OnDemandPricing(List<Ec2OnDemandPricing> ec2OnDemandPricingEntries)
    {
        // Get all EC2 on-demand price entities from the database.
        List<Ec2OnDemandPricingEntity> ec2OnDemandPricingEntities = ec2OnDemandPricingDao.getEc2OnDemandPricingEntities();

        // Load all retrieved entities into a map for easy access. Per database constraints, the EC2 on-demand price key values are unique.
        Map<Ec2OnDemandPricingKey, Ec2OnDemandPricingEntity> ec2OnDemandPricingEntitiesMap = new HashMap<>();
        for (Ec2OnDemandPricingEntity ec2OnDemandPricingEntity : ec2OnDemandPricingEntities)
        {
            ec2OnDemandPricingEntitiesMap
                .put(new Ec2OnDemandPricingKey(ec2OnDemandPricingEntity.getRegionName(), ec2OnDemandPricingEntity.getInstanceType()), ec2OnDemandPricingEntity);
        }

        // Create empty lists for unchanged and updated EC2 on-demand pricing entities.
        List<Ec2OnDemandPricingEntity> unchangedEc2OnDemandPricingEntities = new ArrayList<>();
        List<Ec2OnDemandPricingEntity> updatedEc2OnDemandPricingEntities = new ArrayList<>();

        // Process the submitted list of EC2 on-demand prices.
        for (Ec2OnDemandPricing ec2OnDemandPricing : ec2OnDemandPricingEntries)
        {
            // Get the EC2 on-demand pricing key.
            Ec2OnDemandPricingKey ec2OnDemandPricingKey = ec2OnDemandPricing.getEc2OnDemandPricingKey();

            // Check if this entry already exists in the database.
            if (ec2OnDemandPricingEntitiesMap.containsKey(ec2OnDemandPricingKey))
            {
                // Get the relative database entity.
                Ec2OnDemandPricingEntity ec2OnDemandPricingEntity = ec2OnDemandPricingEntitiesMap.get(ec2OnDemandPricingKey);

                // Check if this entity needs to be updated.
                if (!ec2OnDemandPricing.getPricePerHour().equals(ec2OnDemandPricingEntity.getHourlyPrice()))
                {
                    // Get the original hourly price.
                    String oldHourlyPriceAsString = ec2OnDemandPricingEntity.getHourlyPrice().toPlainString();

                    // Update the entity.
                    ec2OnDemandPricingEntity.setHourlyPrice(ec2OnDemandPricing.getPricePerHour());
                    ec2OnDemandPricingDao.saveAndRefresh(ec2OnDemandPricingEntity);

                    // Add this entity to the list of updated entities.
                    updatedEc2OnDemandPricingEntities.add(ec2OnDemandPricingEntity);

                    // Log the update.
                    LOGGER.info("Updated EC2 on-demand pricing: regionName=\"{}\" instanceType=\"{}\" newHourlyPrice={} oldHourlyPrice={}",
                        ec2OnDemandPricingKey.getRegionName(), ec2OnDemandPricingKey.getInstanceType(), ec2OnDemandPricing.getPricePerHour().toPlainString(),
                        oldHourlyPriceAsString);
                }
                else
                {
                    // Add this entity to the list of unchanged entities.
                    unchangedEc2OnDemandPricingEntities.add(ec2OnDemandPricingEntity);
                }
            }
            else
            {
                // Create and persist a new EC2 on-demand pricing entity.
                Ec2OnDemandPricingEntity ec2OnDemandPricingEntity = new Ec2OnDemandPricingEntity();
                ec2OnDemandPricingEntity.setRegionName(ec2OnDemandPricingKey.getRegionName());
                ec2OnDemandPricingEntity.setInstanceType(ec2OnDemandPricingKey.getInstanceType());
                ec2OnDemandPricingEntity.setHourlyPrice(ec2OnDemandPricing.getPricePerHour());
                ec2OnDemandPricingDao.saveAndRefresh(ec2OnDemandPricingEntity);

                // Log the addition of the new EC2 on-demand pricing entity.
                LOGGER.info("Added EC2 on-demand pricing: regionName=\"{}\" instanceType=\"{}\" hourlyPrice={}", ec2OnDemandPricingKey.getRegionName(),
                    ec2OnDemandPricingKey.getInstanceType(), ec2OnDemandPricing.getPricePerHour().toPlainString());
            }
        }

        // Get a list of entities that needs to be deleted.
        Set<Ec2OnDemandPricingEntity> deletedEc2OnDemandPricingEntities = new HashSet<>(ec2OnDemandPricingEntities);
        deletedEc2OnDemandPricingEntities.removeAll(unchangedEc2OnDemandPricingEntities);
        deletedEc2OnDemandPricingEntities.removeAll(updatedEc2OnDemandPricingEntities);

        // Delete the entities that were not on the list of EC2 on-demand prices.
        for (Ec2OnDemandPricingEntity ec2OnDemandPricingEntity : deletedEc2OnDemandPricingEntities)
        {
            // Delete the entity.
            ec2OnDemandPricingDao.delete(ec2OnDemandPricingEntity);

            // Log the deletion of the EC2 on-demand pricing entity.
            LOGGER.info("Deleted EC2 on-demand pricing: regionName=\"{}\" instanceType=\"{}\" hourlyPrice={}", ec2OnDemandPricingEntity.getRegionName(),
                ec2OnDemandPricingEntity.getInstanceType(), ec2OnDemandPricingEntity.getHourlyPrice().toPlainString());
        }
    }

    /**
     * Converts location description to a short region name.
     *
     * @param location the location description
     *
     * @return the region name
     */
    String convertLocationToRegionName(String location)
    {
        String region;

        switch (location)
        {
            case "US East (N. Virginia)":
                region = "us-east-1";
                break;
            case "US East (Ohio)":
                region = "us-east-2";
                break;
            case "US West (N. California)":
                region = "us-west-1";
                break;
            case "US West (Oregon)":
                region = "us-west-2";
                break;
            case "Canada (Central)":
                region = "ca-central-1";
                break;
            case "Asia Pacific (Mumbai)":
                region = "ap-south-1";
                break;
            case "Asia Pacific (Seoul)":
                region = "ap-northeast-2";
                break;
            case "Asia Pacific (Singapore)":
                region = "ap-southeast-1";
                break;
            case "Asia Pacific (Sydney)":
                region = "ap-southeast-2";
                break;
            case "Asia Pacific (Tokyo)":
                region = "ap-northeast-1";
                break;
            case "EU (Frankfurt)":
                region = "eu-central-1";
                break;
            case "EU (Ireland)":
                region = "eu-west-1";
                break;
            case "EU (London)":
                region = "eu-west-2";
                break;
            case "South America (Sao Paulo)":
                region = "sa-east-1";
                break;
            case "AWS GovCloud (US)":
                region = "us-gov-west-1";
                break;
            default:
                // Default region name to the location description.
                region = location;
                break;
        }

        return region;
    }

    /**
     * Creates an EC2 on-demand pricing entry per specified parameters. This method returns null if input parameters fail validation.
     *
     * @param sku the SKU of the AWS product offering
     * @param location the AWS location information, maybe null
     * @param operatingSystem the operation system, maybe null
     * @param instanceType the EC2 instance type, maybe null
     * @param tenancy the EC2 tenancy, maybe null
     * @param usageType the usage type, maybe null
     * @param preInstalledSoftware the pre-installed software, maybe null
     *
     * @return the EC2 on-demand pricing or null if input parameters fail validation
     */
    Ec2OnDemandPricing createEc2OnDemandPricingEntry(String sku, Object location, Object operatingSystem, Object instanceType, Object tenancy, Object usageType,
        Object preInstalledSoftware)
    {
        Ec2OnDemandPricing result = null;

        // The extra check for usage type is added below to exclude any dedicated/reserved hosts that are marked as having "Shared" tenancy by mistake.
        if (location != null && operatingSystem != null && instanceType != null && tenancy != null && usageType != null && preInstalledSoftware != null &&
            StringUtils.isNotBlank(location.toString()) && operatingSystem.toString().equalsIgnoreCase(JSON_ATTRIBUTE_VALUE_OPERATING_SYSTEM) &&
            StringUtils.isNotBlank(instanceType.toString()) && tenancy.toString().equalsIgnoreCase(JSON_ATTRIBUTE_VALUE_TENANCY) &&
            (usageType.toString().startsWith("BoxUsage") || usageType.toString().contains("-BoxUsage")) &&
            preInstalledSoftware.toString().equalsIgnoreCase(JSON_ATTRIBUTE_VALUE_PRE_INSTALLED_SOFTWARE))
        {
            result = new Ec2OnDemandPricing(new Ec2OnDemandPricingKey(convertLocationToRegionName(location.toString()), instanceType.toString()), null, sku);
        }

        return result;
    }
}
