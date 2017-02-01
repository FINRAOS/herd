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
package org.finra.herd.service.helper;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.api.xml.InstanceDefinition;
import org.finra.herd.model.api.xml.MasterInstanceDefinition;
import org.finra.herd.model.api.xml.NodeTag;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * A helper class for EmrClusterDefinition related code.
 */
@Component
public class EmrClusterDefinitionHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private HerdStringHelper herdStringHelper;

    /**
     * Validates an EMR cluster definition configuration.
     *
     * @param emrClusterDefinition the EMR cluster definition configuration
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateEmrClusterDefinitionConfiguration(EmrClusterDefinition emrClusterDefinition) throws IllegalArgumentException
    {
        Assert.notNull(emrClusterDefinition, "An EMR cluster definition configuration must be specified.");

        Assert.isTrue(StringUtils.isNotBlank(emrClusterDefinition.getSubnetId()), "Subnet ID must be specified");
        for (String token : emrClusterDefinition.getSubnetId().split(","))
        {
            Assert.isTrue(StringUtils.isNotBlank(token), "No blank is allowed in the list of subnet IDs");
        }

        Assert.notNull(emrClusterDefinition.getInstanceDefinitions(), "Instance definitions must be specified.");

        // Check master instances.
        Assert.notNull(emrClusterDefinition.getInstanceDefinitions().getMasterInstances(), "Master instances must be specified.");
        validateMasterInstanceDefinition(emrClusterDefinition.getInstanceDefinitions().getMasterInstances());

        // Check core instances.
        if (emrClusterDefinition.getInstanceDefinitions().getCoreInstances() != null)
        {
            validateInstanceDefinition("core", emrClusterDefinition.getInstanceDefinitions().getCoreInstances(), 0);
            // If instance count is <= 0, remove the entire core instance definition since it is redundant.
            if (emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceCount() <= 0)
            {
                emrClusterDefinition.getInstanceDefinitions().setCoreInstances(null);
            }
        }

        // Check task instances
        if (emrClusterDefinition.getInstanceDefinitions().getTaskInstances() != null)
        {
            validateInstanceDefinition("task", emrClusterDefinition.getInstanceDefinitions().getTaskInstances(), 1);
        }

        // Check that total number of instances does not exceed the max allowed.
        int maxEmrInstanceCount = configurationHelper.getProperty(ConfigurationValue.MAX_EMR_INSTANCES_COUNT, Integer.class);
        if (maxEmrInstanceCount > 0)
        {
            int instancesRequested = emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceCount();
            if (emrClusterDefinition.getInstanceDefinitions().getCoreInstances() != null)
            {
                instancesRequested += emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceCount();
            }
            if (emrClusterDefinition.getInstanceDefinitions().getTaskInstances() != null)
            {
                instancesRequested += emrClusterDefinition.getInstanceDefinitions().getTaskInstances().getInstanceCount();
            }

            Assert.isTrue((maxEmrInstanceCount >= instancesRequested), "Total number of instances requested can not exceed : " + maxEmrInstanceCount);
        }

        // Validate node tags including checking for required tags and detecting any duplicate node tag names in case sensitive manner.
        Assert.notEmpty(emrClusterDefinition.getNodeTags(), "Node tags must be specified.");
        HashSet<String> nodeTagNameValidationSet = new HashSet<>();
        for (NodeTag nodeTag : emrClusterDefinition.getNodeTags())
        {
            Assert.hasText(nodeTag.getTagName(), "A node tag name must be specified.");
            Assert.hasText(nodeTag.getTagValue(), "A node tag value must be specified.");
            Assert.isTrue(!nodeTagNameValidationSet.contains(nodeTag.getTagName()), String.format("Duplicate node tag \"%s\" is found.", nodeTag.getTagName()));
            nodeTagNameValidationSet.add(nodeTag.getTagName());
        }

        // Validate the mandatory AWS tags are there
        for (String mandatoryTag : herdStringHelper.splitStringWithDefaultDelimiter(configurationHelper.getProperty(ConfigurationValue.MANDATORY_AWS_TAGS)))
        {
            Assert.isTrue(nodeTagNameValidationSet.contains(mandatoryTag), String.format("Mandatory AWS tag not specified: \"%s\"", mandatoryTag));
        }

        emrClusterDefinition.setAdditionalMasterSecurityGroups(
            assertNotBlankAndTrim(emrClusterDefinition.getAdditionalMasterSecurityGroups(), "additionalMasterSecurityGroup"));

        emrClusterDefinition
            .setAdditionalSlaveSecurityGroups(assertNotBlankAndTrim(emrClusterDefinition.getAdditionalSlaveSecurityGroups(), "additionalSlaveSecurityGroup"));

        // Fail if security configuration is specified for EMR version less than 4.8.0.
        if (StringUtils.isNotBlank(emrClusterDefinition.getSecurityConfiguration()))
        {
            final DefaultArtifactVersion securityConfigurationMinEmrVersion = new DefaultArtifactVersion("4.8.0");
            Assert.isTrue(StringUtils.isNotBlank(emrClusterDefinition.getReleaseLabel()) &&
                securityConfigurationMinEmrVersion.compareTo(new DefaultArtifactVersion(emrClusterDefinition.getReleaseLabel())) <= 0,
                "EMR security configuration is not supported prior to EMR release 4.8.0.");
        }
    }

    /**
     * Validates the EMR cluster definition key. This method also trims the key parameters.
     *
     * @param key the EMR cluster definition key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateEmrClusterDefinitionKey(EmrClusterDefinitionKey key) throws IllegalArgumentException
    {
        Assert.notNull(key, "An EMR cluster definition key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setEmrClusterDefinitionName(alternateKeyHelper.validateStringParameter("An", "EMR cluster definition name", key.getEmrClusterDefinitionName()));
    }

    /**
     * Asserts that the given list of string contains no blank string and returns a copy of the list with all the values trimmed. If the given list is null, a
     * null is returned.
     *
     * @param list The list of string
     * @param displayName The display name of the element in the list used to construct the error message
     *
     * @return Copy of list with trimmed values
     */
    private List<String> assertNotBlankAndTrim(List<String> list, String displayName)
    {
        List<String> trimmed = null;
        if (list != null)
        {
            for (String string : list)
            {
                Assert.hasText(string, displayName + " must not be blank");
            }
            trimmed = new ArrayList<>();
            for (String string : list)
            {
                trimmed.add(string.trim());
            }
        }
        return trimmed;
    }

    /**
     * Validates the given instance definition. Generates an appropriate error message using the given name. The name specified is one of "master", "core", or
     * "task".
     *
     * @param name name of instance group
     * @param instanceDefinition the instance definition to validate
     * @param minimumInstanceCount The minimum instance count.
     *
     * @throws IllegalArgumentException when any validation error occurs
     */
    private void validateInstanceDefinition(String name, InstanceDefinition instanceDefinition, Integer minimumInstanceCount)
    {
        String capitalizedName = StringUtils.capitalize(name);

        Assert.isTrue(instanceDefinition.getInstanceCount() >= minimumInstanceCount,
            String.format("At least %d %s instance must be specified.", minimumInstanceCount, name));
        Assert.hasText(instanceDefinition.getInstanceType(), "An instance type for " + name + " instances must be specified.");

        if (instanceDefinition.getInstanceSpotPrice() != null)
        {
            Assert.isNull(instanceDefinition.getInstanceMaxSearchPrice(),
                capitalizedName + " instance max search price must not be specified when instance spot price is specified.");

            Assert.isTrue(instanceDefinition.getInstanceSpotPrice().compareTo(BigDecimal.ZERO) > 0,
                capitalizedName + " instance spot price must be greater than 0");
        }

        if (instanceDefinition.getInstanceMaxSearchPrice() != null)
        {
            Assert.isNull(instanceDefinition.getInstanceSpotPrice(),
                capitalizedName + " instance spot price must not be specified when max search price is specified.");

            Assert.isTrue(instanceDefinition.getInstanceMaxSearchPrice().compareTo(BigDecimal.ZERO) > 0,
                capitalizedName + " instance max search price must be greater than 0");

            if (instanceDefinition.getInstanceOnDemandThreshold() != null)
            {
                Assert.isTrue(instanceDefinition.getInstanceOnDemandThreshold().compareTo(BigDecimal.ZERO) > 0,
                    capitalizedName + " instance on-demand threshold must be greater than 0");
            }
        }
        else
        {
            Assert.isNull(instanceDefinition.getInstanceOnDemandThreshold(),
                capitalizedName + " instance on-demand threshold must not be specified when instance max search price is not specified.");
        }
    }

    /**
     * Converts the given master instance definition to a generic instance definition and delegates to validateInstanceDefinition(). Generates an appropriate
     * error message using the name "master".
     *
     * @param masterInstanceDefinition the master instance definition to validate
     *
     * @throws IllegalArgumentException when any validation error occurs
     */
    private void validateMasterInstanceDefinition(MasterInstanceDefinition masterInstanceDefinition)
    {
        InstanceDefinition instanceDefinition = new InstanceDefinition();
        instanceDefinition.setInstanceCount(masterInstanceDefinition.getInstanceCount());
        instanceDefinition.setInstanceMaxSearchPrice(masterInstanceDefinition.getInstanceMaxSearchPrice());
        instanceDefinition.setInstanceOnDemandThreshold(masterInstanceDefinition.getInstanceOnDemandThreshold());
        instanceDefinition.setInstanceSpotPrice(masterInstanceDefinition.getInstanceSpotPrice());
        instanceDefinition.setInstanceType(masterInstanceDefinition.getInstanceType());
        validateInstanceDefinition("master", instanceDefinition, 1);
    }
}
