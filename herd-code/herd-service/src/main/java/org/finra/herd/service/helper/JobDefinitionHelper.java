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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.JobDefinitionAlternateKeyDto;

/**
 * A helper class for StorageFile related code.
 */
@Component
public class JobDefinitionHelper
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * Returns Activiti Id constructed according to the template defined.
     *
     * @param namespaceCd the namespace code value
     * @param jobName the job name value
     *
     * @return the Activiti Id
     */
    public String buildActivitiIdString(String namespaceCd, String jobName)
    {
        // Populate a map with the tokens mapped to actual database values.
        Map<String, String> pathToTokenValueMap = new HashMap<>();
        pathToTokenValueMap.put(getNamespaceToken(), namespaceCd);
        pathToTokenValueMap.put(getJobNameToken(), jobName);

        // The Activiti Id will start as the template.
        String activitiId = getActivitiJobDefinitionTemplate();

        // Substitute the tokens with the actual database values.
        for (Map.Entry<String, String> mapEntry : pathToTokenValueMap.entrySet())
        {
            activitiId = activitiId.replaceAll(mapEntry.getKey(), mapEntry.getValue());
        }

        // Return the final Activiti Id.
        return activitiId;
    }

    /**
     * Gets the Activiti job definition template.
     *
     * @return the Activiti job definition template.
     */
    public String getActivitiJobDefinitionTemplate()
    {
        // Set the default Activiti Id tokenized template.
        // ~namespace~.~jobName~
        String defaultActivitiIdTemplate = getNamespaceToken() + "." + getJobNameToken();

        // Get the Activiti Id template from the environment, but use the default if one isn't configured.
        // This gives us the ability to customize/change the format post deployment.
        String template = configurationHelper.getProperty(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE);

        if (template == null)
        {
            template = defaultActivitiIdTemplate;
        }

        return template;
    }

    /**
     * Gets the job name token.
     *
     * @return the job name token.
     */
    public String getJobNameToken()
    {
        // Set the token delimiter based on the environment configuration.
        String tokenDelimiter = configurationHelper.getProperty(ConfigurationValue.TEMPLATE_TOKEN_DELIMITER);

        // Setup the individual token names (using the configured delimiter).
        return tokenDelimiter + "jobName" + tokenDelimiter;
    }

    /**
     * Gets the Regex pattern to match the namespace and job name from a process definition key.
     *
     * @return the Regex pattern
     */
    public Pattern getNamespaceAndJobNameRegexPattern()
    {
        // Get the job definition template (e.g. ~namespace~.~jobName~) and Regex escape all periods (e.g. ~namespace~\\.~jobName~).
        String jobDefinitionTemplate = getActivitiJobDefinitionTemplate().replaceAll("\\.", "\\\\.");

        // Change the tokens to named capture groups (e.g. (?<namespace>.*)\\.(?<jobName>.*)).
        jobDefinitionTemplate = jobDefinitionTemplate.replaceAll(getNamespaceToken(), "(?<namespace>.*)");
        jobDefinitionTemplate = jobDefinitionTemplate.replaceAll(getJobNameToken(), "(?<jobName>.*)");

        // Compile the Regex pattern.
        return Pattern.compile(jobDefinitionTemplate);
    }

    /**
     * Gets a job definition key using namespace and name of the the job definition extracted from the specified process definition key using the Regex
     * pattern.
     *
     * @param processDefinitionKey the process definition key
     *
     * @return the job definition key
     */
    public JobDefinitionAlternateKeyDto getJobDefinitionKey(String processDefinitionKey)
    {
        return getJobDefinitionKey(processDefinitionKey, getNamespaceAndJobNameRegexPattern());
    }

    /**
     * Gets a job definition key using namespace and name of the the job definition extracted from the specified process definition key and the Regex pattern.
     *
     * @param processDefinitionKey the process definition key
     * @param regexPattern the Regex pattern
     *
     * @return the job definition key
     */
    public JobDefinitionAlternateKeyDto getJobDefinitionKey(String processDefinitionKey, Pattern regexPattern)
    {
        // Create a job definition key.
        JobDefinitionAlternateKeyDto jobDefinitionKey = new JobDefinitionAlternateKeyDto();

        // Use the Regex pattern to match the namespace and job name from a process definition key.
        Matcher matcher = regexPattern.matcher(processDefinitionKey);
        if (matcher.find())
        {
            jobDefinitionKey.setNamespace(matcher.group("namespace"));
            jobDefinitionKey.setJobName(matcher.group("jobName"));
        }
        else
        {
            // Under normal operation, this exception would never be thrown. The only possibility is if someone creates and executes
            // a workflow without using our enforced definition key format. The only way to accomplish this is directly operating
            // against the database. The check is here for robustness, but should not be expected to be covered as part of tests.
            throw new IllegalArgumentException(
                String.format("Process definition key \"%s\" does not match the expected pattern \"%s\".", processDefinitionKey, regexPattern.toString()));
        }

        return jobDefinitionKey;
    }

    /**
     * Gets the namespace token.
     *
     * @return the namespace token.
     */
    public String getNamespaceToken()
    {
        // Set the token delimiter based on the environment configuration.
        String tokenDelimiter = configurationHelper.getProperty(ConfigurationValue.TEMPLATE_TOKEN_DELIMITER);

        // Setup the individual token names (using the configured delimiter).
        return tokenDelimiter + "namespace" + tokenDelimiter;
    }

    /**
     * Masks the value of the given parameter if the parameter is a password. If the parameter has the name which contains the string "password"
     * case-insensitive, the value will be replaced with ****.
     *
     * @param parameter {@link org.finra.herd.model.api.xml.Parameter} to mask
     */
    public void maskPassword(Parameter parameter)
    {
        if (parameter.getName() != null)
        {
            String name = parameter.getName().toUpperCase();
            if (name.contains("PASSWORD"))
            {
                parameter.setValue("****");
            }
        }
    }
}
