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
package org.finra.herd.tools.access.validator;

import static org.finra.herd.tools.access.validator.PropertiesHelper.AWS_REGION_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.AWS_ROLE_ARN_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.AWS_SQS_QUEUE_URL_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_BASE_URL_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_PASSWORD_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_USERNAME_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.NAMESPACE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.PRIMARY_PARTITION_VALUE_PROPERTY;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.sdk.invoker.ApiException;

public class HerdApiClientOperationsTest
{
    @InjectMocks
    private HerdApiClientOperations herdApiClientOperations;

    @Mock
    private PropertiesHelper propertiesHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testMapJsontoBdataKey() throws Exception
    {
        String fileName = "sqsMessage.txt";
        String expectedNamespace = "DMFormatNamespace1";

        Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());

        Stream<String> lines = Files.lines(path);
        String messageBody = lines.collect(Collectors.joining("\n")).trim();
        lines.close();

        BusinessObjectDataKey bdataKey = herdApiClientOperations.mapJsontoBdataKey(messageBody).getBusinessObjectDataKey();

        Assert.assertEquals("Did not get the correct namespace", expectedNamespace, bdataKey.getNamespace());
    }

    @Test
    public void testMissingProperties() throws Exception
    {
        String errorMessage = "";

        List<String> requiredProperties = Arrays
            .asList(HERD_BASE_URL_PROPERTY, HERD_USERNAME_PROPERTY, HERD_PASSWORD_PROPERTY, AWS_ROLE_ARN_PROPERTY, AWS_REGION_PROPERTY, NAMESPACE_PROPERTY,
                BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY, BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY, BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY,
                PRIMARY_PARTITION_VALUE_PROPERTY);

        for (String property : requiredProperties)
        {
            when(propertiesHelper.isBlankOrNull(property)).thenReturn(true);
        }

        try
        {
            herdApiClientOperations.checkPropertiesFile(propertiesHelper, false);
        }
        catch (ApiException e)
        {
            errorMessage = e.getMessage();
        }

        System.out.println(errorMessage);
        for (String property : requiredProperties)
        {
            Assert.assertTrue("Could not find error for " + property, StringUtils.contains(errorMessage, property));
        }

        for (String property : requiredProperties)
        {
            verify(propertiesHelper).isBlankOrNull(property);
        }

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testMissingPropertiesSqs() throws Exception
    {
        String errorMessage = "";

        List<String> requiredProperties =
            Arrays.asList(HERD_BASE_URL_PROPERTY, HERD_USERNAME_PROPERTY, HERD_PASSWORD_PROPERTY, AWS_ROLE_ARN_PROPERTY, AWS_REGION_PROPERTY);

        for (String property : requiredProperties)
        {
            when(propertiesHelper.isBlankOrNull(property)).thenReturn(false);
        }

        try
        {
            when(propertiesHelper.isBlankOrNull(AWS_SQS_QUEUE_URL_PROPERTY)).thenReturn(true);
            herdApiClientOperations.checkPropertiesFile(propertiesHelper, true);
        }
        catch (ApiException e)
        {
            errorMessage = e.getMessage();
        }

        System.out.println(errorMessage);
        for (String property : requiredProperties)
        {
            Assert.assertFalse("Unexpected error for " + property, StringUtils.contains(errorMessage, property));
        }

        Assert.assertTrue("Could not find error for " + AWS_SQS_QUEUE_URL_PROPERTY, StringUtils.contains(errorMessage, AWS_SQS_QUEUE_URL_PROPERTY));

        for (String property : requiredProperties)
        {
            verify(propertiesHelper).isBlankOrNull(property);
        }

        verify(propertiesHelper).isBlankOrNull(AWS_SQS_QUEUE_URL_PROPERTY);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(propertiesHelper);
    }
}
