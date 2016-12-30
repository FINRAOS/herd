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
package org.finra.herd.rest;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ThreadLocalRandom;

import javax.xml.datatype.XMLGregorianCalendar;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.scheduling.annotation.AsyncResult;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionIndexResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionValidateResponse;
import org.finra.herd.service.BusinessObjectDefinitionService;

/**
 * This class tests search index functionality within the business object definition REST controller. This separate test class was created because this one uses
 * a mock business object definition service.
 */
public class BusinessObjectDefinitionRestControllerIndexTest extends AbstractRestTest
{
    @InjectMocks
    private BusinessObjectDefinitionRestController businessObjectDefinitionRestController;

    @Mock
    private BusinessObjectDefinitionService businessObjectDefinitionService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }


    @Test
    public void testIndexBusinessObjectDefinitions()
    {
        // Mock the call to the business object definition service
        when(businessObjectDefinitionService.indexAllBusinessObjectDefinitions()).thenReturn(new AsyncResult<>(null));

        // Create a business object definition.
        BusinessObjectDefinitionIndexResponse businessObjectDefinitionIndexResponse = businessObjectDefinitionRestController.indexBusinessObjectDefinitions();

        // Verify the method call to businessObjectDefinitionService.indexAllBusinessObjectDefinitions()
        verify(businessObjectDefinitionService, times(1)).indexAllBusinessObjectDefinitions();

        // Validate the returned object.
        assertThat("Business object definition index response was null.", businessObjectDefinitionIndexResponse, not(nullValue()));
        assertThat("Business object definition index response index start time was null.", businessObjectDefinitionIndexResponse.getIndexStartTime(),
            not(nullValue()));
        assertThat("Business object definition index response index start time was not an instance of XMLGregorianCalendar.class.",
            businessObjectDefinitionIndexResponse.getIndexStartTime(), instanceOf(XMLGregorianCalendar.class));
    }

    @Test
    public void testValidateIndexBusinessObjectDefinitions()
    {
        // Randomly valid half the time
        boolean isSizeCheckValid = ThreadLocalRandom.current().nextDouble() < 0.5;
        boolean isSpotCheckPercentageValid = ThreadLocalRandom.current().nextDouble() < 0.5;
        boolean isSpotCheckRecentValid = ThreadLocalRandom.current().nextDouble() < 0.5;

        // Mock the call to the business object definition service
        when(businessObjectDefinitionService.indexValidateAllBusinessObjectDefinitions()).thenReturn(new AsyncResult<>(null));
        when(businessObjectDefinitionService.indexSizeCheckValidationBusinessObjectDefinitions()).thenReturn(isSizeCheckValid);
        when(businessObjectDefinitionService.indexSpotCheckPercentageValidationBusinessObjectDefinitions()).thenReturn(isSpotCheckPercentageValid);
        when(businessObjectDefinitionService.indexSpotCheckMostRecentValidationBusinessObjectDefinitions()).thenReturn(isSpotCheckRecentValid);

        // Create a business object definition.
        BusinessObjectDefinitionValidateResponse businessObjectDefinitionValidateResponse =
            businessObjectDefinitionRestController.validateIndexBusinessObjectDefinitions();

        // Verify the method call to businessObjectDefinitionService index validate methods
        verify(businessObjectDefinitionService, times(1)).indexValidateAllBusinessObjectDefinitions();
        verify(businessObjectDefinitionService, times(1)).indexSizeCheckValidationBusinessObjectDefinitions();
        verify(businessObjectDefinitionService, times(1)).indexSpotCheckPercentageValidationBusinessObjectDefinitions();
        verify(businessObjectDefinitionService, times(1)).indexSpotCheckMostRecentValidationBusinessObjectDefinitions();

        // Validate the returned object.
        assertThat("Business object definition validate response was null.", businessObjectDefinitionValidateResponse, not(nullValue()));
        assertThat("Business object definition validate response index start time was null.", businessObjectDefinitionValidateResponse.getValidateStartTime(),
            not(nullValue()));
        assertThat("Business object definition validate response index start time was not an instance of XMLGregorianCalendar.class.",
            businessObjectDefinitionValidateResponse.getValidateStartTime(), instanceOf(XMLGregorianCalendar.class));
        assertThat("Business object definition validate response index size check passed is not true.",
            businessObjectDefinitionValidateResponse.isSizeCheckPassed(), is(isSizeCheckValid));
        assertThat("Business object definition validate response index spot check random passed is not true.",
            businessObjectDefinitionValidateResponse.isSpotCheckRandomPassed(), is(isSpotCheckPercentageValid));
        assertThat("Business object definition validate response index spot check most recent passed is not true.",
            businessObjectDefinitionValidateResponse.isSpotCheckMostRecentPassed(), is(isSpotCheckRecentValid));
    }
}