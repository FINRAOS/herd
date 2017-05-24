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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.SystemJobRunRequest;
import org.finra.herd.model.api.xml.SystemJobRunResponse;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.SystemJobService;
import org.finra.herd.service.systemjobs.FileUploadCleanupJob;
import org.finra.herd.service.systemjobs.JmsPublishingJob;
import org.finra.herd.service.systemjobs.StoragePolicySelectorJob;

/**
 * This class tests various functionality within the Job REST controller.
 */
public class SystemJobRestControllerTest extends AbstractRestTest
{
    @Mock
    private SystemJobService systemJobService;

    @InjectMocks
    private SystemJobRestController systemJobRestController;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testRunSystemJobFileUploadCleanup() throws Exception
    {
        // Create the system job run request.
        SystemJobRunRequest systemJobRunRequest = new SystemJobRunRequest(FileUploadCleanupJob.JOB_NAME,
            Arrays.asList(new Parameter(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES.getKey(), String.valueOf(INTEGER_VALUE))));

        SystemJobRunResponse systemJobRunResponse = new SystemJobRunResponse(FileUploadCleanupJob.JOB_NAME,
            Arrays.asList(new Parameter(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES.getKey(), String.valueOf(INTEGER_VALUE))));

        when(systemJobService.runSystemJob(systemJobRunRequest)).thenReturn(systemJobRunResponse);

        // Request to run the system job.
        SystemJobRunResponse resultSystemJobRunResponse = systemJobRestController.runSystemJob(systemJobRunRequest);

        // Verify the external calls.
        verify(systemJobService).runSystemJob(systemJobRunRequest);
        verifyNoMoreInteractions(systemJobService);

        // Validate the returned object.
        assertEquals(systemJobRunResponse, resultSystemJobRunResponse);
    }

    @Test
    public void testRunSystemJobJmsPublisher() throws Exception
    {
        // Create the system job run request.
        SystemJobRunRequest systemJobRunRequest = new SystemJobRunRequest(JmsPublishingJob.JOB_NAME, null);
        SystemJobRunResponse systemJobRunResponse = new SystemJobRunResponse(JmsPublishingJob.JOB_NAME, null);

        when(systemJobService.runSystemJob(systemJobRunRequest)).thenReturn(systemJobRunResponse);

        // Request to run the system job.
        SystemJobRunResponse resultSystemJobRunResponse = systemJobRestController.runSystemJob(systemJobRunRequest);

        // Verify the external calls.
        verify(systemJobService).runSystemJob(systemJobRunRequest);
        verifyNoMoreInteractions(systemJobService);

        // Validate the returned object.
        assertEquals(systemJobRunResponse, resultSystemJobRunResponse);
    }

    @Test
    public void testRunSystemJobStoragePolicySelector() throws Exception
    {
        // Create the system job run request.
        SystemJobRunRequest systemJobRunRequest = new SystemJobRunRequest(StoragePolicySelectorJob.JOB_NAME,
            Arrays.asList(new Parameter(ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_MAX_BDATA_INSTANCES.getKey(), String.valueOf(INTEGER_VALUE))));
        SystemJobRunResponse systemJobRunResponse = new SystemJobRunResponse(StoragePolicySelectorJob.JOB_NAME,
            Arrays.asList(new Parameter(ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_MAX_BDATA_INSTANCES.getKey(), String.valueOf(INTEGER_VALUE))));

        when(systemJobService.runSystemJob(systemJobRunRequest)).thenReturn(systemJobRunResponse);
        // Request to run the system job.
        SystemJobRunResponse resultSystemJobRunResponse = systemJobRestController.runSystemJob(systemJobRunRequest);

        // Verify the external calls.
        verify(systemJobService).runSystemJob(systemJobRunRequest);
        verifyNoMoreInteractions(systemJobService);

        // Validate the returned object.
        assertEquals(systemJobRunResponse, resultSystemJobRunResponse);
    }
}
