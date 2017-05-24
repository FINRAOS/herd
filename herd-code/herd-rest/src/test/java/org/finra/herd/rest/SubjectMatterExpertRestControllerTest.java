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
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.SubjectMatterExpert;
import org.finra.herd.model.api.xml.SubjectMatterExpertContactDetails;
import org.finra.herd.model.api.xml.SubjectMatterExpertKey;
import org.finra.herd.service.SubjectMatterExpertService;

/**
 * This class tests various functionality within the subject matter expert REST controller.
 */
public class SubjectMatterExpertRestControllerTest extends AbstractRestTest
{
    @Mock
    private SubjectMatterExpertService subjectMatterExpertService;

    @InjectMocks
    private SubjectMatterExpertRestController subjectMatterExpertRestController;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetSubjectMatterExpert()
    {
        SubjectMatterExpert subjectMatterExpert = new SubjectMatterExpert(new SubjectMatterExpertKey(USER_ID),
            new SubjectMatterExpertContactDetails(USER_FULL_NAME, USER_JOB_TITLE, USER_EMAIL_ADDRESS, USER_TELEPHONE_NUMBER));

        when(subjectMatterExpertService.getSubjectMatterExpert(new SubjectMatterExpertKey(USER_ID))).thenReturn(subjectMatterExpert);

        // Get subject matter expert information.
        SubjectMatterExpert result = subjectMatterExpertRestController.getSubjectMatterExpert(USER_ID);
        // Verify the external calls.
        verify(subjectMatterExpertService).getSubjectMatterExpert(new SubjectMatterExpertKey(USER_ID));
        verifyNoMoreInteractions(subjectMatterExpertService);

        // Validate the returned object.
        assertEquals(subjectMatterExpert, result);
    }
}
