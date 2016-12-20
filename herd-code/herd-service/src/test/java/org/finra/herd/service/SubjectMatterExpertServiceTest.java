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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Ignore;
import org.junit.Test;

import org.finra.herd.model.api.xml.SubjectMatterExpert;
import org.finra.herd.model.api.xml.SubjectMatterExpertContactDetails;
import org.finra.herd.model.api.xml.SubjectMatterExpertKey;

/**
 * This class tests various functionality within the subject matter expert service.
 */
public class SubjectMatterExpertServiceTest extends AbstractServiceTest
{
    @Test
    @Ignore
    public void testGetSubjectMatterExpert() throws Exception
    {
        // Get subject matter expert information.
        SubjectMatterExpert result = subjectMatterExpertService.getSubjectMatterExpert(new SubjectMatterExpertKey(USER_ID));

        // Validate the returned object.
        assertEquals(new SubjectMatterExpert(new SubjectMatterExpertKey(USER_ID), new SubjectMatterExpertContactDetails()), result);
    }

    @Test
    public void testGetSubjectMatterExpertMissingRequiredParameters()
    {
        // Try to get subject matter expert information when  subject matter expert user id is not specified.
        try
        {
            subjectMatterExpertService.getSubjectMatterExpert(new SubjectMatterExpertKey(BLANK_TEXT));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A user id must be specified.", e.getMessage());
        }
    }

    @Test
    @Ignore
    public void testGetSubjectMatterExpertTrimParameters()
    {
        // Get subject matter expert information using input parameters with leading and trailing empty spaces.
        SubjectMatterExpert result = subjectMatterExpertService.getSubjectMatterExpert(new SubjectMatterExpertKey(addWhitespace(USER_ID)));

        // Validate the returned object.
        assertEquals(new SubjectMatterExpert(new SubjectMatterExpertKey(USER_ID), new SubjectMatterExpertContactDetails()), result);
    }
}
