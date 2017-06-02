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
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.helper.HerdStringHelper;

public class DelimitedFieldValuesEditorTest extends AbstractRestTest
{
    @InjectMocks
    private DelimitedFieldValuesEditor delimitedFieldValuesEditor;

    @Mock
    private HerdStringHelper herdStringHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSetAsText()
    {
        // Create a list of string values.
        List<String> values = Arrays.asList(STRING_VALUE_2);

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(STRING_VALUE)).thenReturn(values);

        // Call the method under test.
        delimitedFieldValuesEditor.setAsText(STRING_VALUE);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(STRING_VALUE);
        verifyNoMoreInteractions(herdStringHelper);

        // Validate the results.
        DelimitedFieldValues result = (DelimitedFieldValues) delimitedFieldValuesEditor.getValue();
        assertEquals(STRING_VALUE, result.getDelimitedValues());
        assertEquals(values, result.getValues());
    }
}
