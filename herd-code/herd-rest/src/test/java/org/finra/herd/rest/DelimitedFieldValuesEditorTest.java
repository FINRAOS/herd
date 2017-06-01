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

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/*
 * Test the DelimitedFieldValuesEditor class.
 */
public class DelimitedFieldValuesEditorTest extends AbstractRestTest
{
    @Autowired
    DelimitedFieldValuesEditor delimitedFieldValuesEditor;

    @Test
    public void testSetAsText()
    {
        String[] values = {"TEST1", "TEST\\|2", "TEST3"};
        String[] expectedValues = {"TEST1", "TEST|2", "TEST3"};

        delimitedFieldValuesEditor.setAsText(StringUtils.join(values, "|"));

        DelimitedFieldValues delimitedFieldValues = (DelimitedFieldValues) delimitedFieldValuesEditor.getValue();

        assertEquals(StringUtils.join(values, "|"), delimitedFieldValues.getDelimitedValues());

        for (int i = 0; i < values.length; i++)
        {
            assertEquals(expectedValues[i], delimitedFieldValues.getValues().get(i));
        }
    }
}
