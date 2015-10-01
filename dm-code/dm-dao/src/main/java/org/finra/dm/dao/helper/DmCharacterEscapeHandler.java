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
package org.finra.dm.dao.helper;

import java.io.IOException;
import java.io.Writer;

import org.eclipse.persistence.oxm.CharacterEscapeHandler;
import org.springframework.stereotype.Component;

/**
 * A custom escape handler that escapes XML 1.1 restricted characters (http://www.w3.org/TR/xml11/#charsets).
 */
@Component
public class DmCharacterEscapeHandler implements CharacterEscapeHandler
{
    /**
     * Escape restricted XML 1.1 characters inside the buffer and send the output to the Writer. The "&", "<", and ">" characters are always escaped. Single and
     * double quotes are escaped when within an attribute. All XML 1.1 restricted characters are escaped using "&#x(value);".
     *
     * @param buffer the buffer.
     * @param start the start of the buffer.
     * @param length the length of the buffer.
     * @param isAttributeValue Flag that determines whether the buffer is an XML attribute or not.
     * @param outputWriter the output writer.
     *
     * @throws IOException which will stop the marshalling process.
     */
    @Override
    public void escape(char[] buffer, int start, int length, boolean isAttributeValue, Writer outputWriter) throws IOException
    {
        // Loop through all characters in the buffer starting at "start" and going until the length has been reached.
        for (int i = start; i < start + length; i++)
        {
            // Grab a character in the buffer.
            char ch = buffer[i];

            // Handle the standard XML tag escaping.
            if (ch == '&')
            {
                outputWriter.write("&amp;");
                continue;
            }
            if (ch == '<')
            {
                outputWriter.write("&lt;");
                continue;
            }
            if (ch == '>')
            {
                outputWriter.write("&gt;");
                continue;
            }

            // Handle the single and double quote characters when attributes are present.
            if (ch == '"' && isAttributeValue)
            {
                outputWriter.write("&quot;");
                continue;
            }
            if (ch == '\'' && isAttributeValue)
            {
                outputWriter.write("&apos;");
                continue;
            }

            // Escape the character if it's XML 1.1 restricted.
            if (isXml11RestrictedCharacter(ch))
            {
                outputWriter.write("&#x");
                outputWriter.write(Integer.toHexString(ch));
                outputWriter.write(";");
                continue;
            }

            // In all other cases, output the character as is.
            outputWriter.write(ch);
        }
    }

    /**
     * Returns whether the specified character is XML 1.1 restricted.
     *
     * @param ch the character to check.
     *
     * @return True if the character is restricted or false if not.
     */
    public static boolean isXml11RestrictedCharacter(char ch)
    {
        return (((ch >= 0x1) && (ch <= 0x8)) ||
            ((ch >= 0xB) && (ch <= 0xC)) ||
            ((ch >= 0xE) && (ch <= 0x1F)) ||
            ((ch >= 0x7F) && (ch <= 0x84)) ||
            ((ch >= 0x86) && (ch <= 0x9F)));
    }
}
