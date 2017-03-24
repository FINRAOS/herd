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
package org.finra.herd.dao;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.springframework.stereotype.Component;

/**
 * FileInputStreamFactory
 *
 * Factory class for a file input stream. This will help us mock a file input stream for unit testing.
 * This could be used to build other input streams in the future.
 */
@Component
public class InputStreamFactory
{
    /**
     * Method to get a file input stream.
     * @param fileName the name of the file to open
     * @return a FileInputStream
     */
    public InputStream getFileInputStream(String fileName) throws IOException
    {
        return new FileInputStream(fileName);
    }
}
