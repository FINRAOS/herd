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
package org.finra.dm.core;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * Provides additional file utilities.
 */
public class DmFileUtils extends FileUtils
{
    private static final Logger LOGGER = Logger.getLogger(DmFileUtils.class);

    /**
     * Verifies that the specified file exists and can be read as a valid file.
     *
     * @param file the file to check.
     */
    public static void verifyFileExistsAndReadable(File file)
    {
        if (!file.exists())
        {
            throw new IllegalArgumentException("File \"" + file.getName() + "\" doesn't exist.");
        }
        if (!file.isFile())
        {
            throw new IllegalArgumentException("File \"" + file.getName() + "\" is not a valid file that can be read as a manifest. Is it a directory?");
        }
        if (!file.canRead())
        {
            throw new IllegalArgumentException("Unable to read file \"" + file.getName() + "\". Check permissions.");
        }
    }

    /**
     * Cleans a directory without deleting it. This method does not fail in case cleaning is unsuccessful, but simply logs the exception information as a
     * warning.
     *
     * @param directory directory to clean
     */
    public static void cleanDirectoryIgnoreException(File directory)
    {
        try
        {
            FileUtils.cleanDirectory(directory);
        }
        catch (Exception e)
        {
            LOGGER.warn(String.format("Failed to clean \"%s\" directory.", directory), e);
        }
    }
}
