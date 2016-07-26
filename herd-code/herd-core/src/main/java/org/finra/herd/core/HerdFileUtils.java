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
package org.finra.herd.core;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides additional file utilities.
 */
public class HerdFileUtils extends FileUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HerdFileUtils.class);

    /**
     * The number of bytes in a gigabyte.
     */
    public static final long BYTES_PER_GB = 1073741824L;

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
     * @param directory the directory to clean
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

    /**
     * Deletes a directory. This method does not fail in case deletion is unsuccessful, but simply logs the exception information as a warning.
     *
     * @param directory the directory to delete
     */
    public static void deleteDirectoryIgnoreException(File directory)
    {
        try
        {
            FileUtils.deleteDirectory(directory);
        }
        catch (Exception e)
        {
            LOGGER.warn(String.format("Failed to delete \"%s\" directory.", directory), e);
        }
    }

    /**
     * Deletes a file. This method does not fail in case deletion is unsuccessful, but simply logs the exception information as a warning.
     *
     * @param file the file to delete
     */
    public static void deleteFileIgnoreException(File file)
    {
        try
        {
            FileUtils.forceDelete(file);
        }
        catch (Exception e)
        {
            LOGGER.warn(String.format("Failed to delete \"%s\" file.", file), e);
        }
    }
}
