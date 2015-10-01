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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test driver for the FileUtils class.
 */
public class DmFileUtilsTest extends AbstractCoreTest
{
    private static final Path LOCAL_TEST_DIRECTORY = Paths.get(System.getProperty("java.io.tmpdir"), "dm-core-test", RANDOM_SUFFIX);

    @Before
    public void setup()
    {
        LOCAL_TEST_DIRECTORY.toFile().mkdirs();
    }

    @After
    public void cleanup() throws IOException
    {
        DmFileUtils.deleteDirectory(LOCAL_TEST_DIRECTORY.toFile());
    }

    @Test
    public void testVerifyFileExistsAndReadable() throws IOException
    {
        File testFile = createLocalFile(LOCAL_TEST_DIRECTORY.toString(), "SOME_FILE", FILE_SIZE_1_KB);
        DmFileUtils.verifyFileExistsAndReadable(testFile);
    }

    @Test
    public void testVerifyFileExistsAndReadableFileNoExists() throws IOException
    {
        File testFile = new File("I_DO_NOT_EXIST");
        try
        {
            DmFileUtils.verifyFileExistsAndReadable(testFile);
            fail("Should throw an IllegalArgumentException when file does not exist.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("File \"%s\" doesn't exist.", testFile.getName()), e.getMessage());
        }
    }

    @Test
    public void testVerifyFileExistsAndReadableFileIsDirectory() throws IOException
    {
        File testDirectory = LOCAL_TEST_DIRECTORY.toFile();
        try
        {
            DmFileUtils.verifyFileExistsAndReadable(testDirectory);
            fail("Should throw an IllegalArgumentException when argument is a directory.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("File \"%s\" is not a valid file that can be read as a manifest. Is it a directory?", testDirectory.getName()),
                e.getMessage());
        }
    }

    @Test
    public void testVerifyFileExistsAndReadableFileNotReadable() throws IOException
    {
        File testFile = createLocalFile(LOCAL_TEST_DIRECTORY.toString(), "SOME_FILE", FILE_SIZE_1_KB);

        if (testFile.setReadable(false))
        {
            try
            {
                DmFileUtils.verifyFileExistsAndReadable(testFile);
                fail("Should throw an IllegalArgumentException when file is not readable.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("Unable to read file \"%s\". Check permissions.", testFile.getName()), e.getMessage());
            }
        }
    }

    /**
     * Cleans up the local test directory by deleting a test file.
     *
     * @throws IOException if fails to create a local test file
     */
    @Test
    public void testCleanDirectoryIgnoreException() throws IOException
    {
        File testFile = createLocalFile(LOCAL_TEST_DIRECTORY.toString(), "SOME_FILE", FILE_SIZE_1_KB);
        DmFileUtils.cleanDirectoryIgnoreException(LOCAL_TEST_DIRECTORY.toFile());
        assertFalse(testFile.exists());
    }

    /**
     * Tries to clean a directory which is actually a file.
     *
     * @throws IOException if fails to create a local test file
     */
    @Test
    public void testCleanDirectoryIgnoreExceptionWithException() throws Exception
    {
        final File testFile = createLocalFile(LOCAL_TEST_DIRECTORY.toString(), "SOME_FILE", FILE_SIZE_1_KB);
        executeWithoutLogging(DmFileUtils.class, new Command()
        {
            @Override
            public void execute() throws Exception
            {
                DmFileUtils.cleanDirectoryIgnoreException(testFile);
            }
        });
        assertTrue(testFile.exists());
    }
}
