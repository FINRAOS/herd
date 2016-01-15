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
package org.finra.herd.service.helper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;

/**
 * A helper class for TAR functionality.
 */
@Component
public class TarHelper
{
    private static final Logger LOGGER = Logger.getLogger(TarHelper.class);

    @Autowired
    private HerdHelper herdHelper;

    /**
     * Creates a TAR archive file of a directory.
     *
     * @param tarFile the TAR file to be created
     * @param dirPath the directory path
     *
     * @throws IOException on error
     */
    public void createTarArchive(File tarFile, Path dirPath) throws IOException
    {
        try (TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(new BufferedOutputStream(new FileOutputStream(tarFile))))
        {
            tarArchiveOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            tarArchiveOutputStream.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
            addEntryToTarArchive(tarArchiveOutputStream, dirPath, Paths.get(""));
        }
    }

    /**
     * Adds a TAR archive entry to the specified TAR archive stream. The method calls itself recursively for all directories/files found.
     *
     * @param tarArchiveOutputStream the TAR output stream that writes a UNIX tar archive as an output stream
     * @param path the path relative to the base for a directory or file to be added to the TAR archive stream
     * @param base the base for the directory or file path
     *
     * @throws IOException on error
     */
    private void addEntryToTarArchive(TarArchiveOutputStream tarArchiveOutputStream, Path path, Path base) throws IOException
    {
        File file = path.toFile();
        Path entry = Paths.get(base.toString(), file.getName());
        TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(file, entry.toString());
        tarArchiveOutputStream.putArchiveEntry(tarArchiveEntry);

        if (file.isFile())
        {
            try (FileInputStream fileInputStream = new FileInputStream(file))
            {
                // TODO: This method uses a default buffer size of 8K.
                // TODO: Taking a file size in consideration, a bigger buffer size we might increase the performance of the tar.
                IOUtils.copy(fileInputStream, tarArchiveOutputStream);
            }
            finally
            {
                tarArchiveOutputStream.closeArchiveEntry();
            }
        }
        else
        {
            tarArchiveOutputStream.closeArchiveEntry();
            File[] children = file.listFiles();
            if (children != null)
            {
                for (File child : children)
                {
                    addEntryToTarArchive(tarArchiveOutputStream, Paths.get(child.getAbsolutePath()), entry);
                }
            }
        }
    }

    /**
     * Performs a sanity test of the TAR archive file size.
     *
     * @param tarFile the TAR file
     * @param storageFilesSizeBytes the total size of storage files registered for the business object data in the storage
     * @param storageName the storage name
     * @param businessObjectDataKey the business object data key
     *
     * @throws IllegalStateException if the TAR file size is less than the total size of storage files
     */
    public void validateTarFileSize(File tarFile, long storageFilesSizeBytes, String storageName, BusinessObjectDataKey businessObjectDataKey)
        throws IllegalStateException
    {
        long tarFileSizeBytes = tarFile.length();

        // Sanity check for the TAR file size.
        if (tarFileSizeBytes < storageFilesSizeBytes)
        {
            throw new IllegalStateException(String.format(
                "The \"%s\" TAR archive file size (%d bytes) is less than the total size of registered storage files (%d bytes). " +
                    "Storage: {%s}, business object data: {%s}", tarFile.getPath(), tarFileSizeBytes, storageFilesSizeBytes, storageName,
                herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }
    }

    /**
     * Logs contents of a TAR file.
     *
     * @param tarFile the TAR file
     *
     * @throws IOException on error
     */
    public void logTarFileContents(File tarFile) throws IOException
    {
        TarArchiveInputStream tarArchiveInputStream = null;

        try
        {
            tarArchiveInputStream = new TarArchiveInputStream(new FileInputStream(tarFile));
            TarArchiveEntry entry;

            LOGGER.info(String.format("Listing the contents of \"%s\" TAR archive file:", tarFile.getPath()));

            while (null != (entry = tarArchiveInputStream.getNextTarEntry()))
            {
                LOGGER.info(String.format("    %s", entry.getName()));
            }
        }
        finally
        {
            if (tarArchiveInputStream != null)
            {
                tarArchiveInputStream.close();
            }
        }
    }
}
