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
package org.finra.herd.tools.downloader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.finra.herd.tools.common.dto.DownloaderOutputManifestDto;
import org.springframework.stereotype.Component;

/**
 * Manifest writer that writes a downloader JSON output manifest file.
 */
@Component
public class DownloaderManifestWriter
{
    /**
     * Serializes provided manifest object instance as JSON output, written to a file in the specified directory.
     *
     * @param directory the local parent directory path where the manifest file should be created
     * @param manifestFileName the manifest file name
     * @param manifest the UploaderManifest instance to serialize
     *
     * @return the resulting file
     * @throws IOException if an I/O error was encountered while writing the JSON manifest.
     */
    protected File writeJsonManifest(File directory, String manifestFileName, DownloaderOutputManifestDto manifest) throws IOException
    {
        // Create result file object.
        Path resultFilePath = Paths.get(directory.getPath(), manifestFileName);
        File resultFile = new File(resultFilePath.toString());

        // Convert Java object to JSON format.
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(resultFile, manifest);

        return resultFile;
    }
}
