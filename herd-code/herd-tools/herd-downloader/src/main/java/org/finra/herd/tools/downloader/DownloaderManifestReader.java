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

import java.io.IOException;
import java.io.Reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import org.finra.herd.model.dto.DownloaderInputManifestDto;
import org.finra.herd.tools.common.databridge.DataBridgeManifestReader;

/**
 * Manifest reader that reads and validates a downloader JSON input manifest file.
 */
@Component
public class DownloaderManifestReader extends DataBridgeManifestReader<DownloaderInputManifestDto>
{
    @Override
    protected DownloaderInputManifestDto getManifestFromReader(Reader reader, ObjectMapper objectMapper) throws IOException
    {
        return objectMapper.readValue(reader, DownloaderInputManifestDto.class);
    }
}
