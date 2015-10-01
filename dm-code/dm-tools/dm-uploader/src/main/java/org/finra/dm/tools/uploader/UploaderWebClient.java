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
package org.finra.dm.tools.uploader;

import java.io.IOException;
import java.net.URISyntaxException;

import javax.xml.bind.JAXBException;

import org.springframework.stereotype.Component;

import org.finra.dm.model.dto.UploaderInputManifestDto;
import org.finra.dm.model.api.xml.S3KeyPrefixInformation;
import org.finra.dm.tools.common.databridge.DataBridgeWebClient;

/**
 * This class encapsulates web client functionality required to communicate with the Data Management Registration Service.
 */
@Component
public class UploaderWebClient extends DataBridgeWebClient
{
    /**
     * Retrieves S3 key prefix from the Data Management Service.
     *
     * @param manifest the uploader input manifest file information
     * @param createNewVersion if not set, only initial version of the business object data is allowed to be created
     *
     * @return the S3 key prefix
     */
    public S3KeyPrefixInformation getS3KeyPrefix(UploaderInputManifestDto manifest, Boolean createNewVersion)
        throws IOException, JAXBException, URISyntaxException
    {
        return getS3KeyPrefix(manifest, null, createNewVersion);
    }
}
