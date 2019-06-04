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
package org.finra.herd.tools.common.databridge;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.Charsets;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdFileUtils;
import org.finra.herd.model.dto.DataBridgeBaseManifestDto;

/**
 * A base class for uploader and downloader manifest readers.
 */
public abstract class DataBridgeManifestReader<M extends DataBridgeBaseManifestDto>
{
    /**
     * Gets a manifest object from a reader and an object mapper. Sub-classes should override this method to read into their specific manifest object.
     *
     * @param reader the reader to read the file from.
     * @param objectMapper the object mapper.
     *
     * @return the manifest object.
     * @throws IOException if the input file could not be read.
     */
    protected abstract M getManifestFromReader(Reader reader, ObjectMapper objectMapper) throws IOException;

    /**
     * Reads a JSON manifest file into a JSON manifest object.
     *
     * @param jsonManifestFile the JSON manifest file.
     *
     * @return the manifest object.
     * @throws java.io.IOException if any errors were encountered reading the JSON file.
     * @throws IllegalArgumentException if the manifest file has validation errors.
     */
    public M readJsonManifest(File jsonManifestFile) throws IOException, IllegalArgumentException
    {
        // Verify that the file exists and can be read.
        HerdFileUtils.verifyFileExistsAndReadable(jsonManifestFile);

        // Deserialize the JSON manifest.
        BufferedInputStream buffer = new BufferedInputStream(new FileInputStream(jsonManifestFile));
        BufferedReader reader = new BufferedReader(new InputStreamReader(buffer, Charsets.UTF_8));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        objectMapper.setVisibility(PropertyAccessor.GETTER, Visibility.NONE);
        objectMapper.setVisibility(PropertyAccessor.SETTER, Visibility.NONE);
        M manifest = getManifestFromReader(reader, objectMapper);

        // Validate the manifest and return it.
        validateManifest(manifest);
        return manifest;
    }

    /**
     * Validates a manifest.
     *
     * @param manifest the manifest to validate.
     *
     * @throws IllegalArgumentException if the manifest is not valid.
     */
    protected void validateManifest(M manifest) throws IllegalArgumentException
    {
        Assert.hasText(manifest.getNamespace(), "Manifest namespace must be specified.");
        Assert.hasText(manifest.getBusinessObjectDefinitionName(), "Manifest business object definition name must be specified.");
        Assert.hasText(manifest.getBusinessObjectFormatFileType(), "Manifest business object format file type must be specified.");
        Assert.hasText(manifest.getBusinessObjectFormatUsage(), "Manifest business object format usage must be specified.");
        Assert.hasText(manifest.getPartitionKey(), "Manifest business object format partition key must be specified.");
        Assert.hasText(manifest.getPartitionValue(), "Manifest business object data partition value must be specified.");
    }
}
