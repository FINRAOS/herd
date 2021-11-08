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
package org.finra.herd.tools.uploader;

import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.finra.herd.tools.uploader.dto.UploaderInputManifestDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.tools.common.dto.ManifestFile;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.tools.common.databridge.DataBridgeManifestReader;

/**
 * Manifest reader that reads and validates an uploader JSON input manifest file.
 */
@Component
public class UploaderManifestReader extends DataBridgeManifestReader<UploaderInputManifestDto>
{
    public static final int MAX_ATTRIBUTE_NAME_LENGTH = 100;
    public static final int MAX_ATTRIBUTE_VALUE_LENGTH = 4000;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Override
    protected UploaderInputManifestDto getManifestFromReader(Reader reader, ObjectMapper objectMapper) throws IOException
    {
        return objectMapper.readValue(reader, UploaderInputManifestDto.class);
    }

    /**
     * Validates the manifest file.
     *
     * @param manifest the manifest to validate.
     *
     * @throws IllegalArgumentException if the manifest is not valid.
     */
    @Override
    protected void validateManifest(UploaderInputManifestDto manifest) throws IllegalArgumentException
    {
        // Perform the base validation.
        super.validateManifest(manifest);

        Assert.hasText(manifest.getBusinessObjectFormatVersion(), "Manifest business object format version must be specified.");
        Assert.notEmpty(manifest.getManifestFiles(), "Manifest must contain at least 1 file.");

        // Ensure row counts are all positive.
        for (ManifestFile manifestFile : manifest.getManifestFiles())
        {
            Assert.hasText(manifestFile.getFileName(), "Manifest file can not have a blank filename.");

            // Trim to ensure we don't get errors with leading or trailing spaces causing "path" errors when validating that files exist.
            manifestFile.setFileName(manifestFile.getFileName().trim());

            if (manifestFile.getRowCount() != null)
            {
                Assert.isTrue(manifestFile.getRowCount() >= 0, "Manifest file \"" + manifestFile.getFileName() + "\" can not have a negative row count.");
            }
        }

        if (manifest.getAttributes() != null)
        {
            HashSet<String> attributeNameValidationSet = new HashSet<>();

            for (Map.Entry<String, String> entry : manifest.getAttributes().entrySet())
            {
                String attributeName = entry.getKey().trim();
                Assert.hasText(attributeName, "Manifest attribute name must be specified.");

                // Validate attribute key length.
                Assert.isTrue(attributeName.length() <= MAX_ATTRIBUTE_NAME_LENGTH,
                    String.format("Manifest attribute name is longer than %d characters.", MAX_ATTRIBUTE_NAME_LENGTH));

                // Ensure the attribute name isn't a duplicate by using a map with a "lowercase" name as the key for case insensitivity.
                String lowercaseAttributeName = attributeName.toLowerCase();
                Assert.isTrue(!attributeNameValidationSet.contains(lowercaseAttributeName),
                    String.format("Duplicate manifest attribute name found: %s", attributeName));

                // Validate attribute value length.
                if (entry.getValue() != null)
                {
                    Assert.isTrue(entry.getValue().length() <= MAX_ATTRIBUTE_VALUE_LENGTH,
                        String.format("Manifest attribute value is longer than %d characters.", MAX_ATTRIBUTE_VALUE_LENGTH));
                }

                attributeNameValidationSet.add(lowercaseAttributeName);
            }
        }

        if (manifest.getBusinessObjectDataParents() != null)
        {
            HashSet<BusinessObjectDataKey> parentValidationSet = new HashSet<>();

            for (BusinessObjectDataKey businessObjectDataKey : manifest.getBusinessObjectDataParents())
            {
                // Perform validation.
                Assert.hasText(businessObjectDataKey.getBusinessObjectDefinitionName(), "Manifest parent business object definition name must be specified.");
                Assert.hasText(businessObjectDataKey.getBusinessObjectFormatUsage(), "Manifest parent business object format usage must be specified.");
                Assert.hasText(businessObjectDataKey.getBusinessObjectFormatFileType(), "Manifest parent business object format file type must be specified.");
                Assert.hasText(businessObjectDataKey.getPartitionValue(), "Manifest parent business object data partition value must be specified.");

                // Remove leading and trailing spaces.
                businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectDataKey.getBusinessObjectDefinitionName().trim());
                businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectDataKey.getBusinessObjectFormatUsage().trim());
                businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectDataKey.getBusinessObjectFormatFileType().trim());
                businessObjectDataKey.setPartitionValue(businessObjectDataKey.getPartitionValue().trim());

                // Ensure the business object data parent isn't a duplicate by using a unique set
                // with "lowercase" parent alternate key fields that are case insensitive.
                BusinessObjectDataKey lowercaseKey = new BusinessObjectDataKey();
                lowercaseKey.setBusinessObjectDefinitionName(businessObjectDataKey.getBusinessObjectDefinitionName().toLowerCase());
                lowercaseKey.setBusinessObjectFormatUsage(businessObjectDataKey.getBusinessObjectFormatUsage().toLowerCase());
                lowercaseKey.setBusinessObjectFormatFileType(businessObjectDataKey.getBusinessObjectFormatFileType().toLowerCase());
                lowercaseKey.setBusinessObjectFormatVersion(businessObjectDataKey.getBusinessObjectFormatVersion());
                lowercaseKey.setPartitionValue(businessObjectDataKey.getPartitionValue());
                lowercaseKey.setBusinessObjectDataVersion(businessObjectDataKey.getBusinessObjectDataVersion());
                Assert.isTrue(!parentValidationSet.contains(lowercaseKey), String.format("Duplicate manifest business object data parent found: {%s}",
                    businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
                parentValidationSet.add(lowercaseKey);
            }
        }
    }
}
