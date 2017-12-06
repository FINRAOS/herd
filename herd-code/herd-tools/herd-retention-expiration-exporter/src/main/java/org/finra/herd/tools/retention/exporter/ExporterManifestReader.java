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
package org.finra.herd.tools.retention.exporter;

import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.RetentionExpirationExporterInputManifestDto;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.tools.common.databridge.DataBridgeManifestReader;

/**
 * Manifest reader that reads and validates an uploader JSON input manifest file.
 */
@Component
public class ExporterManifestReader extends DataBridgeManifestReader<RetentionExpirationExporterInputManifestDto>
{
    public static final int MAX_ATTRIBUTE_NAME_LENGTH = 100;
    public static final int MAX_ATTRIBUTE_VALUE_LENGTH = 4000;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Override
    protected RetentionExpirationExporterInputManifestDto getManifestFromReader(Reader reader, ObjectMapper objectMapper) throws IOException
    {
        return objectMapper.readValue(reader, RetentionExpirationExporterInputManifestDto.class);
    }

    /**
     * Validates the manifest file.
     *
     * @param manifest the manifest to validate.
     *
     * @throws IllegalArgumentException if the manifest is not valid.
     */
    @Override
    protected void validateManifest(RetentionExpirationExporterInputManifestDto manifest) throws IllegalArgumentException
    {

        if (manifest.getBusinessObjectDataKeys().getBusinessObjectDataKeys() != null)
        {
            HashSet<BusinessObjectDataKey> parentValidationSet = new HashSet<>();

            for (BusinessObjectDataKey businessObjectDataKey : manifest.getBusinessObjectDataKeys().getBusinessObjectDataKeys())
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
