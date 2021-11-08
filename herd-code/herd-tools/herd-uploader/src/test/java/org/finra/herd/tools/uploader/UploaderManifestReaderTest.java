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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.finra.herd.tools.common.dto.UploaderInputManifestDto;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.tools.common.dto.ManifestFile;

/**
 * Unit tests for UploaderManifestReader class.
 */
public class UploaderManifestReaderTest extends AbstractUploaderTest
{
    @Test
    public void testReadJsonManifest() throws IOException
    {
        // Create and read an uploaderInputManifestDto file.
        UploaderInputManifestDto resultManifest =
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), getTestUploaderInputManifestDto()));

        // Validate the results.
        assertUploaderManifest(getTestUploaderInputManifestDto(), resultManifest);
    }

    @Test
    public void testReadJsonManifestMissingRequiredParameters() throws IOException
    {
        UploaderInputManifestDto uploaderInputManifestDto;

        // Try to create and read the uploader input manifest when namespace is not specified.
        uploaderInputManifestDto = getTestUploaderInputManifestDto();
        uploaderInputManifestDto.setNamespace(BLANK_TEXT);
        try
        {
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest namespace must be specified.", e.getMessage());
        }

        // Try to create and read the uploader input manifest when business object definition name is not specified.
        uploaderInputManifestDto = getTestUploaderInputManifestDto();
        uploaderInputManifestDto.setBusinessObjectDefinitionName(BLANK_TEXT);
        try
        {
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest business object definition name must be specified.", e.getMessage());
        }

        // Try to create and read the uploader input manifest when business object format usage is not specified.
        uploaderInputManifestDto = getTestUploaderInputManifestDto();
        uploaderInputManifestDto.setBusinessObjectFormatUsage(BLANK_TEXT);
        try
        {
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest business object format usage must be specified.", e.getMessage());
        }

        // Try to create and read the uploader input manifest when business object format file type is not specified.
        uploaderInputManifestDto = getTestUploaderInputManifestDto();
        uploaderInputManifestDto.setBusinessObjectFormatFileType(BLANK_TEXT);
        try
        {
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest business object format file type must be specified.", e.getMessage());
        }

        // Try to create and read the uploader input manifest when business object format version is not specified.
        uploaderInputManifestDto = getTestUploaderInputManifestDto();
        uploaderInputManifestDto.setBusinessObjectFormatVersion(BLANK_TEXT);
        try
        {
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest business object format version must be specified.", e.getMessage());
        }

        // Try to create and read the uploader input manifest when partition key is not specified.
        uploaderInputManifestDto = getTestUploaderInputManifestDto();
        uploaderInputManifestDto.setPartitionKey(BLANK_TEXT);
        try
        {
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when partition key is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest business object format partition key must be specified.", e.getMessage());
        }

        // Try to create and read the uploader input manifest when partition value is not specified.
        uploaderInputManifestDto = getTestUploaderInputManifestDto();
        uploaderInputManifestDto.setPartitionValue(BLANK_TEXT);
        try
        {
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest business object data partition value must be specified.", e.getMessage());
        }

        // Try to create and read the uploader input manifest when manifest files list is empty.
        uploaderInputManifestDto = getTestUploaderInputManifestDto();
        uploaderInputManifestDto.setManifestFiles(new ArrayList<ManifestFile>());
        try
        {
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest must contain at least 1 file.", e.getMessage());
        }

        // Try to create and read the uploader input manifest when an attribute name is not specified.
        uploaderInputManifestDto = getTestUploaderInputManifestDto();
        uploaderInputManifestDto.getAttributes().put(BLANK_TEXT, ATTRIBUTE_VALUE_1);
        try
        {
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when an attribute name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest attribute name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testReadJsonManifestNoRowCount() throws IOException
    {
        // Get an instance of uploader input manifest object.
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();

        // Make all the row counts null.
        for (ManifestFile manifestFile : uploaderInputManifestDto.getManifestFiles())
        {
            manifestFile.setRowCount(null);
        }

        // Create and read a uploaderInputManifestDto file.
        UploaderInputManifestDto resultManifest =
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));

        // Validate the results.
        assertUploaderManifest(uploaderInputManifestDto, resultManifest);
    }

    @Test
    public void testReadJsonManifestNullAttributesMap() throws IOException
    {
        // Get an instance of uploader input manifest object.
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();

        // Create and read the uploaderInputManifestDto file with the list of attributes set to null.
        uploaderInputManifestDto.setAttributes(null);
        UploaderInputManifestDto resultManifest =
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));

        // Validate the results.
        assertUploaderManifest(uploaderInputManifestDto, resultManifest);
    }

    @Test
    public void testReadJsonManifestEmptyAttributesMap() throws IOException
    {
        // Get an instance of uploader input manifest object.
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();

        // Create and read the uploaderInputManifestDto file with an empty list of attributes.
        uploaderInputManifestDto.setAttributes(new HashMap<String, String>());
        UploaderInputManifestDto resultManifest =
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));

        // Validate the results.
        assertUploaderManifest(uploaderInputManifestDto, resultManifest);
    }

    @Test
    public void testReadJsonManifestNoAttributeValue() throws IOException
    {
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();
        HashMap<String, String> attributes = new HashMap<>();
        uploaderInputManifestDto.setAttributes(attributes);
        attributes.put(ATTRIBUTE_NAME_1_MIXED_CASE, null);
        attributes.put(ATTRIBUTE_NAME_2_MIXED_CASE, "");
        attributes.put(ATTRIBUTE_NAME_3_MIXED_CASE, BLANK_TEXT);
        UploaderInputManifestDto resultManifest =
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));

        // Validate the results.
        assertUploaderManifest(uploaderInputManifestDto, resultManifest);
        assertEquals(attributes.size(), resultManifest.getAttributes().size());
        assertEquals(null, resultManifest.getAttributes().get(ATTRIBUTE_NAME_1_MIXED_CASE));
        assertEquals("", resultManifest.getAttributes().get(ATTRIBUTE_NAME_2_MIXED_CASE));
        assertEquals(BLANK_TEXT, resultManifest.getAttributes().get(ATTRIBUTE_NAME_3_MIXED_CASE));
    }

    @Test
    public void testReadJsonManifestDuplicateAttributeNames() throws IOException
    {
        // Try to create and read the uploader input manifest that contains duplicate attribute names.
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();
        HashMap<String, String> attributes = new HashMap<>();
        uploaderInputManifestDto.setAttributes(attributes);
        attributes.put(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1);
        attributes.put(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_2);
        try
        {
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when manifest contains duplicate attribute names");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate manifest attribute name found: %s", ATTRIBUTE_NAME_1_MIXED_CASE).toUpperCase(), e.getMessage().toUpperCase());
        }
    }

    @Test
    public void testReadJsonManifestMaxAttributeNameLength() throws IOException
    {
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();
        HashMap<String, String> attributes = new HashMap<>();
        uploaderInputManifestDto.setAttributes(attributes);
        String attributeKey = StringUtils.leftPad("key", UploaderManifestReader.MAX_ATTRIBUTE_NAME_LENGTH, '*');  // Returns "*...*key"
        assertTrue(attributeKey.length() == UploaderManifestReader.MAX_ATTRIBUTE_NAME_LENGTH);
        attributes.put(attributeKey, ATTRIBUTE_VALUE_1);
        UploaderInputManifestDto resultManifest =
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));

        // Validate the results.
        assertUploaderManifest(uploaderInputManifestDto, resultManifest);
    }

    @Test
    public void testReadJsonManifestAttributeNameTooLong() throws IOException
    {
        // Try to create and read the uploader input manifest that contains an attribute name which is too long.
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();
        HashMap<String, String> attributes = new HashMap<>();
        uploaderInputManifestDto.setAttributes(attributes);
        String attributeKey = StringUtils.leftPad("value", UploaderManifestReader.MAX_ATTRIBUTE_NAME_LENGTH + 1, '*');  // Returns "*...*key"
        assertTrue(attributeKey.length() == UploaderManifestReader.MAX_ATTRIBUTE_NAME_LENGTH + 1);
        attributes.put(attributeKey, ATTRIBUTE_VALUE_1);
        try
        {
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when an attribute name is too long.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Manifest attribute name is longer than %d characters.", UploaderManifestReader.MAX_ATTRIBUTE_NAME_LENGTH),
                e.getMessage());
        }
    }

    @Test
    public void testReadJsonManifestMaxAttributeValueLength() throws IOException
    {
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();
        HashMap<String, String> attributes = new HashMap<>();
        uploaderInputManifestDto.setAttributes(attributes);
        String attributeKey = ATTRIBUTE_NAME_1_MIXED_CASE;
        String attributeValue = StringUtils.leftPad("value", UploaderManifestReader.MAX_ATTRIBUTE_VALUE_LENGTH, '*');  // Returns "*...*value"
        assertTrue(attributeValue.length() == UploaderManifestReader.MAX_ATTRIBUTE_VALUE_LENGTH);
        attributes.put(attributeKey, attributeValue);
        UploaderInputManifestDto resultManifest =
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));

        // Validate the results.
        assertUploaderManifest(uploaderInputManifestDto, resultManifest);
    }

    @Test
    public void testReadJsonManifestAttributeValueTooLong() throws IOException
    {
        // Try to create and read the uploader input manifest that contains an attribute value which is too long.
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();
        HashMap<String, String> attributes = new HashMap<>();
        uploaderInputManifestDto.setAttributes(attributes);
        String attributeValue = StringUtils.leftPad("value", UploaderManifestReader.MAX_ATTRIBUTE_VALUE_LENGTH + 1, '*');  // Returns "*...*value"
        assertTrue(attributeValue.length() == UploaderManifestReader.MAX_ATTRIBUTE_VALUE_LENGTH + 1);
        attributes.put(ATTRIBUTE_NAME_1_MIXED_CASE, attributeValue);
        try
        {
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when an attribute value is too long.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Manifest attribute value is longer than %s characters.", UploaderManifestReader.MAX_ATTRIBUTE_VALUE_LENGTH),
                e.getMessage());
        }
    }

    @Test
    public void testReadJsonManifestNullParentsList() throws IOException
    {
        // Create and read the uploaderInputManifestDto file with the list of parents set to null.
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();
        uploaderInputManifestDto.setBusinessObjectDataParents(null);
        UploaderInputManifestDto resultManifest =
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));

        // Validate the results.
        assertUploaderManifest(uploaderInputManifestDto, resultManifest);
    }

    @Test
    public void testReadJsonManifestEmptyParentsList() throws IOException
    {
        // Create and read the uploaderInputManifestDto file with an empty list of parents.
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();
        uploaderInputManifestDto.setBusinessObjectDataParents(new ArrayList<BusinessObjectDataKey>());
        UploaderInputManifestDto resultManifest =
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));

        // Validate the results.
        assertUploaderManifest(uploaderInputManifestDto, resultManifest);
    }

    @Test
    public void testReadJsonManifestDuplicateParents() throws IOException
    {
        // Add a duplicate business object data parents to the manifest.
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();
        BusinessObjectDataKey parent = new BusinessObjectDataKey();
        uploaderInputManifestDto.getBusinessObjectDataParents().add(parent);
        parent.setNamespace(NAMESPACE_CD.toLowerCase());
        parent.setBusinessObjectDefinitionName(TEST_BUSINESS_OBJECT_DEFINITION.toLowerCase());
        parent.setBusinessObjectFormatUsage(TEST_BUSINESS_OBJECT_FORMAT_USAGE.toLowerCase());
        parent.setBusinessObjectFormatFileType(TEST_BUSINESS_OBJECT_FORMAT_FILE_TYPE.toLowerCase());
        parent.setBusinessObjectFormatVersion(TEST_BUSINESS_OBJECT_FORMAT_VERSION);
        parent.setPartitionValue(TEST_PARENT_PARTITION_VALUE);
        parent.setBusinessObjectDataVersion(TEST_DATA_VERSION_V0);

        try
        {
            // Try to read and validate a JSON manifest file that has duplicate business object data parents.
            uploaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto));
            fail("Suppose to throw an IllegalArgumentException when the uploader manifest file contains duplicate business object data parents.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate manifest business object data parent found: {namespace: \"%s\", " +
                "businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                "businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", businessObjectDataSubPartitionValues: \"\", " +
                "businessObjectDataVersion: %d}", parent.getNamespace(), parent.getBusinessObjectDefinitionName(), parent.getBusinessObjectFormatUsage(),
                parent.getBusinessObjectFormatFileType(), parent.getBusinessObjectFormatVersion(), parent.getPartitionValue(),
                parent.getBusinessObjectDataVersion()), e.getMessage());
        }
    }

    /**
     * Validates an uploader input manifest instance.
     *
     * @param expectedUploaderInputManifest the expected uploader input manifest instance
     * @param actualUploaderInputManifest the actual instance of the uploader input manifest to be validated
     */
    private void assertUploaderManifest(UploaderInputManifestDto expectedUploaderInputManifest, UploaderInputManifestDto actualUploaderInputManifest)
    {
        // Validate all regular fields.
        assertEquals(expectedUploaderInputManifest.getBusinessObjectDefinitionName(), actualUploaderInputManifest.getBusinessObjectDefinitionName());
        assertEquals(expectedUploaderInputManifest.getBusinessObjectFormatUsage(), actualUploaderInputManifest.getBusinessObjectFormatUsage());
        assertEquals(expectedUploaderInputManifest.getBusinessObjectFormatFileType(), actualUploaderInputManifest.getBusinessObjectFormatFileType());
        assertEquals(expectedUploaderInputManifest.getBusinessObjectFormatVersion(), actualUploaderInputManifest.getBusinessObjectFormatVersion());
        assertEquals(expectedUploaderInputManifest.getPartitionKey(), actualUploaderInputManifest.getPartitionKey());
        assertEquals(expectedUploaderInputManifest.getPartitionValue(), actualUploaderInputManifest.getPartitionValue());

        // Validate the files by comparing two file list ignoring the order.
        assertTrue(expectedUploaderInputManifest.getManifestFiles().containsAll(actualUploaderInputManifest.getManifestFiles()));
        assertTrue(actualUploaderInputManifest.getManifestFiles().containsAll(expectedUploaderInputManifest.getManifestFiles()));

        // Validate the attributes.
        assertEquals(expectedUploaderInputManifest.getAttributes(), actualUploaderInputManifest.getAttributes());

        // Validate the business object data parents.
        assertEquals(expectedUploaderInputManifest.getBusinessObjectDataParents(), actualUploaderInputManifest.getBusinessObjectDataParents());
    }
}
