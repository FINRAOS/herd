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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.Arrays;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.tools.common.dto.DataBridgeBaseManifestDto;

public class DataBridgeManifestReaderTest
{
    public static class TestDataBridgeManifestReader extends DataBridgeManifestReader<DataBridgeBaseManifestDto>
    {
        @Override
        protected DataBridgeBaseManifestDto getManifestFromReader(Reader reader, ObjectMapper objectMapper) throws IOException
        {
            return objectMapper.readValue(reader, DataBridgeBaseManifestDto.class);
        }
    }

    private DataBridgeManifestReader<DataBridgeBaseManifestDto> dataBridgeManifestReader;

    @Before
    public void before()
    {
        dataBridgeManifestReader = new TestDataBridgeManifestReader();
    }

    @Test
    public void testReadJsonManifest() throws Exception
    {
        DataBridgeBaseManifestDto dataBridgeBaseManifestDto = getDataBridgeBaseManifestDto();

        File file = createTempFile(new ObjectMapper().writeValueAsString(dataBridgeBaseManifestDto));
        try
        {
            dataBridgeManifestReader.readJsonManifest(file);
        }
        catch (Exception e)
        {
            Assert.fail("unexpected exception was thrown " + e);
        }
    }

    @Test
    public void testReadJsonManifestNoNamespace() throws Exception
    {
        DataBridgeBaseManifestDto dataBridgeBaseManifestDto = getDataBridgeBaseManifestDto();
        dataBridgeBaseManifestDto.setNamespace(null);

        testReadJsonManifest(dataBridgeBaseManifestDto, "Manifest namespace must be specified.");
    }

    @Test
    public void testReadJsonManifestNoDefinitionName() throws Exception
    {
        DataBridgeBaseManifestDto dataBridgeBaseManifestDto = getDataBridgeBaseManifestDto();
        dataBridgeBaseManifestDto.setBusinessObjectDefinitionName(null);

        testReadJsonManifest(dataBridgeBaseManifestDto, "Manifest business object definition name must be specified.");
    }

    @Test
    public void testReadJsonManifestNoFileType() throws Exception
    {
        DataBridgeBaseManifestDto dataBridgeBaseManifestDto = getDataBridgeBaseManifestDto();
        dataBridgeBaseManifestDto.setBusinessObjectFormatFileType(null);

        testReadJsonManifest(dataBridgeBaseManifestDto, "Manifest business object format file type must be specified.");
    }

    @Test
    public void testReadJsonManifestNoFormatUsage() throws Exception
    {
        DataBridgeBaseManifestDto dataBridgeBaseManifestDto = getDataBridgeBaseManifestDto();
        dataBridgeBaseManifestDto.setBusinessObjectFormatUsage(null);

        testReadJsonManifest(dataBridgeBaseManifestDto, "Manifest business object format usage must be specified.");
    }

    @Test
    public void testReadJsonManifestNoPartitionKey() throws Exception
    {
        DataBridgeBaseManifestDto dataBridgeBaseManifestDto = getDataBridgeBaseManifestDto();
        dataBridgeBaseManifestDto.setPartitionKey(null);

        testReadJsonManifest(dataBridgeBaseManifestDto, "Manifest business object format partition key must be specified.");
    }

    @Test
    public void testReadJsonManifestNoPartitionValue() throws Exception
    {
        DataBridgeBaseManifestDto dataBridgeBaseManifestDto = getDataBridgeBaseManifestDto();
        dataBridgeBaseManifestDto.setPartitionValue(null);

        testReadJsonManifest(dataBridgeBaseManifestDto, "Manifest business object data partition value must be specified.");
    }

    private void testReadJsonManifest(DataBridgeBaseManifestDto dataBridgeBaseManifestDto, String expectedExceptionMessage) throws IOException,
        JsonProcessingException
    {
        File file = createTempFile(new ObjectMapper().writeValueAsString(dataBridgeBaseManifestDto));
        try
        {
            dataBridgeManifestReader.readJsonManifest(file);
            Assert.fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", expectedExceptionMessage, e.getMessage());
        }
    }

    private DataBridgeBaseManifestDto getDataBridgeBaseManifestDto()
    {
        DataBridgeBaseManifestDto dataBridgeBaseManifestDto = new DataBridgeBaseManifestDto();
        dataBridgeBaseManifestDto.setNamespace("testNamespace");
        dataBridgeBaseManifestDto.setBusinessObjectDefinitionName("testBusinessObjectDefinitionName");
        dataBridgeBaseManifestDto.setBusinessObjectFormatUsage("testBusinessObjectFormatUsage");
        dataBridgeBaseManifestDto.setBusinessObjectFormatFileType("testBusinessObjectFormatFileType");
        dataBridgeBaseManifestDto.setBusinessObjectFormatVersion("testBusinessObjectFormatVersion");
        dataBridgeBaseManifestDto.setPartitionKey("testPartitionKey");
        dataBridgeBaseManifestDto.setPartitionValue("testPartitionValue");
        dataBridgeBaseManifestDto.setSubPartitionValues(Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2"));
        return dataBridgeBaseManifestDto;
    }

    private File createTempFile(String content) throws IOException
    {
        File file = File.createTempFile("test" + (int) (Math.random() * 1000), null);
        file.deleteOnExit();
        try (PrintWriter writer = new PrintWriter(file))
        {
            writer.print(content);
        }
        return file;
    }
}
