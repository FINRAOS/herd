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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchResult;
import org.finra.herd.model.dto.RegServerAccessParamsDto;

/**
 * Executes the ExporterApp workflow.
 */
@Component
public class ExporterController
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExporterController.class);

    @Autowired
    private ExporterWebClient exporterWebClient;

    private static String followCVSformat(String value)
    {
        String result = value;
        if (result.contains("\""))
        {
            result = result.replace("\"", "\"\"");
        }
        return result;
    }

    /**
     * Executes the retention expiration exporter workflow.
     *
     * @param namespace the namespace of business object data
     * @param businessObjectDefinitionName the business object definition name of business object data
     * @param localOutputFile the local output file
     * @param regServerAccessParamsDto the DTO for the parameters required to communicate with the registration server
     *
     * @throws Exception if any problems were encountered
     */
    public void performRetentionExpirationExport(String namespace, String businessObjectDefinitionName, File localOutputFile,
        RegServerAccessParamsDto regServerAccessParamsDto) throws Exception
    {
        BusinessObjectDataSearchResult businessObjectDataSearchResult;

        try
        {
            // Initialize the web client.
            exporterWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);

            // Validate that specified business object definition exists.
            exporterWebClient.getBusinessObjectDefinition(namespace, businessObjectDefinitionName);

            // Get business object data search result and add them to the CSV file.
            //businessObjectDataSearchResult = exporterWebClient.searchBusinessObjectData(...);
            // TODO: 1) Create search request: businessObjectDataSearchRequest
            // Start a list of businessObjectDataElements resultList - List<BusinessObjectData> results
            // pageNum = 1
            // call searchBdata with pageNum=1
            // UNTIL businessObjectDataElements.size() == 0 in response:
            //     add all businessObjectDataElements to resultList
            //     pageNUm++
            //     call searchBdata with pageNum

            businessObjectDataSearchResult = this.dummyBusinessObjectDataSearchResult();

            // Writing business object data to the CSV file
            Writer writer = new OutputStreamWriter(new FileOutputStream(localOutputFile), StandardCharsets.UTF_8);
            for (BusinessObjectData businessObjectData : businessObjectDataSearchResult.getBusinessObjectDataElements())
            {
                // Creating the url the UDC
                final String url =
                    "https://udc.finra.org/data-objects/" + businessObjectData.getNamespace() + "/" + businessObjectData.getBusinessObjectDefinitionName() +
                        "/" + businessObjectData.getBusinessObjectFormatUsage() + "/" + businessObjectData.getBusinessObjectFormatFileType() + "/" +
                        businessObjectData.getBusinessObjectFormatVersion() + "/" + businessObjectData.getPartitionValue() + "/" +
                        businessObjectData.getVersion() + ";subPartitionValues=" + businessObjectData.getSubPartitionValues().get(0) + "," +
                        businessObjectData.getSubPartitionValues().get(1) + "," + businessObjectData.getSubPartitionValues().get(2) + "," +
                        businessObjectData.getSubPartitionValues().get(3);
                List<String> businessObjectDataRecords = Arrays.asList(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
                    businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
                    "" + businessObjectData.getBusinessObjectFormatVersion(), businessObjectData.getPartitionValue(),
                    businessObjectData.getSubPartitionValues().get(0), businessObjectData.getSubPartitionValues().get(1),
                    businessObjectData.getSubPartitionValues().get(2), businessObjectData.getSubPartitionValues().get(3), "" + businessObjectData.getVersion(),
                    url);
                this.writeLine(writer, businessObjectDataRecords, '"');
                LOGGER.info("BusinessObjectData=" + businessObjectDataRecords.toString());
            }

            writer.flush();
            writer.close();
        }
        catch (JAXBException | IOException | URISyntaxException e)
        {
            throw e;
        }
    }

    /**
     * @param writer
     * @param values
     * @param customQuote
     *
     * @throws IOException
     */
    private void writeLine(Writer writer, List<String> values, char customQuote) throws IOException
    {
        boolean first = true;

        StringBuilder sb = new StringBuilder();
        for (String value : values)
        {
            if (!first)
            {
                sb.append(',');
            }
            if (customQuote == ' ')
            {
                sb.append(followCVSformat(value));
            }
            else
            {
                sb.append(customQuote).append(followCVSformat(value)).append(customQuote);
            }

            first = false;
        }
        sb.append("\n");
        writer.append(sb.toString());
    }

    /**
     * @return
     * @throws JAXBException
     * @throws IOException
     * @throws URISyntaxException
     */
    private BusinessObjectDataSearchResult dummyBusinessObjectDataSearchResult() throws JAXBException, IOException, URISyntaxException
    {
        List<BusinessObjectData> businessObjectDataList = new ArrayList();
        BusinessObjectDataSearchResult businessObjectDataSearchResult = new BusinessObjectDataSearchResult();

        for (int i = 0; i < 1199; i++)
        {
            BusinessObjectData businessObjectDataKey = new BusinessObjectData();

            businessObjectDataKey.setNamespace("aniruddha");
            businessObjectDataKey.setBusinessObjectDefinitionName("ani-bdef-name");
            businessObjectDataKey.setBusinessObjectFormatUsage("ANI-TEST");
            businessObjectDataKey.setBusinessObjectFormatFileType("TXT");
            businessObjectDataKey.setBusinessObjectFormatVersion(0);
            businessObjectDataKey.setPartitionValue("ani");
            businessObjectDataKey.setSubPartitionValues(Arrays.asList("ani0" + i, "ani1" + i, "ani2" + i, "ani3" + i));
            businessObjectDataKey.setVersion(0);

            businessObjectDataList.add(businessObjectDataKey);
        }
        businessObjectDataSearchResult.setBusinessObjectDataElements(businessObjectDataList);

        return businessObjectDataSearchResult;
    }
}
