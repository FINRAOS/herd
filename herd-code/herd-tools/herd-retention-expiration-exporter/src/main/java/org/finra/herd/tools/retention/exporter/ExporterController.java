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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
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

    /**
     * Executes the retention expiration exporter workflow.
     *
     * @param namespace the namespace of business object data
     * @param businessObjectDefinitionName the business object definition name of business object data
     * @param localOutputFile the local output file
     * @param regServerAccessParamsDto the DTO for the parameters required to communicate with the registration server
     * @param udcServerHost the hostname of the UDC application server
     *
     * @throws Exception if any problems were encountered
     */
    public void performRetentionExpirationExport(String namespace, String businessObjectDefinitionName, File localOutputFile,
        RegServerAccessParamsDto regServerAccessParamsDto, String udcServerHost) throws Exception
    {
        // Initialize the web client.
        exporterWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);

        // Validate that specified business object definition exists.
        exporterWebClient.getBusinessObjectDefinition(namespace, businessObjectDefinitionName);

        // Creating request for business object data search
        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(namespace);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        businessObjectDataSearchKeys.add(businessObjectDataSearchKey);
        BusinessObjectDataSearchFilter businessObjectDataSearchFilter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest(Collections.singletonList(businessObjectDataSearchFilter));

        // Result list for business object data
        List<BusinessObjectData> businessObjectDataList = new ArrayList<>();

        // Fetch business object data from server until no records found
        int pageNumber = 1;
        BusinessObjectDataSearchResult businessObjectDataSearchResult = exporterWebClient.searchBusinessObjectData(request, pageNumber);
        while (CollectionUtils.isNotEmpty(businessObjectDataSearchResult.getBusinessObjectDataElements()))
        {
            LOGGER.info("Fetched {} business object data records from the registration server.",
                CollectionUtils.size(businessObjectDataSearchResult.getBusinessObjectDataElements()));
            businessObjectDataList.addAll(businessObjectDataSearchResult.getBusinessObjectDataElements());
            pageNumber++;
            businessObjectDataSearchResult = exporterWebClient.searchBusinessObjectData(request, pageNumber);
        }

        // Write business object data to the csv file
        writeToCsvFile(localOutputFile, namespace, businessObjectDefinitionName, udcServerHost, businessObjectDataList);
    }

    /**
     * Writes business object data to the csv file.
     *
     * @param localOutputFile the file to write
     * @param namespace the namespace of business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param udcServerHost the hostname of the UDC application server
     * @param businessObjectDataList the list of business object data
     *
     * @throws IOException if any problems were encountered
     */
    private void writeToCsvFile(File localOutputFile, String namespace, String businessObjectDefinitionName, String udcServerHost,
        List<BusinessObjectData> businessObjectDataList) throws IOException
    {
        // Creating the url the UDC
        String businessObjectDefinitionUdcUri = String.format("https://%s/data-entities/%s/%s", udcServerHost, namespace, businessObjectDefinitionName);

        // Create the local output file.
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(localOutputFile), StandardCharsets.UTF_8))
        {
            for (BusinessObjectData businessObjectData : businessObjectDataList)
            {
                List<String> businessObjectDataRecords = Arrays.asList(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
                    businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
                    Integer.toString(businessObjectData.getBusinessObjectFormatVersion()), businessObjectData.getPartitionValue(),
                    businessObjectData.getSubPartitionValues().get(0), businessObjectData.getSubPartitionValues().get(1),
                    businessObjectData.getSubPartitionValues().get(2), businessObjectData.getSubPartitionValues().get(3),
                    Integer.toString(businessObjectData.getVersion()), businessObjectDefinitionUdcUri);
                writeLine(writer, businessObjectDataRecords);
            }
        }
    }

    /**
     * Write one line in the csv file.
     *
     * @param writer file write object
     * @param values value to write
     *
     * @throws IOException if any problems were encountered
     */
    private void writeLine(Writer writer, List<String> values) throws IOException
    {
        final char customQuote = '"';

        StringBuilder stringBuilder = new StringBuilder();
        boolean first = true;
        for (String value : values)
        {
            if (!first)
            {
                stringBuilder.append(',');
            }
            stringBuilder.append(customQuote).append(applyCsvFormatting(value)).append(customQuote);
            first = false;
        }
        stringBuilder.append(System.lineSeparator());
        writer.append(stringBuilder.toString());
    }

    /**
     * Applies CSV formatting to a string value.
     *
     * @param value the string value to format
     *
     * @return the CSV formatted string value
     */
    private String applyCsvFormatting(String value)
    {
        return value.replace("\"", "\"\"");
    }
}
