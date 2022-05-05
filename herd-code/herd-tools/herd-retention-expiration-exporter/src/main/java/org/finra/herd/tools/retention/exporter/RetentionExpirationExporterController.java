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

import static org.finra.herd.model.dto.ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_MAX_PAGE_SIZE;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.model.dto.RetentionExpirationExporterAggregateStatsDto;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.sdk.model.BusinessObjectData;
import org.finra.herd.sdk.model.BusinessObjectDataSearchFilter;
import org.finra.herd.sdk.model.BusinessObjectDataSearchKey;
import org.finra.herd.sdk.model.BusinessObjectDataSearchRequest;
import org.finra.herd.sdk.model.BusinessObjectDataSearchResult;
import org.finra.herd.sdk.model.BusinessObjectDefinition;
import org.finra.herd.sdk.model.BusinessObjectFormatKey;
import org.finra.herd.sdk.model.RegistrationDateRangeFilter;

@Component
class RetentionExpirationExporterController
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RetentionExpirationExporterController.class);

    public static final List<String> SUMMARY_HEADER = Arrays
        .asList("Namespace", "Business Object Definition Name", "Business Object Format Usage", "Business Object Format File Type",
            "Business Object Format Version", "Min Primary Partition Value", "Max Primary Partition Value", "Count", "Input Start Registration Date Time",
            "Input End Registration Date Time", "Oldest Registration Date Time", "Latest Registration Date Time", "Business Object Definition Display Name",
            "Business Object Definition URI");

    @Autowired
    private RetentionExpirationExporterWebClient retentionExpirationExporterWebClient;

    /**
     * Executes the retention expiration exporter workflow.
     *
     * @param namespace the namespace of business object data
     * @param businessObjectDefinitionName the business object definition name of business object data
     * @param startRegistrationDateTime the start date-time for the registration date-time range
     * @param endRegistrationDateTime the end date-time for the registration date-time range
     * @param localOutputFile the local output file
     * @param regServerAccessParamsDto the DTO for the parameters required to communicate with the registration server
     * @param udcServerHost the hostname of the UDC application server
     *
     * @throws Exception if any problems were encountered
     */
    void performRetentionExpirationExport(String namespace, String businessObjectDefinitionName, DateTime startRegistrationDateTime,
        DateTime endRegistrationDateTime, File localOutputFile, RegServerAccessParamsDto regServerAccessParamsDto, String udcServerHost) throws Exception
    {
        // Fail if local output file already exists.
        if (localOutputFile.exists())
        {
            throw new IllegalArgumentException(String.format("The specified local output file \"%s\" already exists.", localOutputFile.toString()));
        }

        // Initialize the web client.
        retentionExpirationExporterWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);

        // Validate that specified business object definition exists.
        BusinessObjectDefinition businessObjectDefinition =
            retentionExpirationExporterWebClient.getBusinessObjectDefinition(namespace, businessObjectDefinitionName);

        // Get business object display name.
        String businessObjectDefinitionDisplayName = getBusinessObjectDefinitionDisplayName(businessObjectDefinition);

        // Create a search request for business object data with the filter on retention expiration option.
        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(namespace);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        if (startRegistrationDateTime != null || endRegistrationDateTime != null)
        {
            RegistrationDateRangeFilter registrationDateRangeFilter = new RegistrationDateRangeFilter();
            registrationDateRangeFilter.setStartRegistrationDate(startRegistrationDateTime);
            registrationDateRangeFilter.setEndRegistrationDate(endRegistrationDateTime);
            businessObjectDataSearchKey.setRegistrationDateRangeFilter(registrationDateRangeFilter);
        }
        businessObjectDataSearchKey.setFilterOnRetentionExpiration(true);
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        businessObjectDataSearchKeys.add(businessObjectDataSearchKey);
        BusinessObjectDataSearchFilter businessObjectDataSearchFilter = new BusinessObjectDataSearchFilter();
        businessObjectDataSearchFilter.setBusinessObjectDataSearchKeys(businessObjectDataSearchKeys);
        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        request.setBusinessObjectDataSearchFilters(Collections.singletonList(businessObjectDataSearchFilter));

        // Create a result list for business object data.
        List<BusinessObjectData> businessObjectDataList = new ArrayList<>();

        // Use maximum allowed page size as defined in the application server side code as the page size when extracting the data from the application server.
        Integer pageSize = (Integer) BUSINESS_OBJECT_DATA_SEARCH_MAX_PAGE_SIZE.getDefaultValue();

        // Fetch business object data from server until no records found.
        Integer pageNumber = 1;
        BusinessObjectDataSearchResult businessObjectDataSearchResult =
            retentionExpirationExporterWebClient.searchBusinessObjectData(request, pageNumber, pageSize);
        while (CollectionUtils.isNotEmpty(businessObjectDataSearchResult.getBusinessObjectDataElements()))
        {
            LOGGER.info("Fetched {} business object data records from the registration server.",
                CollectionUtils.size(businessObjectDataSearchResult.getBusinessObjectDataElements()));
            businessObjectDataList.addAll(businessObjectDataSearchResult.getBusinessObjectDataElements());
            pageNumber++;
            businessObjectDataSearchResult = retentionExpirationExporterWebClient.searchBusinessObjectData(request, pageNumber, pageSize);
        }

        // Write business object data to the output CSV file.
        writeToCsvFile(localOutputFile, businessObjectDataList);

        // Create local summary file
        File localSummaryFile = new File(localOutputFile.getAbsolutePath() + "_summary.csv");

        // Extract all unique formats from list of BData
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> aggregateStats = getAggregateStats(businessObjectDataList);

        // Create business object definition URI.
        String businessObjectDefinitionUdcUri = getBusinessObjectDefinitionUdcUri(udcServerHost, namespace, businessObjectDefinitionName);

        // Write summary information to the second output file.
        writeToSummaryFile(localSummaryFile, aggregateStats, startRegistrationDateTime, endRegistrationDateTime, businessObjectDefinitionDisplayName, businessObjectDefinitionUdcUri);
    }

    /**
     * Computes business object format version aggregate stats from the list of business object data.
     *
     * @param businessObjectDataList the list of business object data
     *
     * @return the business object format version aggregate stats
     */
    Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> getAggregateStats(List<BusinessObjectData> businessObjectDataList)
    {
        // Extract all unique formats from list of BData
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> aggregateStats = new LinkedHashMap<>();

        for (BusinessObjectData businessObjectData : businessObjectDataList)
        {
            // Get business object format key from business object data
            BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey();
            businessObjectFormatKey.setNamespace(businessObjectData.getNamespace());
            businessObjectFormatKey.setBusinessObjectDefinitionName(businessObjectData.getBusinessObjectDefinitionName());
            businessObjectFormatKey.setBusinessObjectFormatUsage(businessObjectData.getBusinessObjectFormatUsage());
            businessObjectFormatKey.setBusinessObjectFormatFileType(businessObjectData.getBusinessObjectFormatFileType());
            businessObjectFormatKey.setBusinessObjectFormatVersion(businessObjectData.getBusinessObjectFormatVersion());

            // If business object data has createdOn value defined, convert to XMLGregorianCalendar value.
            XMLGregorianCalendar createdOnXmlGregorianCalendar =
                businessObjectData.getCreatedOn() == null ? null : HerdDateUtils.getXMLGregorianCalendarValue(businessObjectData.getCreatedOn().toDate());

            // Add business object format key to the map if it is not there.
            if (!aggregateStats.containsKey(businessObjectFormatKey))
            {
                // Initialize aggregate stats dto with first business object data for this business object format version.
                // Please note that createdOn value will be null if start/end registration date time are not specified.
                RetentionExpirationExporterAggregateStatsDto retentionExpirationExporterAggregateStatsDto =
                    new RetentionExpirationExporterAggregateStatsDto(businessObjectData.getPartitionValue(), businessObjectData.getPartitionValue(), 1,
                        createdOnXmlGregorianCalendar, createdOnXmlGregorianCalendar);
                aggregateStats.put(businessObjectFormatKey, retentionExpirationExporterAggregateStatsDto);
            }
            // If XXX exists, update the aggregate stats.
            else
            {
                // Get retention expiration exporter aggrate stats dto.
                RetentionExpirationExporterAggregateStatsDto retentionExpirationExporterAggregateStatsDto = aggregateStats.get(businessObjectFormatKey);

                // Update min primary partition value.
                if (StringUtils.compare(retentionExpirationExporterAggregateStatsDto.getMinPrimaryPartitionValue(), businessObjectData.getPartitionValue()) > 0)
                {
                    retentionExpirationExporterAggregateStatsDto.setMinPrimaryPartitionValue(businessObjectData.getPartitionValue());
                }

                // Update max primary partition value.
                if (StringUtils.compare(retentionExpirationExporterAggregateStatsDto.getMaxPrimaryPartitionValue(), businessObjectData.getPartitionValue()) < 0)
                {
                    retentionExpirationExporterAggregateStatsDto.setMaxPrimaryPartitionValue(businessObjectData.getPartitionValue());
                }

                // Update count.
                retentionExpirationExporterAggregateStatsDto.setCount(retentionExpirationExporterAggregateStatsDto.getCount() + 1);

                // Update oldest and latest registration date time, if business object data has createdOn value specified.
                if (createdOnXmlGregorianCalendar != null)
                {
                    // Update oldest registration date time, if it is not set yet or business object data has an older createdOn value.
                    if (retentionExpirationExporterAggregateStatsDto.getOldestRegistrationDateTime() == null ||
                        retentionExpirationExporterAggregateStatsDto.getOldestRegistrationDateTime().compare(createdOnXmlGregorianCalendar) > 0)
                    {
                        retentionExpirationExporterAggregateStatsDto.setOldestRegistrationDateTime(createdOnXmlGregorianCalendar);
                    }

                    // Update latest registration date time, if it is not set yet or business object data has a newer createdOn value.
                    if (retentionExpirationExporterAggregateStatsDto.getLatestRegistrationDateTime() == null ||
                        retentionExpirationExporterAggregateStatsDto.getLatestRegistrationDateTime().compare(createdOnXmlGregorianCalendar) < 0)
                    {
                        retentionExpirationExporterAggregateStatsDto.setLatestRegistrationDateTime(createdOnXmlGregorianCalendar);
                    }
                }
            }
        }

        return aggregateStats;
    }

    /**
     * Writes business object data to the summary file.
     *
     * @param localOutputFile                     the file to write
     * @param aggregateStats                      the business object format version aggregate stats
     * @param startRegistrationDateTime           the start registration date time
     * @param endRegistrationDateTime             the end registration date time
     * @param businessObjectDefinitionDisplayName the display name of the business object definition
     * @param businessObjectDefinitionUdcUri      the business object definition UDC URI
     *
     * @throws IOException if any problems were encountered
     */
    private void writeToSummaryFile(File localOutputFile, Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> aggregateStats,
        DateTime startRegistrationDateTime, DateTime endRegistrationDateTime, String businessObjectDefinitionDisplayName, String businessObjectDefinitionUdcUri)
        throws IOException
    {
        // Open local output file to write.
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(localOutputFile), StandardCharsets.UTF_8))
        {
            // Define all headers for the output file.
            List<String> headers = SUMMARY_HEADER;

            // Write csv file headers.
            writeLine(writer, headers);

            // Write aggregate stat for each business object format version.
            for (BusinessObjectFormatKey businessObjectFormatKey : aggregateStats.keySet())
            {
                RetentionExpirationExporterAggregateStatsDto retentionExpirationExporterAggregateStatsDto = aggregateStats.get(businessObjectFormatKey);
                List<String> businessObjectDataRecords = new ArrayList<>(Arrays
                    .asList(businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(),
                        businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType(),
                        businessObjectFormatKey.getBusinessObjectFormatVersion().toString(),
                        retentionExpirationExporterAggregateStatsDto.getMinPrimaryPartitionValue() == null ? "" :
                            retentionExpirationExporterAggregateStatsDto.getMinPrimaryPartitionValue(),
                        retentionExpirationExporterAggregateStatsDto.getMaxPrimaryPartitionValue() == null ? "" :
                            retentionExpirationExporterAggregateStatsDto.getMaxPrimaryPartitionValue(),
                        retentionExpirationExporterAggregateStatsDto.getCount().toString(),
                        startRegistrationDateTime == null ? "" : startRegistrationDateTime.toString(),
                        endRegistrationDateTime == null ? "" : endRegistrationDateTime.toString(),
                        retentionExpirationExporterAggregateStatsDto.getOldestRegistrationDateTime() == null ? "" :
                            retentionExpirationExporterAggregateStatsDto.getOldestRegistrationDateTime().toString(),
                        retentionExpirationExporterAggregateStatsDto.getLatestRegistrationDateTime() == null ? "" :
                            retentionExpirationExporterAggregateStatsDto.getLatestRegistrationDateTime().toString(), businessObjectDefinitionDisplayName,
                        businessObjectDefinitionUdcUri));
                writeLine(writer, businessObjectDataRecords);
            }
        }
    }

    /**
     * Get business object definition display name from business object definition.
     *
     * @param businessObjectDefinition the business object definition
     *
     * @return the business object definition display name
     */
    String getBusinessObjectDefinitionDisplayName(BusinessObjectDefinition businessObjectDefinition)
    {
        return StringUtils.isNotEmpty(businessObjectDefinition.getDisplayName()) ? businessObjectDefinition.getDisplayName() :
            businessObjectDefinition.getBusinessObjectDefinitionName();
    }

    /**
     * Builds and returns business object definition UDC URI.
     *
     * @param udcServerHost the hostname of the UDC application server
     * @param namespace the namespace of business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     *
     * @return the business object definition URI
     *
     * @throws URISyntaxException if an URI syntax error was encountered
     * @throws MalformedURLException if an URL syntax error was encountered
     */
    String getBusinessObjectDefinitionUdcUri(String udcServerHost, String namespace, String businessObjectDefinitionName)
        throws URISyntaxException, MalformedURLException
    {
        URIBuilder uriBuilder =
            new URIBuilder().setScheme("https").setHost(udcServerHost).setPath(String.format("/data-entities/%s/%s", namespace, businessObjectDefinitionName));
        return String.valueOf(uriBuilder.build().toURL());
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
     * Writes business object data to the CSV file.
     *
     * @param localOutputFile the file to write
     * @param businessObjectDataList the list of business object data
     *
     * @throws IOException if any problems were encountered
     */
    private void writeToCsvFile(File localOutputFile, List<BusinessObjectData> businessObjectDataList) throws IOException
    {
        // Create the local output file.
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(localOutputFile), StandardCharsets.UTF_8))
        {
            // Write csv file header.
            writeLine(writer, Arrays.asList("Namespace", "Business Object Definition Name", "Business Object Format Usage", "Business Object Format File Type",
                "Business Object Format Version", "Primary Partition Value", "Sub-Partition Value 1", "Sub-Partition Value 2", "Sub-Partition Value 3",
                "Sub-Partition Value 4", "Business Object Data Version"));

            for (BusinessObjectData businessObjectData : businessObjectDataList)
            {
                int subPartitionsCount = CollectionUtils.size(businessObjectData.getSubPartitionValues());
                List<String> businessObjectDataRecords = Arrays.asList(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
                    businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
                    Integer.toString(businessObjectData.getBusinessObjectFormatVersion()), businessObjectData.getPartitionValue(),
                    subPartitionsCount > 0 ? businessObjectData.getSubPartitionValues().get(0) : "",
                    subPartitionsCount > 1 ? businessObjectData.getSubPartitionValues().get(1) : "",
                    subPartitionsCount > 2 ? businessObjectData.getSubPartitionValues().get(2) : "",
                    subPartitionsCount > 3 ? businessObjectData.getSubPartitionValues().get(3) : "", Integer.toString(businessObjectData.getVersion()));
                writeLine(writer, businessObjectDataRecords);
            }
        }
    }
}
