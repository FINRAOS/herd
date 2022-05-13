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
import java.net.MalformedURLException;
import java.net.URISyntaxException;
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
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
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

    public static final List<String> BUSINESS_OBJECT_DATA_HEADER = Arrays
        .asList("Namespace", "Business Object Definition Name", "Business Object Format Usage", "Business Object Format File Type",
            "Business Object Format Version", "Primary Partition Value", "Sub-Partition Value 1", "Sub-Partition Value 2", "Sub-Partition Value 3",
            "Sub-Partition Value 4", "Business Object Data Version");

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

        // Extract all unique formats from list of BData
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> aggregateStats = getAggregateStats(businessObjectDataList);

        // Create business object definition URI.
        String businessObjectDefinitionUdcUri = getBusinessObjectDefinitionUdcUri(udcServerHost, namespace, businessObjectDefinitionName);

        // Write all information to an Excel file.
        writeToExcelFile(localOutputFile, aggregateStats, startRegistrationDateTime, endRegistrationDateTime, businessObjectDefinitionDisplayName,
            businessObjectDefinitionUdcUri, businessObjectDataList);
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
     * Writes business object data to the excel file.
     *
     * @param localOutputFile                     the file to write
     * @param aggregateStats                      the business object format version aggregate stats
     * @param startRegistrationDateTime           the start registration date time
     * @param endRegistrationDateTime             the end registration date time
     * @param businessObjectDefinitionDisplayName the display name of the business object definition
     * @param businessObjectDefinitionUdcUri      the business object definition UDC URI
     * @param businessObjectDataList              the business object data list
     *
     * @throws IOException if any problems were encountered
     */
    private void writeToExcelFile(File localOutputFile, Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> aggregateStats,
        DateTime startRegistrationDateTime, DateTime endRegistrationDateTime, String businessObjectDefinitionDisplayName, String businessObjectDefinitionUdcUri,
        List<BusinessObjectData> businessObjectDataList) throws IOException
    {
        // Create xssf workbook.
        XSSFWorkbook workbook = new XSSFWorkbook();

        // Create summary sheet.
        XSSFSheet summarySheet = workbook.createSheet("Summary");

        // Initialize row number.
        int summaryRowNum = 0;

        // Write summary headers.
        XSSFRow summaryHeaders = summarySheet.createRow(summaryRowNum++);
        int summaryCellIdx = 0;
        for (String header : SUMMARY_HEADER)
        {
            Cell cell = summaryHeaders.createCell(summaryCellIdx++);
            cell.setCellValue(header);
        }

        // Write aggregate stat for each business object format version.
        for (BusinessObjectFormatKey businessObjectFormatKey : aggregateStats.keySet())
        {
            XSSFRow summaryContents = summarySheet.createRow(summaryRowNum++);
            RetentionExpirationExporterAggregateStatsDto retentionExpirationExporterAggregateStatsDto = aggregateStats.get(businessObjectFormatKey);
            List<String> aggragateStatsRow = new ArrayList<>(Arrays
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

            // Write value to each cell.
            summaryCellIdx = 0;
            for (String aggragateStatsCell : aggragateStatsRow)
            {
                Cell cell = summaryContents.createCell(summaryCellIdx++);
                cell.setCellValue(aggragateStatsCell);
            }
        }

        // Autosize the summary sheet.
        for (int columnIndex = 0; columnIndex < SUMMARY_HEADER.size(); columnIndex++)
        {
            summarySheet.autoSizeColumn(columnIndex);
        }

        // Create business object data sheet.
        XSSFSheet detailSheet = workbook.createSheet("Business Object Data");

        // Initialize row number.
        int businessObjectDataRowNum = 0;

        // Write detailed headers.
        XSSFRow detailHeaders = detailSheet.createRow(businessObjectDataRowNum++);
        int detailCellIndex = 0;
        for (String header : BUSINESS_OBJECT_DATA_HEADER)
        {
            Cell cell = detailHeaders.createCell(detailCellIndex++);
            cell.setCellValue(header);
        }

        // Write detailed information for each business object data.
        for (BusinessObjectData businessObjectData : businessObjectDataList)
        {
            XSSFRow detailContents = detailSheet.createRow(businessObjectDataRowNum++);
            int subPartitionsCount = CollectionUtils.size(businessObjectData.getSubPartitionValues());
            List<String> businessObjectDataRow = Arrays.asList(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
                businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
                Integer.toString(businessObjectData.getBusinessObjectFormatVersion()), businessObjectData.getPartitionValue(),
                subPartitionsCount > 0 ? businessObjectData.getSubPartitionValues().get(0) : "",
                subPartitionsCount > 1 ? businessObjectData.getSubPartitionValues().get(1) : "",
                subPartitionsCount > 2 ? businessObjectData.getSubPartitionValues().get(2) : "",
                subPartitionsCount > 3 ? businessObjectData.getSubPartitionValues().get(3) : "", Integer.toString(businessObjectData.getVersion()));

            // Write value to each cell.
            detailCellIndex = 0;
            for (String businessObjectDataCell : businessObjectDataRow)
            {
                Cell cell = detailContents.createCell(detailCellIndex++);
                cell.setCellValue(businessObjectDataCell);
            }
        }

        // Autosize the business object data sheet.
        for (int columnIndex = 0; columnIndex < BUSINESS_OBJECT_DATA_HEADER.size(); columnIndex++)
        {
            detailSheet.autoSizeColumn(columnIndex);
        }

        // Write the workbook to local.
        try (FileOutputStream out = new FileOutputStream(localOutputFile))
        {
            workbook.write(out);
        }
        finally
        {
            workbook.close();
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
}
