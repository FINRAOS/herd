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
package org.finra.dm.model.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * A DTO that holds Hive partition specific parameters required for generating an ADD PARTITION Hive DDL statement.
 */
public class HivePartitionDto
{
    /**
     * The relative path for the partition data files. The path is relative to the top level partition folder. Note that the path will contain a starting
     * "directory" separator character, but it will not contain a trailing one.
     */
    private String path;

    /**
     * The ordered list of partition values specific for this Hive partition stored as strings.
     */
    private List<String> partitionValues;

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    /**
     * Gets the partition values.
     *
     * @return the partition values.
     */
    public List<String> getPartitionValues()
    {
        if (partitionValues == null)
        {
            partitionValues = new ArrayList<>();
        }
        return partitionValues;
    }

    public void setPartitionValues(List<String> partitionValues)
    {
        this.partitionValues = partitionValues;
    }

    /**
     * Returns a builder that can easily build this DTO.
     *
     * @return the builder.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * A builder that makes it easier to construct this DTO.
     */
    public static class Builder
    {
        private HivePartitionDto hivePartition = new HivePartitionDto();

        public Builder path(String path)
        {
            hivePartition.setPath(path);
            return this;
        }

        public Builder partitionValues(List<String> partitionValues)
        {
            hivePartition.setPartitionValues(partitionValues);
            return this;
        }

        public HivePartitionDto build()
        {
            return hivePartition;
        }
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (object == null || getClass() != object.getClass())
        {
            return false;
        }

        HivePartitionDto that = (HivePartitionDto) object;

        if (partitionValues != null ? !partitionValues.equals(that.partitionValues) : that.partitionValues != null)
        {
            return false;
        }
        if (path != null ? !path.equals(that.path) : that.path != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = partitionValues != null ? partitionValues.hashCode() : 0;
        result = 31 * result + (path != null ? path.hashCode() : 0);
        return result;
    }
}
