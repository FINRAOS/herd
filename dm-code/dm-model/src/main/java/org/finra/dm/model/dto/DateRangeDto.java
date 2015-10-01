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

import java.util.Date;

/**
 * A date range DTO specified in terms of two java.util.Date objects.
 */
public class DateRangeDto
{
    /**
     * The lower bound date.
     */
    private Date lowerDate;

    /**
     * The upper bound date.
     */
    private Date upperDate;

    public Date getLowerDate()
    {
        return lowerDate;
    }

    public void setLowerDate(Date lowerDate)
    {
        this.lowerDate = lowerDate;
    }

    public Date getUpperDate()
    {
        return upperDate;
    }

    public void setUpperDate(Date upperDate)
    {
        this.upperDate = upperDate;
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
        private DateRangeDto dateRange = new DateRangeDto();

        public Builder lowerDate(Date lowerDate)
        {
            dateRange.setLowerDate(lowerDate);
            return this;
        }

        public Builder upperDate(Date upperDate)
        {
            dateRange.setUpperDate(upperDate);
            return this;
        }

        public DateRangeDto build()
        {
            return dateRange;
        }
    }
}
