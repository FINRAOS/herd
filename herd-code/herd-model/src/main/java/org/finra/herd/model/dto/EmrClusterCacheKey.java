package org.finra.herd.model.dto;

import java.util.Objects;

/**
 * EMR cluster cache key used to retrieve the EMR clusterId from the EMR cluster cache.
 */
public class EmrClusterCacheKey
{
    private String clusterName;

    private String accountId;

    public EmrClusterCacheKey(String clusterName, String accountId)
    {
        this.clusterName = clusterName;
        this.accountId = accountId;
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public void setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
    }

    public String getAccountId()
    {
        return accountId;
    }

    public void setAccountId(String accountId)
    {
        this.accountId = accountId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        EmrClusterCacheKey that = (EmrClusterCacheKey) o;
        return Objects.equals(clusterName, that.clusterName) &&
            Objects.equals(accountId, that.accountId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(clusterName, accountId);
    }
}
