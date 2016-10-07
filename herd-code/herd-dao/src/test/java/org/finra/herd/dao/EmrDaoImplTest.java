package org.finra.herd.dao;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;

import org.finra.herd.dao.impl.EmrDaoImpl;
import org.finra.herd.model.dto.AwsParamsDto;

public class EmrDaoImplTest extends EmrDaoImpl
{

    @Override
    public AmazonElasticMapReduceClient getEmrClient(AwsParamsDto awsParamsDto)
    {
         return super.getEmrClient(awsParamsDto);
    }
}
