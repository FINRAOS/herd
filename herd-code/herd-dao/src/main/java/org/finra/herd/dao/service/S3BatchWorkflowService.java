package org.finra.herd.dao.service;

import org.springframework.stereotype.Service;

import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;

@Service
public interface S3BatchWorkflowService
{
    void batchRestoreObjects(final S3FileTransferRequestParamsDto params, int expirationInDays, String archiveRetrievalOption);
}
