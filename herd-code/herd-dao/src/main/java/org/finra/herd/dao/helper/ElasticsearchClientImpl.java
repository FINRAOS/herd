package org.finra.herd.dao.helper;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * ElasticsearchClientImpl is a stubbed version of the ElasticsearchClient used to setup a node for creating a SearchBuilder.
 */
public class ElasticsearchClientImpl implements ElasticsearchClient
{
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(
        Action<Request, Response, RequestBuilder> action, Request request)
    {
        return null;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(
        Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> actionListener)
    {

    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(
        Action<Request, Response, RequestBuilder> action)
    {
        return null;
    }

    @Override
    public ThreadPool threadPool()
    {
        return null;
    }
}
