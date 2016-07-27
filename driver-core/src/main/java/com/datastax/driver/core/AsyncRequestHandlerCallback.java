/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.*;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection callback for receiving multiple asynchronous results from the server
 * by pushing them to a queue.
 */
class AsyncRequestHandlerCallback implements Connection.ResponseCallback {

    private static final Logger logger = LoggerFactory.getLogger(AsyncRequestHandlerCallback.class);

    private final RowIteratorImpl.PageQueue queue;
    private final Message.Request request;
    private final AsyncPagingOptions asyncPagingOptions;
    private final Statement statement;
    private final SessionManager session;
    private final RequestHandler.QueryPlan queryPlan;
    private volatile Host host;
    private volatile boolean stopped;
    private volatile Connection connection;

    AsyncRequestHandlerCallback(RowIteratorImpl.PageQueue queue,
                                Cluster.Manager manager,
                                Message.Request request,
                                AsyncPagingOptions asyncPagingOptions) {
        this.queue = queue;
        this.request = request;
        this.asyncPagingOptions = asyncPagingOptions;
        this.statement = queue.statement;
        this.session = queue.session;
        this.queryPlan = new RequestHandler.QueryPlan(manager.loadBalancingPolicy().newQueryPlan(statement.getKeyspace(), statement));

        manager.addAsyncHandler(this);
    }

    AsyncPagingOptions pagingOptions() {
        return asyncPagingOptions;
    }

    @Override
    public Message.Request request() {
        return request;
    }

    /** Stop sending results to the queue */
    public void stop() {
        stopped = true;

        final ListenableFuture<Boolean> fut = sendCancelRequest();
        try {
            boolean ret = fut.get();
            if (logger.isTraceEnabled())
                logger.trace("Cancellation request for {} {}", asyncPagingOptions.id, ret ? "succeeded" : "failed");
        }
        catch (Exception ex)
        {
            logger.error("Cancellation request for {} failed with exception", asyncPagingOptions.id, ex);
        }
    }

    public void release() {
        if (connection != null) {
            connection.release();
            connection = null;
            host = null;
        }
    }

    public void sendRequest() {
        maybeConnect();
        connection.write(this, -1, false);
    }

    private void maybeConnect() {
        if (connection != null) {
            assert host != null : "Expected host with connection";
            return;
        }

        while ((host = queryPlan.next()) != null && !stopped) {
            try {
                HostConnectionPool currentPool = session.pools.get(host);
                if (currentPool == null || currentPool.isClosed())
                    continue;

                connection = currentPool.reserveConnection();
                break;
            }
            catch (Exception ex) {
                logger.error("[{}] Failed to connect to {}", pagingOptions().id, host, ex);

                if (metricsEnabled())
                    metrics().getErrorMetrics().getConnectionErrors().inc();

                host = null;
                connection = null;
            }
        }

        if (connection == null)
            throw new RuntimeException("Could not connect to any hosts");
    }

    private boolean metricsEnabled() {
        return session.configuration().getMetricsOptions().isEnabled();
    }

    private Metrics metrics() {
        return session.cluster.manager.metrics;
    }


    /**
     * Send a cancel message for this async session so that the server can release resources.
     *
     * @return - a future returning a boolean indicating if the server has acknowledged receipt of the cancel request.
     */
    private ListenableFuture<Boolean> sendCancelRequest()
    {
        if (host == null) {
            logger.error("Cannot send cancel request for {}, no host", asyncPagingOptions.id);
            return Futures.immediateFuture(false);
        }

        if (connection == null) {
            logger.error("Cannot send cancel request for {}, not connected", asyncPagingOptions.id);
            return Futures.immediateFuture(false);
        }
        try {
            if (logger.isTraceEnabled())
                logger.trace("Sending cancel request for {} to {}", asyncPagingOptions.id, host);

            Connection.Future fut = connection.write(Requests.Cancel.asyncPaging(asyncPagingOptions.id));
            return Futures.transform(fut, new AsyncFunction<Message.Response, Boolean>() {
                @Override
                public ListenableFuture<Boolean> apply(Message.Response response) throws Exception {
                    if (logger.isTraceEnabled())
                        logger.trace("Cancellation request for {} received {}", asyncPagingOptions.id, response);
                    if (response instanceof Responses.Result.Void) {
                        return Futures.immediateFuture(true);
                    } else {
                        return Futures.immediateFuture(false);
                    }
                }
            }, session.configuration().getPoolingOptions().getInitializationExecutor());
        }
        catch (Exception ex)
        {
            logger.error("Failed to cancel request {} due to exception", asyncPagingOptions.id, ex);
            return Futures.immediateFailedFuture(ex);
        }
    }

    private void setException(Exception ex)
    {
        if (!stopped)
            queue.put(ex);

        session.cluster.manager.removeAsyncHandler(this);
    }

    void onData(Responses.Result.Rows rows) {
        if (!stopped)
            queue.put(rows);

        if (rows.metadata.asyncPagingParams.last)
            session.cluster.manager.removeAsyncHandler(this);
    }

    @Override
    public void onSet(Connection connection, Message.Response response, long latency, int retryCount) {
        try {
            switch (response.type) {
                case RESULT:
                    Responses.Result rm = (Responses.Result) response;
                    switch (rm.kind) {
                        case VOID:
                            // expected null reply since we will get data later on
                            break;
                        default:
                            setException(new UnsupportedOperationException("Unexpected result kind : " + rm.kind.toString()));
                    }
                    break;
                case ERROR:
                    setException(((Responses.Error) response).asException(connection.address));
                    break;
                default:
                    // This mean we have probably have a bad node, so defunct the connection
                    connection.defunct(new ConnectionException(connection.address, String.format("Got unexpected %s response", response.type)));
                    setException(new DriverInternalError(String.format("Got unexpected %s response from %s", response.type, connection.address)));
                    break;
            }
        } catch (RuntimeException e) {
            // If we get a bug here, the client will not get it, so better forwarding the error
            setException(new DriverInternalError("Unexpected error while processing response from " + connection.address, e));
        }
    }

    @Override
    public void onException(Connection connection, Exception exception, long latency, int retryCount) {
        setException(exception);
    }

    @Override
    public boolean onTimeout(Connection connection, long latency, int retryCount) {
        // This is only called for internal calls (i.e, when the future is not wrapped in RequestHandler).
        // So just set an exception for the final result, which should be handled correctly by said internal call.
        setException(new OperationTimedOutException(connection.address));
        return true;
    }

    @Override
    public int retryCount() {
        // This is only called for internal calls (i.e, when the future is not wrapped in RequestHandler).
        // There is no retry logic in that case, so the value does not really matter.
        return 0;
    }
}
