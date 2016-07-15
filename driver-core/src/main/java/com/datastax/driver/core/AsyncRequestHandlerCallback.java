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

import java.util.UUID;
import java.util.concurrent.*;

/**
 * A request handler callback that receives multiple results asynchronously and pushes them to a queue that is
 * then read by a AsyncResultSetIterator.
 */
class AsyncRequestHandlerCallback implements RequestHandler.Callback {

    private static final Logger logger = LoggerFactory.getLogger(AsyncRequestHandlerCallback.class);

    private final AsyncResultSetIterator.ResultQueue queue;
    private final SessionManager session;
    private final ProtocolVersion protocolVersion;
    private final Message.Request request;
    private final AsyncPagingOptions asyncPagingOptions;
    private RequestHandler handler;
    private volatile boolean stopped;

    AsyncRequestHandlerCallback(AsyncResultSetIterator.ResultQueue queue,
                                SessionManager session,
                                Cluster.Manager manager,
                                Message.Request request,
                                AsyncPagingOptions asyncPagingOptions) {
        this.queue = queue;
        this.session = session;
        this.protocolVersion = manager.protocolVersion();
        this.request = request;
        this.asyncPagingOptions = asyncPagingOptions;

        manager.addAsyncHandler(this);
    }

    /**
     * @return the unique identifier for the async session
     */
    public UUID asyncId() {
        return asyncPagingOptions.id;
    }

    @Override
    public void register(RequestHandler handler) {
        this.handler = handler;
    }

    @Override
    public Message.Request request() {
        return request;
    }

    @Override
    public void onSet(Connection connection, Message.Response response, ExecutionInfo info, Statement statement, long latency) {
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

    public void onData(Responses.Result.Rows rows) {
        ResultSet resultSet = ArrayBackedResultSet.fromMessage(rows, session, protocolVersion, handler.executionInfo(), handler.statement(), asyncPagingOptions);
        if (!stopped)
            queue.put(resultSet, rows.metadata.asyncPagingParams);
    }

    /** Stop sending results to the queue */
    public void stop() {
        stopped = true;

        final ListenableFuture<Boolean> fut = sendCancelRequest();
        try {
            boolean ret = fut.get();
            logger.info("Cancellation request for {} {}", asyncPagingOptions.id, ret ? "succeeded" : "failed");
        }
        catch (Exception ex)
        {
            logger.error("Cancellation request for {} failed with exception", asyncPagingOptions.id, ex);
        }
    }

    /**
     * Send a cancel message for this async session so that the server can release resources.
     *
     * @return - a future returning a boolean indicating if the server has acknowledged receipt of the cancel request.
     */
    private ListenableFuture<Boolean> sendCancelRequest()
    {
        ExecutionInfo executionInfo = handler.executionInfo();
        if (executionInfo == null) {
            logger.error("Cannot send cancel request for {}, no execution info", asyncPagingOptions.id);
            return Futures.immediateFuture(false);
        }

        Host host = executionInfo.getQueriedHost();
        if (host == null) {
            logger.error("Cannot send cancel request for {}, no host", asyncPagingOptions.id);
            return Futures.immediateFuture(false);
        }

        HostConnectionPool currentPool = handler.manager().pools.get(host);
        if (currentPool == null || currentPool.isClosed()) {
            logger.error("Cannot send cancel request for {}, no connection available", asyncPagingOptions.id);
            return Futures.immediateFuture(false);
        }

        try {
            final PoolingOptions poolingOptions = handler.manager().configuration().getPoolingOptions();
            final Connection connection = currentPool.borrowConnection(poolingOptions.getPoolTimeoutMillis(), TimeUnit.MILLISECONDS);
            logger.debug("Sending cancellation request for {} to {}", asyncPagingOptions.id, host);
            Connection.Future startupResponseFuture = connection.write(Requests.Cancel.asyncPaging(asyncPagingOptions.id));
            return Futures.transform(startupResponseFuture, new AsyncFunction<Message.Response, Boolean>() {
                @Override
                public ListenableFuture<Boolean> apply(Message.Response response) throws Exception {
                    try {
                        logger.debug("Cancellation request for {} received {}", asyncPagingOptions.id, response);
                        if (response instanceof Responses.Result.Void) {
                            return Futures.immediateFuture(true);
                        } else {
                            return Futures.immediateFuture(false);
                        }
                    }
                    finally {
                        connection.release();
                    }
                }
            }, handler.manager().configuration().getPoolingOptions().getInitializationExecutor());
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
    }

    @Override
    public void onSet(Connection connection, Message.Response response, long latency, int retryCount) {
        // This is only called for internal calls (i.e, when the callback is not wrapped in ResponseHandler),
        // so don't bother with ExecutionInfo.
        onSet(connection, response, null, null, latency);
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
