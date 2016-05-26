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
import com.datastax.driver.core.utils.MoreFutures;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.*;

import static com.datastax.driver.core.Message.Response.Type.ERROR;


/**
 * A request handler callback that receives multiple results asynchronously and pushes them to a queue that is
 * then read by a AsyncResultSetIterator.
 */
class AsyncRequestHandlerCallback implements RequestHandler.Callback {

    private static final int MAX_QUEUE_SIZE = 10; // Maximum number of items on the queue before we block the netty IO thread
    private static final int ON_QUEUE_FULL_SLEEP_MILLIS = 1; // Amount of time we block for if the queue is full

    private static final Logger logger = LoggerFactory.getLogger(AsyncRequestHandlerCallback.class);

    private final PriorityBlockingQueue<Result> queue;
    private final SessionManager session;
    private final ProtocolVersion protocolVersion;
    private final Message.Request request;
    private final AsyncPagingOptions asyncPagingOptions;
    private RequestHandler handler;
    private volatile boolean stopped;

    AsyncRequestHandlerCallback(PriorityBlockingQueue<Result> queue,
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
        sendResult(new Result(resultSet, rows.metadata.asyncPagingParams));
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
        sendResult(new Result(ex));
    }

    private void sendResult(Result result)
    {
        try {
            if (logger.isTraceEnabled())
                logger.trace("Sending result {}", result);

            // we may process messages out of sequence if another thread goes ahead but the server will
            // send them in sequence, so if our message is not the smallest message in the queue it is safe to block
            // until the size of the queue is smaller or our message is the smallest
            while(!stopped && queue.size() > MAX_QUEUE_SIZE) {
                Result lowest = queue.peek();
                // need to check on != null due to a possible race
                if (lowest != null && result.seqNo > lowest.seqNo) {
                    Thread.sleep(ON_QUEUE_FULL_SLEEP_MILLIS);
                }
            }

            if (!stopped)
                queue.put(result);
        }
        catch (Exception ex) {
            logger.error("Failed to report result {} due to interrupted exception", result, ex);
        }
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

    public final static class Result implements Comparable<Result> {
        public final ResultSet resultSet; // can be null
        public final Exception exception; // can be null
        public final int seqNo;
        public final boolean last;

        public Result(ResultSet resultSet, Responses.Result.Rows.AsyncPagingParams params) {
            this(resultSet, null, params.seqNo, params.last);
        }

        public Result(Exception exception) {
            this (null, exception, 0, true);
        }

        private Result(ResultSet resultSet, Exception exception, int seqNo, boolean last) {
            this.resultSet = resultSet;
            this.exception = exception;
            this.seqNo = seqNo;
            this.last = last;
        }

        @Override
        public int compareTo(Result that) {
            return this.seqNo == that.seqNo ? 0 : this.seqNo < that.seqNo ? -1 : 1;
        }

        @Override
        public String toString() {
            return String.format("%s - %d%s", resultSet, seqNo, last ? " final" : "");
        }
    }
}
