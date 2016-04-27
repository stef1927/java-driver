package com.datastax.driver.core;

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

import com.datastax.driver.core.exceptions.*;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;


/**
 * A request handler callback that pushes results on a queue. Each a result is a CompletableFuture<ResultSet>, so
 * that clients can then chain their own async. result processing and join on all futures.
 */
class StreamingRequestHandlerCallback implements RequestHandler.Callback {

    private static final Logger logger = LoggerFactory.getLogger(StreamingRequestHandlerCallback.class);
    static final ResultSetFutureImpl STOP = new ResultSetFutureImpl();

    private final BlockingDeque<ResultSetFutureImpl> queue;
    private final SessionManager session;
    private final ProtocolVersion protocolVersion;
    private final Message.Request request;


    StreamingRequestHandlerCallback(BlockingDeque<ResultSetFutureImpl> queue,
                                    SessionManager session,
                                    ProtocolVersion protocolVersion,
                                    Message.Request request) {
        this.queue = queue;
        this.session = session;
        this.protocolVersion = protocolVersion;
        this.request = request;
    }

    @Override
    public void register(RequestHandler handler) {
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
                        case ROWS:
                            set(ArrayBackedResultSet.fromMessage(rm, session, protocolVersion, info, statement));
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
        finally {
            maybeClose(response);
        }

    }

    private void setException(Exception ex)
    {
        ResultSetFutureImpl future = new ResultSetFutureImpl();
        sendResult(future);
        future.setException(ex);
    }

    private void set(ResultSet result)
    {
        ResultSetFutureImpl future = new ResultSetFutureImpl();
        sendResult(future);
        future.set(result);
    }

    private void maybeClose(Message.Response response)
    {
        // we have a BUG that sometime causes some final pages to be missed
        // when the driver is overwhelmed, this is because if the last message
        // is processed by a thread before any of the preceding messages,
        // they will be missed, to fix this we need to add a sequence number to the
        // replies
        if (!response.isMultiPart())
            sendResult(STOP);
    }

    private void sendResult(ResultSetFutureImpl result)
    {
        try {
            logger.debug("Sending debug {}", result);
            queue.put(result);
        }
        catch (InterruptedException ex) {
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

    // We sometimes need (in the driver) to set the future from outside this class,
    // but AbstractFuture#set is protected so this method. We don't want it public
    // however, no particular reason to give users rope to hang themselves.
    void setResult(ResultSet rs) {
        set(rs);
    }

    @Override
    public int retryCount() {
        // This is only called for internal calls (i.e, when the future is not wrapped in RequestHandler).
        // There is no retry logic in that case, so the value does not really matter.
        return 0;
    }

    public final static class ResultSetFutureImpl  extends AbstractFuture<ResultSet> implements ResultSetFuture
    {
        @Override
        public boolean set(ResultSet value) {
            return super.set(value);
        }

        public boolean setException(Throwable throwable) {
            return super.setException(throwable);
        }

        @Override
        public ResultSet getUninterruptibly() {
            try {
                return Uninterruptibles.getUninterruptibly(this);
            } catch (ExecutionException e) {
                throw DriverThrowables.propagateCause(e);
            }
        }

        @Override
        public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
            try {
                return Uninterruptibles.getUninterruptibly(this, timeout, unit);
            } catch (ExecutionException e) {
                throw DriverThrowables.propagateCause(e);
            }
        }
    }
}
