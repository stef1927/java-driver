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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

class RowIteratorImpl extends RowIterator {
    private static final Logger logger = LoggerFactory.getLogger(RowIteratorImpl.class);

    private final AsyncRequestHandlerCallback cb;
    private final PageQueue queue;
    private final SessionManager session;
    private final Cluster.Manager manager;
    private final Statement statement;
    private Page currentPage;
    private boolean isClosed;

    RowIteratorImpl(AsyncRequestHandlerCallback cb, PageQueue queue) {
        this.cb = cb;
        this.queue = queue;
        this.session = queue.session;
        this.manager = session.getCluster().manager;
        this.statement = queue.statement;
        this.currentPage = new Page();
    }

    /**
     * Encapsulated a result as either a ResultSet or an Exception. Also contains
     * sequence number and indication of whether this is the last result.
     */
    private final static class Page {
        Responses.Result.Rows.RowIterator rowIterator;
        ColumnDefinitions columnDefinitions;
        ByteBuffer pagingState;
        Exception exception;
        int seqNo = 0;
        boolean last = false;

        Page() {

        }

        Page(Exception exception) {
            this.exception = exception;
        }

        Page(Responses.Result.Rows response) {
            this.rowIterator = response.rowIterator();
            this.columnDefinitions = response.metadata.columns;
            this.pagingState = response.metadata.pagingState;
            this.exception = null;
            this.seqNo = response.metadata.asyncPagingParams.seqNo;
            this.last = response.metadata.asyncPagingParams.last;
        }

        boolean hasNext() {
            return rowIterator != null && rowIterator.hasNext();
        }

        List<ByteBuffer> next() {
            return rowIterator == null ? null : rowIterator.next();
        }

        int remaining() {
            return rowIterator == null ? 0 : rowIterator.remaining();
        }

        void close() {
            try {
                if (rowIterator != null) {
                    rowIterator.close();
                    rowIterator = null;
                }
            }
            catch (Exception ex) {
                logger.error("Failed to release row iterator", ex);
            }
        }

        @Override
        public String toString() {
            return String.format("[no. %d, %s rows%s]",
                    seqNo,
                    rowIterator == null ? "-" : Integer.toString(rowIterator.remaining()),
                    last ? " (final)" : "");
        }
    }

    /**
     * The queue used to feed messages from the Netty thread to the consumer thread
     */
    final static class PageQueue {
        static final int CAPACITY = 4;
        final Statement statement;
        final SessionManager session;
        final long timeoutMillis;
        final ArrayBlockingQueue<Page> queue;

        PageQueue(Statement statement, SessionManager session) {
            this.statement = statement;
            this.session = session;
            this.timeoutMillis = statement.getReadTimeoutMillis() > 0
                    ? statement.getReadTimeoutMillis()
                    : session.getCluster().manager.connectionFactory.getReadTimeoutMillis();
            this.queue = new ArrayBlockingQueue<Page>(CAPACITY);
        }

        public Page take() {
            try {
                Page ret = queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
                if (ret == null)
                    throw new RuntimeException("Timed out reading result from queue");

                return ret;
            }
            catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted whilst reading page from queue", ex);
            }
        }

        public void put(Exception exception) {
            put(new Page(exception));
        }


        public void put(Responses.Result.Rows response) {
            put(new Page(response));
        }

        private void put(Page page) {
            try {
                if (!queue.offer(page, timeoutMillis, TimeUnit.MILLISECONDS))
                    throw new RuntimeException("Timed out inserting page into queue");
            }
            catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted whilst inserting page into queue", ex);
            }
        }

        public int size() {
            return queue.size();
        }
    }

    @Override
    public ColumnDefinitions getColumnDefinitions() {
        if (currentPage.columnDefinitions != null)
            return currentPage.columnDefinitions;

        BoundStatement boundStatement = null;
        if (statement instanceof StatementWrapper) {
            Statement wrappedStatement = ((StatementWrapper) statement).getWrappedStatement();
            if (wrappedStatement instanceof BoundStatement)
                boundStatement = (BoundStatement)wrappedStatement;
        }
        else if (statement instanceof BoundStatement)
            boundStatement = (BoundStatement)statement;

        return boundStatement == null ? null : boundStatement.statement.getPreparedId().resultSetMetadata;
    }

    public AsyncPagingOptions pagingOptions() {
        return cb.pagingOptions();
    }

    public int pageNo() {
        return currentPage.seqNo;
    }

    @Override
    public void close() {
        if (!isClosed) {
            if (logger.isTraceEnabled())
                logger.trace("Closing iterator, remaining: {}/{},", currentPage.remaining(), queue.size());

            isClosed = true;
            currentPage.close();

            try {
                if (!currentPage.last)
                    cb.stop();
            }
            finally {
                cb.release();
            }
        }
    }

    @Override
    public State state() {
        if (isClosed)
            throw new UnsupportedOperationException("Already closed");

        return new StateImpl(Collections.<Row>emptyList(), this);
    }

    @Override
    public boolean hasNext() {
        if (isClosed)
            return false;

        try {
            if (currentPage.hasNext())
                return true;

            newPage();

            return currentPage.hasNext();
        }
        catch (Throwable t)
        {
            logger.error("Got exception in stream iterator", t);
            return false;
        }
    }

    @Override
    public Row next() {
        return isClosed ? null : nextRow(currentPage.next());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    private Row nextRow(List<ByteBuffer> row) {
        return ArrayBackedRow.fromData(getColumnDefinitions(), manager.metadata.tokenFactory(),
                manager.protocolVersion(), row);
    }

    private void newPage() {
        if (logger.isTraceEnabled())
            logger.trace("Closing page {}", currentPage);

        currentPage.close();

        if (currentPage.last) {
            return;
        }

        long nextSeqNo = currentPage.seqNo + 1;
        currentPage.close();
        currentPage = queue.take();

        if (logger.isTraceEnabled())
            logger.trace("Iterator switched to page {}", currentPage);

        if (currentPage.exception != null) {
            logger.error("Received error in current page: {}", currentPage, currentPage.exception);
            currentPage.exception.printStackTrace();
            return;
        }

        if (currentPage.seqNo > nextSeqNo) {
            logger.error("Iterator got out of order msg {}, expected {}", currentPage.seqNo, nextSeqNo);
            throw new RuntimeException(String.format("Got out of order message with seq. no %d, expected %d",
                    currentPage.seqNo, nextSeqNo));
        }
    }


    /**
     * This state implementation
     */
    private static class StateImpl implements State {
        final SessionManager session;
        final Statement statement;
        final AsyncPagingOptions pagingOptions;
        final ByteBuffer pagingState;
        final List<Row> rows;

        StateImpl(List<Row> previousPageRows, RowIteratorImpl iterator) {
            if (logger.isTraceEnabled())
                logger.trace("Creating state with {} previous rows and {} remaining rows",
                             previousPageRows.size(), iterator.currentPage.remaining());

            this.session = iterator.session;
            this.statement = iterator.statement;
            this.pagingOptions = iterator.pagingOptions();
            this.pagingState = iterator.currentPage.pagingState;
            this.rows = makeRows(previousPageRows, iterator);

            iterator.close(); // no longer safe to use
        }

        private List<Row> makeRows(List<Row> previousPageRows, RowIteratorImpl iterator) {
            Page page = iterator.currentPage;
            if (previousPageRows.isEmpty() && !page.hasNext())
                return Collections.emptyList();

            List<Row> ret = new ArrayList<Row>(previousPageRows.size() + page.remaining());
            ret.addAll(previousPageRows);
            while(page.hasNext())
                ret.add(iterator.nextRow(page.next()));

            return ret;
        }

        @Override
        public RowIterator resume() {
            if (pagingState == null)
                return com.datastax.driver.core.RowIterator.EMPTY;

            assert !(statement instanceof BatchStatement);

            final PageQueue queue = new PageQueue(statement, session);
            final AsyncPagingOptions nextPagingOptions = pagingOptions.withNewId();

            final AsyncRequestHandlerCallback cb = new AsyncRequestHandlerCallback(queue, session.getCluster().manager,
                    session.makeRequestMessage(statement, pagingState, nextPagingOptions), nextPagingOptions);

            cb.sendRequest();
            return new StateBasedIterator(this, new RowIteratorImpl(cb, queue));
        }
    }

    /**
     * This iterator will exhaust any rows from the previous page and then forward to the next iterator.
     */
    private static class StateBasedIterator extends RowIterator {
        private final List<Row> rows;
        private final RowIteratorImpl inner;
        private int currentRow;
        private boolean isClosed;

        StateBasedIterator(StateImpl state, RowIteratorImpl inner) {
            this.rows = state.rows;
            this.inner = inner;
            this.currentRow = 0;

            if (inner.currentPage.pagingState == null && state.pagingState != null)
                inner.currentPage.pagingState = state.pagingState;
        }

        @Override
        public ColumnDefinitions getColumnDefinitions() {
            return inner.getColumnDefinitions();
        }

        @Override
        public AsyncPagingOptions pagingOptions() {
            return inner.pagingOptions();
        }

        @Override
        public int pageNo() {
            if (rows.isEmpty())
                return inner.pageNo();

            return currentRow < rows.size() ? 1 : 1 + inner.pageNo();
        }

        @Override
        public void close() {
            if (!isClosed) {
                isClosed = true;
                inner.close();
            }
        }

        @Override
        public State state() {
            if (isClosed)
                throw new UnsupportedOperationException("Already closed");

            if (rows.isEmpty() || currentRow >= rows.size())
                return inner.state();

            return new StateImpl(rows.subList(currentRow, rows.size()), inner);
        }

        @Override
        public boolean hasNext() {
            if (isClosed)
                return false;

            return (currentRow < rows.size()) || inner.hasNext();
        }

        @Override
        public Row next() {
            if (isClosed)
                return null;

            if (currentRow < rows.size()) {
                Row ret = rows.get(currentRow);
                currentRow++;
                return ret;
            }

            return inner.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }
}
