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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class RowIteratorImpl extends RowIterator {
    private static final Logger logger = LoggerFactory.getLogger(RowIteratorImpl.class);

    private final AsyncRequestHandlerCallback cb;
    private final PageQueue queue;
    private final int[] queueSizeDist = new int[PageQueue.LENGTH + 1];
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

        void fill(Exception exception) {
            close();
            this.exception = exception;
        }

        void fill(Responses.Result.Rows response) {
            this.rowIterator = response.rowIterator();
            this.columnDefinitions = response.metadata.columns;
            this.pagingState = response.metadata.pagingState;
            this.exception = null;
            this.seqNo = response.metadata.asyncPagingParams.seqNo;
            this.last = response.metadata.asyncPagingParams.last;
        }

        void fill(Page other)
        {
            this.rowIterator = other.rowIterator;
            this.columnDefinitions = other.columnDefinitions;
            this.pagingState = other.pagingState;
            this.exception = other.exception;
            this.seqNo = other.seqNo;
            this.last = other.last;
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
            if (rowIterator != null) {
                rowIterator.close();
                rowIterator = null;
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
     * A non-blocking bounded queue of results. This is an ad-hoc adaptation of the LMAX disruptor that
     * supports a single consumer (the iterator, which has been declared as such in the documentation)
     * and a single producer (the Netty event loop thread of the channel).
     * The waiting strategy is a loop with a Thread.yield.
     * See the technical paper at https://lmax-exchange.github.io/disruptor/ for more details.
     */
    final static class PageQueue {
        static int LENGTH = 4; // this should be a power of two
        final Statement statement;
        final SessionManager session;
        final long timeoutMillis;
        final Page[] pages = new Page[LENGTH];

        volatile int producerIndex = -1; // the index where the producer is writing to
        volatile int publishedIndex = -1; // the index where the producer has written to
        volatile int consumerIndex = 0; // the index where the (single) consumer is reading from

        PageQueue(Statement statement, SessionManager session) {
            this.statement = statement;
            this.session = session;
            timeoutMillis = statement.getReadTimeoutMillis() > 0
                    ? statement.getReadTimeoutMillis()
                    : session.getCluster().manager.connectionFactory.getReadTimeoutMillis();

            assert (pages.length & (pages.length - 1)) == 0 : "Results length must be a power of 2";
            for (int i = 0; i < pages.length; i++) {
                pages[i] = new Page(); //pre-allocate in order to reduce garbage and make it cache friendly
            }
        }

        // this return the modulo for a power of two
        private int index(int i)
        {
            return i & (pages.length - 1);
        }

        /**
         This method is called by the consumer, before reading it must wait
         until a published result is available.
         */
        void take(Page ret) {
            long start = System.currentTimeMillis();
            while (consumerIndex > publishedIndex) {
                Thread.yield();

                if (System.currentTimeMillis() - start > timeoutMillis)
                    throw new RuntimeException("Timed out reading result from slot");
            }

            if (logger.isTraceEnabled())
                logger.trace("Reading from index {}...", consumerIndex);

            ret.fill(pages[index(consumerIndex)]);
            consumerIndex++;
        }

        /**
         * This method is called by the producer, {@link AsyncRequestHandlerCallback}, first we
         * increase the producer index to the slot we'll be writing to, then wait for the consumer
         * to have read it, then write and publish it.
         */
        public void put(Exception exception) {
            long start = System.currentTimeMillis();
            producerIndex++;
            waitForConsumer(start);

            if (logger.isTraceEnabled())
                logger.trace("Writing exception at index {}", producerIndex);

            pages[index(producerIndex)].fill(exception);
            publishedIndex = producerIndex;
        }

        /**
         * This method is called by the producer, {@link AsyncRequestHandlerCallback}, first we
         * increase the producer index to the slot we'll be writing to, then wait for the consumer
         * to have read it, then write and publish it.
         */
        public void put(Responses.Result.Rows response) {
            long start = System.currentTimeMillis();
            producerIndex++;
            waitForConsumer(start);

            if (logger.isTraceEnabled())
                logger.trace("Writing result with seq No {} at index {}",
                        response.metadata.asyncPagingParams.seqNo, producerIndex);

            pages[index(producerIndex)].fill(response);
            publishedIndex = producerIndex;
        }

        /**
         * Wait for the consumer to have consumed the slow the producer will write to.
         */
        private void waitForConsumer(long start) {
            while(producerIndex - consumerIndex >= pages.length) {
                Thread.yield(); //wait for consumer to catch up

                if (System.currentTimeMillis() - start > timeoutMillis)
                    throw new RuntimeException("Timed out waiting consumer to catch up");
            }
        }

        public int size() {
            return publishedIndex - consumerIndex + 1;
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
                logger.trace("Closing iterator, remaining: {}/{}, queue size distribution: {}",
                             currentPage.remaining(), queue.size(), Arrays.toString(queueSizeDist));

            isClosed = true;
            currentPage.close();

            if (!currentPage.last)
                cb.stop();
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

        if (logger.isTraceEnabled())
            queueSizeDist[queue.size()]++;

        queue.take(currentPage);

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

            new RequestHandler(session, cb, statement).sendRequest();

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

            if (currentRow < rows.size()) {
               return true;
            }

            return inner.hasNext();
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
