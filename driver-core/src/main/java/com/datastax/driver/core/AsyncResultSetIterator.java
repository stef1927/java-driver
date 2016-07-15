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

import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

class AsyncResultSetIterator extends ResultSetIterator {
    private static final Logger logger = LoggerFactory.getLogger(AsyncResultSetIterator.class);

    private final AsyncRequestHandlerCallback cb;
    private final ResultQueue queue;
    private final PriorityQueue<Result> outOfOrderResults;
    private int nextSeqNo = 1;
    private boolean endReached = false;

    AsyncResultSetIterator(AsyncRequestHandlerCallback cb, ResultQueue queue) {
        this.cb = cb;
        this.queue = queue;
        this.outOfOrderResults = new PriorityQueue<Result>();
        this.nextSeqNo = 1;
        this.endReached = false;
    }

    /**
     * Encapsulated a result as either a ResultSet or an Exception. Also contains
     * sequence number and indication of whether this is the last result.
     */
    private final static class Result implements Comparable<Result> {
        ResultSet resultSet; // either resultSet or exception are available
        Exception exception;
        int seqNo;
        boolean last;

        void fill(Exception exception) {
            this.resultSet = null;
            this.exception = exception;
            this.seqNo = 0;
            this.last = true;
        }

        void fill(ResultSet resultSet, int seqNo, boolean last) {
            this.resultSet = resultSet;
            this.exception = null;
            this.seqNo = seqNo;
            this.last = last;
        }

        void fill(Result other)
        {
            this.resultSet = other.resultSet;
            this.exception = other.exception;
            this.seqNo = other.seqNo;
            this.last = other.last;
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

    /**
     * A non-blocking bounded queue of results. This is an ad-hoc adaptation of the LMAX disruptor that
     * supports a single consumer (the iterator, which has been declared as such in the documentation)
     * and multiple producers (the Netty threads). In fact I think Netty guarantees a single thread on each
     * channel and so we could simplify it to a single producer as well (just remove CAS on the producersIndex).
     * The waiting strategy is a loop with a Thread.yield.
     * See the technical paper at https://lmax-exchange.github.io/disruptor/ for more details.
     */
    final static class ResultQueue
    {
        Result[] results = new Result[16]; // this should be a power of two
        final long timeoutMillis;
        AtomicInteger producersIndex = new AtomicInteger(-1); // the index where the producers are writing to
        volatile int publishedIndex = -1; // the index where the producers have written to
        volatile int consumerIndex = 0; // the index where the (single) consumer is reading from

        ResultQueue(Statement statement, Cluster cluster)
        {
            assert (results.length & (results.length - 1)) == 0 : "Results length must be a power of 2";
            for (int i = 0; i < results.length; i++) {
                results[i] = new Result(); //pre-allocate in order to reduce garbage and make it cache friendly
            }

            timeoutMillis = statement.getReadTimeoutMillis() > 0
                    ? statement.getReadTimeoutMillis()
                    : cluster.manager.connectionFactory.getReadTimeoutMillis();
        }

        // this return the modulo for a power of two
        private int index(int i)
        {
            return i & (results.length - 1);
        }

        /**
         This method is called by the consumer, which is single threaded (the iterator below). We wait
         until a published result is available.
         */
        Result take() {
            long start = System.currentTimeMillis();
            while (consumerIndex > publishedIndex) {
                Thread.yield();

                if (System.currentTimeMillis() - start > timeoutMillis)
                    throw new RuntimeException("Timed out reading result from slot");
            }

            if (logger.isTraceEnabled())
                logger.trace("Reading from index {}...", consumerIndex);

            Result ret = new Result();
            ret.fill(results[index(consumerIndex)]);
            consumerIndex++;
            return ret;
        }

        /**
         * This method is called by the producers, {@link AsyncRequestHandlerCallback} so we cas
         * in order to get the next producer index and then wait for the published index to be moved
         * by whichever producer got the slot before ours, at which point we publish our result.
         */
        public void put(Exception exception)
        {
            long start = System.currentTimeMillis();
            int index = claimProducersIndex(start);

            if (logger.isTraceEnabled())
                logger.trace("Writing exception at index {}", index);

            results[index(index)].fill(exception);

            while(publishedIndex != (index - 1)) {
                Thread.yield();

                if (System.currentTimeMillis() - start > timeoutMillis)
                    throw new RuntimeException("Timed out writing exception to claimed slot");
            }

            publishedIndex = index;
        }

        /**
         * This method is called by the producers, {@link AsyncRequestHandlerCallback} so we cas
         * in order to get the next producer index and then wait for the published index to be moved
         * by whichever producer got the slot before ours, at which point we publish our result.
         */
        public void put(ResultSet resultSet, Responses.Result.Rows.AsyncPagingParams params)
        {
            long start = System.currentTimeMillis();
            int index = claimProducersIndex(start);

            if (logger.isTraceEnabled())
                logger.trace("Writing result with seq No {} at index {}", params.seqNo, index);

            results[index(index)].fill(resultSet, params.seqNo, params.last);

            while(publishedIndex != (index - 1)) {
                Thread.yield();

                if (System.currentTimeMillis() - start > timeoutMillis)
                    throw new RuntimeException("Timed out writing result to claimed slot");
            }

            publishedIndex = index;
        }

        /**
         * To claim a slot we simply do a CAS but we must be careful not to overwrite the
         * entries not yet read by the consumer and so we ensure the consumer is within LEN distance
         * from our claimed slot
         * */
        private int claimProducersIndex(long start)
        {
            int ret = producersIndex.incrementAndGet();
            while(ret - consumerIndex >= results.length)
            {
                Thread.yield(); //wait for consumer to catch up

                if (System.currentTimeMillis() - start > timeoutMillis)
                    throw new RuntimeException("Timed out waiting consumer to catch up");
            }
            return ret;
        }
    }

    @Override
    public void close()
    {
        if (!endReached)
        {
            cb.stop();
        }
    }

    @Override
    protected ResultSet computeNext() {
        try {
            if (endReached) {
                return endOfData();
            }

            if (outOfOrderResults.size() > 0) { // check the stash of out-of-order messages first
                Result ret = outOfOrderResults.peek();

                if (logger.isTraceEnabled())
                    logger.trace("Iterator checking lowest out of order msg {}, expecting {}", ret.seqNo, nextSeqNo);

                if (ret.seqNo == nextSeqNo) {
                    return getResult(outOfOrderResults.poll());
                }
            }

            while (true) {
                Result ret = queue.take();

                if (ret.seqNo > nextSeqNo && ret.exception == null) {
                    if (logger.isTraceEnabled())
                        logger.trace("Iterator got out of order msg {}, expected {}", ret.seqNo, nextSeqNo);
                    outOfOrderResults.offer(ret);
                }
                else {
                    return getResult(ret);
                }
            }
        }
        catch (Throwable t)
        {
            logger.error("Got exception in stream iterator", t);
            return endOfData();
        }
    }

    private ResultSet getResult(Result result) {
        if (logger.isTraceEnabled())
            logger.trace("Iterator returning {} - {}", result, result.resultSet);

        if (result.last) {
            endReached = true;
        }

        if (result.exception != null) {
            result.exception.printStackTrace();
            return endOfData();
        }

        nextSeqNo = result.seqNo + 1;
        return result.resultSet;
    }
}
