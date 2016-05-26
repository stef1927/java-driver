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

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

class AsyncResultSetIterator extends ResultSetIterator {
    private static final Logger logger = LoggerFactory.getLogger(AsyncResultSetIterator.class);

    private final AsyncRequestHandlerCallback cb;
    private final PriorityBlockingQueue<AsyncRequestHandlerCallback.Result> queue;
    private final long readTimeoutMillis;

    private int nextSeqNo = 1;
    private boolean endReached = false;
    private long outOfSequenceReceivedAt = 0;


    AsyncResultSetIterator(AsyncRequestHandlerCallback cb,
                           Statement statement,
                           Cluster cluster,
                           PriorityBlockingQueue<AsyncRequestHandlerCallback.Result> queue) {
        this.cb = cb;
        this.queue = queue;
        this.readTimeoutMillis = statement.getReadTimeoutMillis() > 0
                ? statement.getReadTimeoutMillis()
                : cluster.manager.connectionFactory.getReadTimeoutMillis();

        this.nextSeqNo = 1;
        this.endReached = false;
        this.outOfSequenceReceivedAt = 0;
    }

    @Override
    public void close()
    {
        if (!endReached)
        {
            cb.stop();
            queue.clear();
        }
    }

    @Override
    protected ResultSet computeNext() {
        try {
            if (endReached) {
                return endOfData();
            }

            AsyncRequestHandlerCallback.Result ret;
            while (true) {
                ret = queue.poll(readTimeoutMillis, TimeUnit.MILLISECONDS);
                if (ret == null) {
                    logger.error("Failed to receive any message in {} milliseconds for {} at seq no. {}",
                            readTimeoutMillis, cb.asyncId(), nextSeqNo);
                    return endOfData();
                }
                if (ret.seqNo > nextSeqNo) {
                    if (outOfSequenceReceivedAt == 0) {
                        outOfSequenceReceivedAt = System.nanoTime();
                    }
                    else if (System.nanoTime() - outOfSequenceReceivedAt > TimeUnit.MILLISECONDS.toNanos(readTimeoutMillis)) {
                        logger.error("Failed to receive expected seq no. {}, given up", nextSeqNo);
                        return endOfData();
                    }

                    logger.debug("Iterator got out of order msg {}, expected {}, retrying", ret.seqNo, nextSeqNo);
                    queue.put(ret);
                    Thread.sleep(1);
                }
                else {
                    if (ret.seqNo == nextSeqNo)
                        nextSeqNo++;

                    if (outOfSequenceReceivedAt != 0)
                        outOfSequenceReceivedAt = 0;

                    break; // error or correct seqNo received
                }
            }

            if (logger.isTraceEnabled())
                logger.trace("Iterator got {} - {}, remaining in queue {}", ret, ret.resultSet, queue.size());

            if (ret.exception != null) {
                ret.exception.printStackTrace();
                return endOfData();
            }

            if (ret.last) {
                endReached = true;
            }

            return ret.resultSet;
        }
        catch (Throwable t)
        {
            logger.error("Got exception in stream iterator", t);
            return endOfData();
        }
    }
}
