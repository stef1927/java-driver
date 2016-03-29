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

import java.io.Closeable;
import java.util.Iterator;

/**
 * A row iterator over the entire query. This is a low level class intended for
 * analytics tools that must retrieve the entire query data set as quickly as possible.
 *
 * Paging can be configured via {@link AsyncPagingOptions} when calling
 * {@link Session#execute(Statement, AsyncPagingOptions)}. Paging is then done transparently
 * by the iterator.
 *
 * A session is kept open server side, if the data is not consumed quickly enough (at least one page
 * per RPC request timeout), this will result in timeout exceptions server side. The iterator
 * will also throw an exception if it fails to receive pages from the server within the same timeout.
 *
 * This iterator MUST be closed in order to release server side resources and off-heap memory. Further,
 * this iterator is not thread safe, it must be called from a single thread or it must be synchronized.
 */
public abstract class RowIterator implements Iterator<Row>, Closeable {

    /**
     * An empty iterator.
     */
    static RowIterator EMPTY = new RowIterator() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        @Override
        public Row next() {
            return null;
        }

        @Override
        public ColumnDefinitions getColumnDefinitions() {
            return null;
        }

        @Override
        public AsyncPagingOptions pagingOptions() {
            return null;
        }

        @Override
        public int pageNo() {
            return 0;
        }

        @Override
        public void close() {

        }

        @Override
        public State state() {
            return new State() {

                @Override
                public RowIterator resume() {
                    return EMPTY;
                }
            };

        }
    };

    /**
     * Returns the column definitions that can be used to decode a row. If using a prepared
     * statement, then a valid value should always be available. If not using a prepared statement,
     * then this will return null until the first page is received.
     *
     * @return the columns definitions related to the rows or null
     */
    public abstract ColumnDefinitions getColumnDefinitions();

    /**
     * Returns the paging options that were used to create the iterator,
     * see {@link Session#execute(Statement, AsyncPagingOptions)}.
     *
     * @return the paging options
     */
    public abstract AsyncPagingOptions pagingOptions();

    /**
     * Return the current page number, or zero if no pages are available. When resuming
     * a previous iteration, previous pages are not considered.
     *
     * @return The page number starting at one, or zero if no page is available.
     */
    public abstract int pageNo();

    /**
     * Interrupts the iteration, client and possible server side, releases any
     * off-heap memory in use.
     */
    public abstract void close();


    /**
     * A simple interface to capture the current state of the iteration. Use this
     * if you want to interrupt iteration and resume later on.
     */
    public interface State {
        /**
         * Resume iteration by creating a new iterator that will iterate rows where the
         * iterator that created the state stopped when the state was retrieved.
         *
         * @return a new row iterator
         */
        RowIterator resume();
    }

    /**
     * Retrieve the current state, this can be used to resume iteration later on, see {@link State}.
     * Calling this will also terminate the iteration, i.e. next time hasNext() is called, it will return
     * false.
     *
     * @return the current state, or null if none is available.
     */
    public abstract State state();
}

