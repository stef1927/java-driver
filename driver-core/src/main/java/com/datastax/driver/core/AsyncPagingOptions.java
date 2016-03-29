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

import java.util.UUID;

public class AsyncPagingOptions {

    public enum PageUnit
    {
        BYTES(1),
        ROWS(2);

        public final int id;
        PageUnit(int id)
        {
            this.id = id;
        }
    }

    private static final UUID NO_ASYNC_PAGING_UUID = new UUID(0, 0);
    public static final int DEFAULT_PAGE_SIZE_BYTES = 64 * 1024;

    public static final AsyncPagingOptions NO_PAGING = new AsyncPagingOptions(NO_ASYNC_PAGING_UUID);
    public static final AsyncPagingOptions DEFAULT_PAGING = new AsyncPagingOptions(UUID.randomUUID());

    public static final AsyncPagingOptions create(int pageSize, PageUnit pageUnit) {
        return new AsyncPagingOptions(UUID.randomUUID(), pageSize, pageUnit, 0, 0);
    }

    public static final AsyncPagingOptions create(int pageSize, PageUnit pageUnit, int maxPages) {
        return new AsyncPagingOptions(UUID.randomUUID(), pageSize, pageUnit, maxPages, 0);
    }

    public static final AsyncPagingOptions create(int pageSize, PageUnit pageUnit, int maxPages, int maxPagesPerSecond) {
        return new AsyncPagingOptions(UUID.randomUUID(), pageSize, pageUnit, maxPages, maxPagesPerSecond);
    }

    public final UUID id;
    public final int pageSize;
    public final PageUnit pageUnit;
    public final int maxPages;
    public final int maxPagesPerSecond;

    private AsyncPagingOptions(UUID id) {
        this(id, DEFAULT_PAGE_SIZE_BYTES,  PageUnit.BYTES, 0, 0);
    }

    private AsyncPagingOptions(UUID id, int pageSize, PageUnit pageUnit, int maxPages, int maxPagesPerSecond) {
        this.id = id;
        this.pageSize = pageSize;
        this.pageUnit = pageUnit;
        this.maxPages = maxPages;
        this.maxPagesPerSecond = maxPagesPerSecond;
    }

    AsyncPagingOptions withNewId()
    {
        return new AsyncPagingOptions(UUID.randomUUID(), pageSize, pageUnit, maxPages, maxPagesPerSecond);
    }

    @Override
    public String toString() {
        return String.format("async-paging-options=%s,%d %s,%d,%d",
                id, pageSize, pageUnit.name(), maxPages, maxPagesPerSecond);
    }

    public boolean enabled() {
        return !id.equals(NO_ASYNC_PAGING_UUID);
    }
}
