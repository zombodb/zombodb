/*
 * Copyright 2016 ZomboDB, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tcdi.zombodb.query;

class VisibilityInfo {
    final int readerOrd;
    final int maxdoc;
    final int docid;
    public final long xid;

    VisibilityInfo(int readerOrd, int maxdoc, int docid, long xid) {
        this.readerOrd = readerOrd;
        this.maxdoc = maxdoc;
        this.docid = docid;
        this.xid = xid;
    }
}
