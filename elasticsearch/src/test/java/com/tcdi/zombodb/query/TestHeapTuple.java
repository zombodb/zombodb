/*
 * Copyright 2017 ZomboDB, LLC
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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestHeapTuple {

    @Test
    public void testHeapTupleComparator() throws Exception {

        assertTrue(new HeapTuple(41, 0).compareTo(new HeapTuple(42, 0)) < 0);
        assertTrue(new HeapTuple(42, 0).compareTo(new HeapTuple(42, 0)) == 0);
        assertTrue(new HeapTuple(43, 0).compareTo(new HeapTuple(42, 0)) > 0);

        assertTrue(new HeapTuple(0, 1).compareTo(new HeapTuple(0, 0)) > 0);
        assertTrue(new HeapTuple(0, 1).compareTo(new HeapTuple(0, 1)) == 0);
        assertTrue(new HeapTuple(0, 1).compareTo(new HeapTuple(0, 2)) < 0);

        assertTrue(new HeapTuple(42, 42).compareTo(new HeapTuple(0, 43)) > 0);
        assertTrue(new HeapTuple(42, 42).compareTo(new HeapTuple(42, 42)) == 0);
        assertTrue(new HeapTuple(42, 42).compareTo(new HeapTuple(43, 42)) < 0);
    }

    @Test
    public void testHeapTupleEquality() throws Exception {
        assertEquals(new HeapTuple(42, 0), new HeapTuple(42, 0));
        assertNotEquals(new HeapTuple(41, 0), new HeapTuple(42, 0));
    }
}
