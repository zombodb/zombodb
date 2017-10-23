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
