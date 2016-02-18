/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.common.termlist.math;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Returns the sum of the natural logs for this collection of values.
 *
 * Uses {@link FastMath#log(double)} to compute the logs.
 * Therefore,
 * <ul>
 * <li>If any of values are &lt; 0, the result is <code>NaN.</code></li>
 * <li>If all values are non-negative and less than
 * <code>Double.POSITIVE_INFINITY</code>,  but at least one value is 0, the
 * result is <code>Double.NEGATIVE_INFINITY.</code></li>
 * <li>If both <code>Double.POSITIVE_INFINITY</code> and
 * <code>Double.NEGATIVE_INFINITY</code> are among the values, the result is
 * <code>NaN.</code></li>
 * </ul>
 *
 * <strong>Note that this implementation is not synchronized.</strong> If
 * multiple threads access an instance of this class concurrently, and at least
 * one of the threads invokes the <code>increment()</code> or
 * <code>clear()</code> method, it must be synchronized externally.
 */
public class SumOfLogs extends AbstractStorelessUnivariateStatistic {

    /**Number of values that have been added */
    private int n;

    /**
     * The currently running value
     */
    private double value;

    /**
     * Create a SumOfLogs instance
     */
    public SumOfLogs() {
       value = 0d;
       n = 0;
    }

    @Override
    public void increment(final double d) {
        value += FastMath.log(d);
        n++;
    }

    @Override
    public double getResult() {
        return value;
    }

    public long getN() {
        return n;
    }

    @Override
    public void clear() {
        value = 0d;
        n = 0;
    }

    /**
     * Returns the sum of the natural logs of the entries in the specified portion of
     * the input array, or <code>Double.NaN</code> if the designated subarray
     * is empty.
     *
     * Throws <code>IllegalArgumentException</code> if the array is null.
     *
     * See {@link SumOfLogs}.
     *
     * @param values the input array
     * @param begin index of the first array element to include
     * @param length the number of elements to include
     * @return the sum of the natural logs of the values or 0 if
     * length = 0
     * @throws IllegalArgumentException if the array is null or the array index
     *  parameters are not valid
     */
    @Override
    public double evaluate(final double[] values, final int begin, final int length) {
        double sumLog = Double.NaN;
        if (test(values, begin, length, true)) {
            sumLog = 0.0;
            for (int i = begin; i < begin + length; i++) {
                sumLog += FastMath.log(values[i]);
            }
        }
        return sumLog;
    }

    public void merge(SumOfLogs sumOfLogs) {
        n += sumOfLogs.n;
        value += sumOfLogs.value;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        n = in.readInt();
        value = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(n);
        out.writeDouble(value);
    }
}