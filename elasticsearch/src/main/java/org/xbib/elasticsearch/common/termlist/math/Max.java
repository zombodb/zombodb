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
 * Returns the maximum of the available values.
 *
 * <ul>
 * <li>The result is <code>NaN</code> iff all values are <code>NaN</code>
 * (i.e. <code>NaN</code> values have no impact on the value of the statistic).</li>
 * <li>If any of the values equals <code>Double.POSITIVE_INFINITY</code>,
 * the result is <code>Double.POSITIVE_INFINITY.</code></li>
 * </ul>
 *
 * <strong>Note that this implementation is not synchronized.</strong> If
 * multiple threads access an instance of this class concurrently, and at least
 * one of the threads invokes the <code>increment()</code> or
 * <code>clear()</code> method, it must be synchronized externally.
 *
 */
public class Max extends AbstractStorelessUnivariateStatistic {

    /**
     * Number of values that have been added
     */
    private long n;

    /**
     * Current value of the statistic
     */
    private double value;

    /**
     * Create a Max instance
     */
    public Max() {
        n = 0;
        value = Double.NaN;
    }

    @Override
    public void increment(final double d) {
        if (d > value || Double.isNaN(value)) {
            value = d;
        }
        n++;
    }

    @Override
    public void clear() {
        value = Double.NaN;
        n = 0;
    }

    @Override
    public double getResult() {
        return value;
    }

    public long getN() {
        return n;
    }

    /**
     * Returns the maximum of the entries in the specified portion of
     * the input array, or <code>Double.NaN</code> if the designated subarray
     * is empty.
     *
     * Throws <code>IllegalArgumentException</code> if the array is null or
     * the array index parameters are not valid.
     *
     * <ul>
     * <li>The result is <code>NaN</code> iff all values are <code>NaN</code>
     * (i.e. <code>NaN</code> values have no impact on the value of the statistic).</li>
     * <li>If any of the values equals <code>Double.POSITIVE_INFINITY</code>,
     * the result is <code>Double.POSITIVE_INFINITY.</code></li>
     * </ul>
     *
     * @param values the input array
     * @param begin  index of the first array element to include
     * @param length the number of elements to include
     * @return the maximum of the values or Double.NaN if length = 0
     * @throws IllegalArgumentException if the array is null or the array index
     *                                  parameters are not valid
     */
    @Override
    public double evaluate(final double[] values, final int begin, final int length) {
        double max = Double.NaN;
        if (test(values, begin, length)) {
            max = values[begin];
            for (int i = begin; i < begin + length; i++) {
                if (!Double.isNaN(values[i])) {
                    max = (max > values[i]) ? max : values[i];
                }
            }
        }
        return max;
    }

    public void merge(Max max) {
        n += max.n;
        if (max.value > value || Double.isNaN(value)) {
            value = max.value;
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        n = in.readLong();
        value = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(n);
        out.writeDouble(value);
    }
}