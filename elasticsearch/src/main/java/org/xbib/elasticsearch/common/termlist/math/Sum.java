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
  * Returns the sum of the available values.
 *
 * If there are no values in the dataset, then 0 is returned.
 * If any of the values are
 * <code>NaN</code>, then <code>NaN</code> is returned.
 *
 * <strong>Note that this implementation is not synchronized.</strong> If
 * multiple threads access an instance of this class concurrently, and at least
 * one of the threads invokes the <code>increment()</code> or
 * <code>clear()</code> method, it must be synchronized externally.
 *
 */
public class Sum extends AbstractStorelessUnivariateStatistic {

    /** */
    private long n;

    /**
     * The currently running sum.
     */
    private double value;

    /**
     * Create a Sum instance
     */
    public Sum() {
        n = 0;
        value = 0;
    }

    @Override
    public void increment(final double d) {
        value += d;
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
        value = 0;
        n = 0;
    }

    /**
     * The sum of the entries in the specified portion of
     * the input array, or 0 if the designated subarray
     * is empty.
     *
     * Throws <code>IllegalArgumentException</code> if the array is null.
     *
     * @param values the input array
     * @param begin index of the first array element to include
     * @param length the number of elements to include
     * @return the sum of the values or 0 if length = 0
     * @throws IllegalArgumentException if the array is null or the array index
     *  parameters are not valid
     */
    @Override
    public double evaluate(final double[] values, final int begin, final int length) {
        double sum = Double.NaN;
        if (test(values, begin, length, true)) {
            sum = 0.0;
            for (int i = begin; i < begin + length; i++) {
                sum += values[i];
            }
        }
        return sum;
    }

    /**
     * The weighted sum of the entries in the specified portion of
     * the input array, or 0 if the designated subarray
     * is empty.
     *
     * Throws <code>IllegalArgumentException</code> if any of the following are true:
     * <ul><li>the values array is null</li>
     *     <li>the weights array is null</li>
     *     <li>the weights array does not have the same length as the values array</li>
     *     <li>the weights array contains one or more infinite values</li>
     *     <li>the weights array contains one or more NaN values</li>
     *     <li>the weights array contains negative values</li>
     *     <li>the start and length arguments do not determine a valid array</li>
     * </ul>
     *
     * Uses the formula, <pre>
     *    weighted sum = Sigma(values[i] * weights[i])
     * </pre>
     *
     * @param values the input array
     * @param weights the weights array
     * @param begin index of the first array element to include
     * @param length the number of elements to include
     * @return the sum of the values or 0 if length = 0
     * @throws IllegalArgumentException if the parameters are not valid
     */
    public double evaluate(final double[] values, final double[] weights,
                           final int begin, final int length) {
        double sum = Double.NaN;
        if (test(values, weights, begin, length, true)) {
            sum = 0.0;
            for (int i = begin; i < begin + length; i++) {
                sum += values[i] * weights[i];
            }
        }
        return sum;
    }

    /**
     * The weighted sum of the entries in the the input array.
     *
     * Throws <code>IllegalArgumentException</code> if any of the following are true:
     * <ul><li>the values array is null</li>
     *     <li>the weights array is null</li>
     *     <li>the weights array does not have the same length as the values array</li>
     *     <li>the weights array contains one or more infinite values</li>
     *     <li>the weights array contains one or more NaN values</li>
     *     <li>the weights array contains negative values</li>
     * </ul>
     *
     * Uses the formula, <pre>
     *    weighted sum = Sigma(values[i] * weights[i])
     * </pre>
     *
     * @param values the input array
     * @param weights the weights array
     * @return the sum of the values or Double.NaN if length = 0
     * @throws IllegalArgumentException if the parameters are not valid
     */
    public double evaluate(final double[] values, final double[] weights) {
        return evaluate(values, weights, 0, values.length);
    }

    public void merge(Sum sum) {
        n += sum.n;
        value += sum.value;
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