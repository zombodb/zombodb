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

import org.elasticsearch.common.io.stream.Streamable;

/**
 * Abstract base class for all implementations of the
 * {@link UnivariateStatistic} interface.
 *
 * Provides a default implementation of <code>evaluate(double[]),</code>
 * delegating to <code>evaluate(double[], int, int)</code> in the natural way.
 *
 *
 * Also includes a <code>test</code> method that performs generic parameter
 * validation for the <code>evaluate</code> methods.
 *
 */
public abstract class AbstractUnivariateStatistic
    implements UnivariateStatistic, Streamable {

    /** Stored data. */
    private double[] storedData;

    /**
     * Set the data array.
     *
     * The stored value is a copy of the parameter array, not the array itself
     *
     * @param values data array to store (may be null to remove stored data)
     * @see #evaluate()
     */
    public void setData(final double[] values) {
        storedData = (values == null) ? null : values.clone();
    }

    /**
     * Get a copy of the stored data array.
     * @return copy of the stored data array (may be null)
     */
    public double[] getData() {
        return (storedData == null) ? null : storedData.clone();
    }

    /**
     * Get a reference to the stored data array.
     * @return reference to the stored data array (may be null)
     */
    protected double[] getDataRef() {
        return storedData;
    }

    /**
     * Set the data array.
     * @param values data array to store
     * @param begin the index of the first element to include
     * @param length the number of elements to include
     * @see #evaluate()
     */
    public void setData(final double[] values, final int begin, final int length) {
        storedData = new double[length];
        System.arraycopy(values, begin, storedData, 0, length);
    }

    /**
     * Returns the result of evaluating the statistic over the stored data.
     * The stored array is the one which was set by previous calls to
     * @return the value of the statistic applied to the stored data
     */
    public double evaluate() {
        return evaluate(storedData);
    }

    public double evaluate(final double[] values) {
        test(values, 0, 0);
        return evaluate(values, 0, values.length);
    }

    public abstract double evaluate(final double[] values, final int begin, final int length);

    /**
     * This method is used by <code>evaluate(double[], int, int)</code> methods
     * to verify that the input parameters designate a subarray of positive length.
     * <ul>
     * <li>returns <code>true</code> iff the parameters designate a subarray of
     * positive length</li>
     * <li>throws <code>IllegalArgumentException</code> if the array is null or
     * or the indices are invalid</li>
     * <li>returns <code>false</code> if the array is non-null, but
     * <code>length</code> is 0.</li>
     * </ul>
     *
     * @param values the input array
     * @param begin index of the first array element to include
     * @param length the number of elements to include
     * @return true if the parameters are valid and designate a subarray of positive length
     * @throws IllegalArgumentException if the indices are invalid or the array is null
     */
    protected boolean test(
        final double[] values,
        final int begin,
        final int length) {
        return test(values, begin, length, false);
    }

    /**
     * This method is used by <code>evaluate(double[], int, int)</code> methods
     * to verify that the input parameters designate a subarray of positive length.
     *
     * <ul>
     * <li>returns <code>true</code> iff the parameters designate a subarray of
     * non-negative length</li>
     * <li>throws <code>IllegalArgumentException</code> if the array is null or
     * or the indices are invalid</li>
     * <li>returns <code>false</code> if the array is non-null, but
     * <code>length</code> is 0 unless <code>allowEmpty</code> is <code>true</code></li>
     * </ul>
     *
     * @param values the input array
     * @param begin index of the first array element to include
     * @param length the number of elements to include
     * @param allowEmpty if <code>true</code> then zero length arrays are allowed
     * @return true if the parameters are valid
     * @throws IllegalArgumentException if the indices are invalid or the array is null
     */
    protected boolean test(final double[] values, final int begin, final int length, final boolean allowEmpty){
        if (values == null) {
            throw new IllegalArgumentException("values is null");
        }
        if (begin < 0) {
            throw new IllegalArgumentException("begin < 0");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length < 0");
        }
        if (begin + length > values.length) {
            throw new IllegalArgumentException("begin + length > values.length");
        }
        if (length == 0 && !allowEmpty) {
            return false;
        }
        return true;

    }

    /**
     * This method is used by <code>evaluate(double[], double[], int, int)</code> methods
     * to verify that the begin and length parameters designate a subarray of positive length
     * and the weights are all non-negative, non-NaN, finite, and not all zero.
     *
     * <ul>
     * <li>returns <code>true</code> iff the parameters designate a subarray of
     * positive length and the weights array contains legitimate values.</li>
     * <li>throws <code>IllegalArgumentException</code> if any of the following are true:
     * <ul><li>the values array is null</li>
     *     <li>the weights array is null</li>
     *     <li>the weights array does not have the same length as the values array</li>
     *     <li>the weights array contains one or more infinite values</li>
     *     <li>the weights array contains one or more NaN values</li>
     *     <li>the weights array contains negative values</li>
     *     <li>the start and length arguments do not determine a valid array</li></ul>
     * </li>
     * <li>returns <code>false</code> if the array is non-null, but
     * <code>length</code> is 0.</li>
     * </ul>
     *
     * @param values the input array
     * @param weights the weights array
     * @param begin index of the first array element to include
     * @param length the number of elements to include
     * @return true if the parameters are valid and designate a subarray of positive length
     * @throws IllegalArgumentException if the indices are invalid or the array is null
     */
    protected boolean test(
        final double[] values,
        final double[] weights,
        final int begin,
        final int length) {
        return test(values, weights, begin, length, false);
    }

    /**
     * This method is used by <code>evaluate(double[], double[], int, int)</code> methods
     * to verify that the begin and length parameters designate a subarray of positive length
     * and the weights are all non-negative, non-NaN, finite, and not all zero.
     *
     * <ul>
     * <li>returns <code>true</code> iff the parameters designate a subarray of
     * non-negative length and the weights array contains legitimate values.</li>
     * <li>throws <code>IllegalArgumentException</code> if any of the following are true:
     * <ul><li>the values array is null</li>
     *     <li>the weights array is null</li>
     *     <li>the weights array does not have the same length as the values array</li>
     *     <li>the weights array contains one or more infinite values</li>
     *     <li>the weights array contains one or more NaN values</li>
     *     <li>the weights array contains negative values</li>
     *     <li>the start and length arguments do not determine a valid array</li></ul>
     * </li>
     * <li>returns <code>false</code> if the array is non-null, but
     * <code>length</code> is 0 unless <code>allowEmpty</code> is <code>true</code>.</li>
     * </ul>
     *
     * @param values the input array.
     * @param weights the weights array.
     * @param begin index of the first array element to include.
     * @param length the number of elements to include.
     * @param allowEmpty if {@code true} than allow zero length arrays to pass.
     * @return {@code true} if the parameters are valid.
     * @throws IllegalArgumentException if the indices are invalid or the array
     * is {@code null}.
     */
    protected boolean test(final double[] values, final double[] weights, final int begin, final int length, final boolean allowEmpty){

        if (weights == null) {
            throw new IllegalArgumentException("weights == null");
        }

        if (weights.length != values.length) {
            throw new IllegalArgumentException("weights.length != values.length");
        }

        boolean containsPositiveWeight = false;
        for (int i = begin; i < begin + length; i++) {
            if (Double.isNaN(weights[i])) {
                throw new IllegalArgumentException("Double.isNaN(weights[i])");
            }
            if (Double.isInfinite(weights[i])) {
                throw new IllegalArgumentException("Double.isInfinite(weights[i])");
            }
            if (weights[i] < 0) {
                throw new IllegalArgumentException("weights[i] < 0");
            }
            if (!containsPositiveWeight && weights[i] > 0.0) {
                containsPositiveWeight = true;
            }
        }

        if (!containsPositiveWeight) {
            throw new IllegalArgumentException("containsPositiveWeight");
        }

        return test(values, begin, length, allowEmpty);
    }
}
