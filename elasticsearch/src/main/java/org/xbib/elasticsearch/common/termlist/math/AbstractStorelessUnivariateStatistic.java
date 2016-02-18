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

/**
 * Provides default <code>evaluate()</code> and <code>incrementAll(double[])</code>
 * implementations.
 *
 * <strong>Note that these implementations are not synchronized.</strong>
 */
public abstract class AbstractStorelessUnivariateStatistic
    extends AbstractUnivariateStatistic
    implements StorelessUnivariateStatistic {

    /**
     * This default implementation calls {@link #clear}, then invokes
     * {@link #increment} in a loop over the the input array, and then uses
     * {@link #getResult} to compute the return value.
     *
     * Note that this implementation changes the internal state of the
     * statistic.  Its side effects are the same as invoking {@link #clear} and
     * then {@link #incrementAll(double[])}.
     *
     * Implementations may override this method with a more efficient and
     * possibly more accurate implementation that works directly with the
     * input array.
     *
     * If the array is null, an IllegalArgumentException is thrown.
     * @param values input array
     * @return the value of the statistic applied to the input array
     */
    @Override
    public double evaluate(final double[] values) {
        if (values == null) {
            throw new IllegalArgumentException();
        }
        return evaluate(values, 0, values.length);
    }

    /**
     * This default implementation calls {@link #clear}, then invokes
     * {@link #increment} in a loop over the specified portion of the input
     * array, and then uses {@link #getResult} to compute the return value.
     *
     * Note that this implementation changes the internal state of the
     * statistic.  Its side effects are the same as invoking {@link #clear} and
     * then {@link #incrementAll(double[], int, int)}.
     *
     * Implementations may override this method with a more efficient and
     * possibly more accurate implementation that works directly with the
     * input array.
     *
     * If the array is null or the index parameters are not valid, an
     * IllegalArgumentException is thrown.
     * @param values the input array
     * @param begin the index of the first element to include
     * @param length the number of elements to include
     * @return the value of the statistic applied to the included array entries
     */
    @Override
    public double evaluate(final double[] values, final int begin, final int length) {
        if (test(values, begin, length)) {
            clear();
            incrementAll(values, begin, length);
        }
        return getResult();
    }

    public abstract void clear();

    public abstract double getResult();

    public abstract void increment(final double d);

    /**
     * This default implementation just calls {@link #increment} in a loop over
     * the input array.
     * <p>
     * Throws IllegalArgumentException if the input values array is null.</p>
     *
     * @param values values to add
     * @throws IllegalArgumentException if values is null
     */
    public void incrementAll(double[] values) {
        if (values == null) {
            throw new IllegalArgumentException();
        }
        incrementAll(values, 0, values.length);
    }

    /**
     * This default implementation just calls {@link #increment} in a loop over
     * the specified portion of the input array.
     * <p>
     * Throws IllegalArgumentException if the input values array is null.</p>
     *
     * @param values  array holding values to add
     * @param begin   index of the first array element to add
     * @param length  number of array elements to add
     * @throws IllegalArgumentException if values is null
     */
    public void incrementAll(double[] values, int begin, int length) {
        if (test(values, begin, length)) {
            int k = begin + length;
            for (int i = begin; i < k; i++) {
                increment(values[i]);
            }
        }
    }

    /**
     * Returns true iff <code>object</code> is an
     * <code>AbstractStorelessUnivariateStatistic</code> returning the same
     * values as this for <code>getResult()</code> and <code>getN()</code>
     * @param object object to test equality against.
     * @return true if object returns the same value as this
     */
    @Override
    public boolean equals(Object object) {
        if (object == this ) {
            return true;
        }
       if (!(object instanceof AbstractStorelessUnivariateStatistic)) {
            return false;
        }
        AbstractStorelessUnivariateStatistic stat = (AbstractStorelessUnivariateStatistic) object;
        return Precision.equalsIncludingNaN(stat.getResult(), this.getResult()) &&
               Precision.equalsIncludingNaN(stat.getN(), this.getN());
    }

    /**
     * Returns hash code based on getResult() and getN()
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return 31* (31 + new Double(getResult()).hashCode()) + new Double(getN()).hashCode();
    }

}