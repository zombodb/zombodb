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
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 * Computes a statistic related to the Second Central Moment.  Specifically,
 * what is computed is the sum of squared deviations from the sample mean.
 * The following recursive updating formula is used:
 * Let <ul>
 * <li> dev = (current obs - previous mean) </li>
 * <li> n = number of observations (including current obs) </li>
 * </ul>
 * Then
 * new value = old value + dev^2 * (n -1) / n.
 *
 * Returns <code>Double.NaN</code> if no data values have been added and
 * returns <code>0</code> if there is just one value in the data set.
 *
 * <strong>Note that this implementation is not synchronized.</strong> If
 * multiple threads access an instance of this class concurrently, and at least
 * one of the threads invokes the <code>increment()</code> or
 * <code>clear()</code> method, it must be synchronized externally.
 */
public class SecondMoment extends FirstMoment implements Streamable {

    /** second moment of values that have been added */
    protected double m2;

    protected double sum;

    protected double sumsquare;

    protected double sigma;

    public SecondMoment() {
        super();
        m2 = Double.NaN;
        sum = 0.0d;
        sumsquare = 0.0d;
        sigma = Double.NaN;
    }

    @Override
    public void increment(final double d) {
        if (n < 1) {
            m1 = m2 = 0.0;
        }
        super.increment(d);
        m2 += ((double) n - 1) * dev * nDev;

        // Satz von Steiner (Verschiebungssatz)
        sum += d;
        sumsquare += (d * d);
        sigma = n > 1 ? FastMath.sqrt((1.0d / (n-1.0d)) * (sumsquare - (n * m1 * m1) ) ) : 0.0d;
    }

    public void merge(SecondMoment m) {
        super.merge(m);
        sum += m.sum;
        sumsquare += m.sumsquare;
        m2 = sumsquare - (n * m1 * m1);
        sigma = n > 1 ? FastMath.sqrt((1.0d / (n-1.0d)) * (sumsquare - (n * m1 * m1) ) ) : 0.0d;
    }

    @Override
    public void clear() {
        super.clear();
        m2 = Double.NaN;
    }

    @Override
    public double getResult() {
        return m2;
    }

    public double getSum() {
        return sum;
    }

    public double getSumSquare() {
        return sumsquare;
    }

    public double getSigma() {
        return sigma;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        m2 = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeDouble(m2);
    }
}