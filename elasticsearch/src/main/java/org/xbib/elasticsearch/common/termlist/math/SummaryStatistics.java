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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.Serializable;

/**
 * Computes summary statistics for a stream of data values added using the
 * {@link #addValue(double) addValue} method. The data values are not stored in
 * memory, so this class can be used to compute statistics for very large data
 * streams.
 */
public class SummaryStatistics implements Streamable, ToXContent, Serializable {

    /** min of values that have been added */
    private Min min = new Min();

    /** max of values that have been added */
    private Max max = new Max();

    /** SecondMoment is used to compute the mean and variance */
    private SecondMoment secondMoment = new SecondMoment();

    /** mean of values that have been added */
    private Mean mean = new Mean(secondMoment);

    /** variance of values that have been added */
    private Variance variance = new Variance(secondMoment);

    /** sumLog of values that have been added */
    private SumOfLogs sumlog = new SumOfLogs();

    /** geoMean of values that have been added */
    private GeometricMean geoMean = new GeometricMean(sumlog);

    /**
     * Construct a SummaryStatistics instance
     */
    public SummaryStatistics() {
    }

    /**
     * Add a value to the data
     * @param value the value to add
     */
    public void addValue(double value) {
        min.increment(value);
        max.increment(value);
        sumlog.increment(value);
        secondMoment.increment(value);
    }

    /**
     * Returns the number of available values
     * @return The number of available values
     */
    public long getN() {
        return secondMoment.getN();
    }

    /**
     * Returns the sum of the values that have been added
     * @return The sum or <code>Double.NaN</code> if no values have been added
     */
    public double getSum() {
        return secondMoment.getSum();
    }

    /**
     * Returns the sum of the squares of the values that have been added.
     * Double.NaN is returned if no values have been added.
     * @return The sum of squares
     */
    public double getSumsq() {
        return secondMoment.getSumSquare();
    }


    public  double getSigma() {
        return secondMoment.getSigma();
    }

    /**
     * Returns the mean of the values that have been added.
     * Double.NaN is returned if no values have been added.
     * @return the mean
     */
    public double getMean() {
        return mean.getResult();
    }

    /**
     * Returns the (sample) variance of the available values.
     * This method returns the bias-corrected sample variance (using {@code n - 1} in
     * the denominator).
     * Double.NaN is returned if no values have been added.
     *
     * @return the variance
     */
    public double getVariance() {
        return variance.getResult();
    }

    /**
     * Returns the maximum of the values that have been added.
     * Double.NaN is returned if no values have been added.
     * @return the maximum
     */
    public double getMax() {
        return max.getResult();
    }

    /**
     * Returns the minimum of the values that have been added.
     * Double.NaN is returned if no values have been added.
     * @return the minimum
     */
    public double getMin() {
        return min.getResult();
    }


    /**
     * Returns the sum of the logs of the values that have been added.
     * Double.NaN is returned if no values have been added.
     * @return the sum of logs
     */
    public double getSumOfLogs() {
        return sumlog.getResult();
    }

    /**
     * Returns the geometric mean of the values that have been added.
     * Double.NaN is returned if no values have been added.
     * @return the geometric mean
     */
    public double getGeometricMean() {
        return geoMean.getResult();
    }

    /**
     * Generates a text report displaying summary statistics from values that
     * have been added.
     * @return String with line feeds displaying statistics
     */
    @Override
    public String toString() {
        String endl = "\n";
        return "SummaryStatistics:" + endl + "n: " + getN() + endl
                + "min: " + getMin() + endl
                + "max: " + getMax() + endl
                + "mean: " + getMean() + endl
                + "geometric mean: " + getGeometricMean() + endl
                + "sum of squares: " + getSumsq() + endl
                + "sum of logs: " + getSumOfLogs() + endl
                + "standard deviation: " + getSigma() + endl
                + "variance: " + getVariance() + endl;
    }

    /**
     * Resets all statistics and storage
     */
    public void clear() {
        min.clear();
        max.clear();
        sumlog.clear();
        secondMoment.clear();
        geoMean.clear();
        mean.clear();
        variance.clear();
    }

    public void update(SummaryStatistics other) {
        min.merge(other.min);
        max.merge(other.max);
        sumlog.merge(other.sumlog);
        secondMoment.merge(other.secondMoment);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        min.writeTo(out);
        max.writeTo(out);
        sumlog.writeTo(out);
        secondMoment.writeTo(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        min = new Min();
        min.readFrom(in);
        max = new Max();
        max.readFrom(in);
        sumlog = new SumOfLogs();
        sumlog.readFrom(in);
        secondMoment = new SecondMoment();
        secondMoment.readFrom(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("min", getMin())
                .field("max", getMax())
                .field("mean", getMean())
                .field("geomean", getGeometricMean())
                .field("sumofsquares", getSumsq())
                .field("sumoflogs", getSumOfLogs())
                .field("sigma", getSigma())
                .field("variance", getVariance());
        return builder;
    }
}