/**
 Copyright (C) 2011-2015 Jörg Prante

 Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 specific language governing permissions and limitations under the License.
 */
package org.xbib.elasticsearch.common.termlist.math;


import org.junit.Assert;
import org.junit.Test;

public class TestStatisticalMath extends Assert {

    @Test
    public void testOneSequence() throws Exception {
        int[] numbers = new int[] { 32, 34, 34, 42};
        SummaryStatistics stat = new SummaryStatistics();
        for (int n : numbers) {
            stat.addValue(n);
        }
        assertEquals(stat.getN(), 4);
        assertEquals(stat.getSum(), 142.0, 0);
        assertEquals(stat.getSumsq(), 5100.0, 0);
        assertEquals(stat.getMean(), 35.5, 0);
        assertEquals(stat.getSigma(), 4.43471156521669, 0);
        assertEquals(stat.getVariance(), 19.666666666666654, 0);
    }

    @Test
    public void testTwoSequences() throws Exception {
        int[] numbers1 = new int[] { 32, 34 };
        SummaryStatistics stat = new SummaryStatistics();
        for (int n : numbers1) {
            stat.addValue(n);
        }
        int[] numbers2 = new int[] { 34, 42 };
        SummaryStatistics stat2 = new SummaryStatistics();
        for (int n : numbers2) {
            stat2.addValue(n);
        }

        stat.update(stat2);

        assertEquals(stat.getN(), 4);
        assertEquals(stat.getSum(), 142.0, 0);
        assertEquals(stat.getSumsq(), 5100.0, 0);
        assertEquals(stat.getMean(), 35.5, 0);
        assertEquals(stat.getSigma(), 4.43471156521669, 0);
        assertEquals(stat.getVariance(), 19.666666666666668, 0); // rounding error
    }

}
