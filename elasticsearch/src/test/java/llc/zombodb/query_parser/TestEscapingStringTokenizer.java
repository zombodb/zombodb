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
package llc.zombodb.query_parser;

import llc.zombodb.query_parser.utils.EscapingStringTokenizer;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class TestEscapingStringTokenizer {


    @Test
    public void testIt() throws Exception {
        EscapingStringTokenizer st = new EscapingStringTokenizer("this is a test", " ", false);
        assertEquals(Arrays.asList("this", "is", "a", "test"), st.getAllTokens());
    }

    @Test
    public void testProperEscapingReturnDelimiter() throws Exception {
        EscapingStringTokenizer st = new EscapingStringTokenizer("\\\\\\\\*", "*", true);
        assertEquals(Arrays.asList("\\\\", "*"), st.getAllTokens());
    }

    @Test
    public void testProperEscapingReturnDelimiter2() throws Exception {
        EscapingStringTokenizer st = new EscapingStringTokenizer("\\*", "*", true);
        String token = st.nextToken();
        assertFalse(st.isDelimiter());
        assertEquals("*", token);
    }

    @Test
    public void testProperEscapingReturnDelimiter3() throws Exception {
        EscapingStringTokenizer st = new EscapingStringTokenizer("\\\\*", "*", true);
        assertEquals(Arrays.asList("\\", "*"), st.getAllTokens());
    }

    @Test
    public void testProperEscapingReturnDelimiter4() throws Exception {
        EscapingStringTokenizer st = new EscapingStringTokenizer("* * * *", "*", true);
        int cnt = 0;
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (cnt % 2 == 0) {
                assertEquals("*", token);
                assertTrue(st.isDelimiter());
            } else {
                assertEquals(" ", token);
                assertFalse(st.isDelimiter());
            }
            cnt++;
        }
    }

    @Test
    public void testProperEscapingReturnDelimiter5() throws Exception {
        EscapingStringTokenizer st = new EscapingStringTokenizer("\\* \\* \\* \\*", "*", true);
        assertEquals("* * * *", st.nextToken());
    }

    @Test
    public void testMultiDelimitersNoReturn() throws Exception {
        EscapingStringTokenizer st = new EscapingStringTokenizer("[[a,b,c,d]]", ", \r\n\t\f\"'[]", false);
        assertEquals(Arrays.asList("a", "b", "c", "d"), st.getAllTokens());
    }
}
