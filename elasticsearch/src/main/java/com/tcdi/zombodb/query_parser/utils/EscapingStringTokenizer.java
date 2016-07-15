/*
 * Copyright 2013-2015 Technology Concepts & Design, Inc
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
package com.tcdi.zombodb.query_parser.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Mimics the behavior of <code>java.util.StringTokenizer</code>, except
 * if a delimiter is escaped (preceded with a backslash <i>\</i>, that delimiter
 * is included as part of the token and the backslash is removed.<p>
 * <p/>
 * If a backslash appears anywhere else (ie, not directly before a delimiter
 * character) it <b>is</b> included as part of the token.
 *
 * @author e_ridge
 * @see java.util.StringTokenizer
 */
public class EscapingStringTokenizer {
    private final String input;
    private final int len;
    private final String delimiters;
    private final boolean returnDelims;
    private final StringBuilder token = new StringBuilder();

    private int pos = 0;
    private boolean isDelimiter = false;

    public EscapingStringTokenizer(String str, String delim) {
        this(str, delim, false);
    }

    public EscapingStringTokenizer(String str, String delimiters, boolean returnDelims) {
        this.input = str;
        this.len = input.length();
        this.delimiters = delimiters;
        this.returnDelims = returnDelims;
    }

    public boolean hasMoreTokens() {
        int tmp = pos;
        try {
            String next = nextToken();
            return !next.equals("");
        } catch (NoSuchElementException nsee) {
            return false;
        } finally {
            pos = tmp;
        }
    }

    public String nextToken() {
        final StringBuilder tok = this.token;
        final String in = this.input;
        final int l = this.len;
        final boolean retDelims = this.returnDelims;
        final String delims = this.delimiters;

        if (pos >= len)
            throw new NoSuchElementException();

        isDelimiter = false;
        tok.setLength(0);
        while (pos < l) {
            char ch = in.charAt(pos++);

            if (ch != '\\' || pos >= l) {
                if (delims.indexOf(ch) >= 0) { // char is a delimiter
                    if (tok.length() == 0) { // and it's the first char we've found
                        if (retDelims) { // and we're asked to return them
                            isDelimiter = true;
                            return String.valueOf(ch);
                        } else {
                            continue;
                        }
                    } else { // we have a token
                        --pos;
                        return tok.toString();
                    }
                }
                tok.append(ch);
            } else {
                char nextch = in.charAt(pos);
                tok.append(nextch);
                ++pos;
            }
        }
        return tok.toString();
    }

    public List<String> getAllTokens() {
        try {
            List<String> tokens = new ArrayList<>();
            while (hasMoreTokens())
                tokens.add(nextToken());
            return tokens;
        } finally {
            // reset state
            isDelimiter = false;
            pos = 0;
        }
    }

    /**
     * is the last token returned a delimiter?
     */
    public boolean isDelimiter() {
        return isDelimiter;
    }
}
