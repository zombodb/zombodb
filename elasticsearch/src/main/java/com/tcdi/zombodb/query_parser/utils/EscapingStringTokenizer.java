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
import java.util.Collection;
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
    private String input;
    private String delimiters;
    private boolean returnDelims;
    private int pos = 0;
    private boolean isDelimiter = false;

    public EscapingStringTokenizer(String str, String delim) {
        this(str, delim, false);
    }

    public EscapingStringTokenizer(String str, String delimiters, boolean returnDelims) {
        input = str;
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
        if (pos >= input.length())
            throw new NoSuchElementException();

        isDelimiter = false;
        StringBuilder token = new StringBuilder();
        while (pos < input.length()) {
            char ch = input.charAt(pos++);

            if (ch != '\\' || pos >= input.length()) {
                if (isDelimiter(ch)) { // char is a delimiter
                    if (token.length() == 0) { // and it's the first char we've found
                        if (returnDelims) { // and we're asked to return them
                            isDelimiter = true;
                            return String.valueOf(ch);
                        } else {
                            continue;
                        }
                    } else { // we have a token
                        --pos;
                        return token.toString();
                    }
                }
                token.append(ch);
            } else {
                char nextch = input.charAt(pos);
                token.append(nextch);
                ++pos;
            }
        }
        return token.toString();
    }

    public Collection<String> getAllTokens() {
        List<String> tokens = new ArrayList<>();
        while (hasMoreTokens())
            tokens.add(nextToken());
        return tokens;
    }

    /**
     * is the last token returned a delimiter?
     */
    public boolean isDelimiter() {
        return isDelimiter;
    }


    private boolean isDelimiter(char ch) {
        return delimiters.indexOf(ch) >= 0;
    }

}
