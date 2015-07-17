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
package com.tcdi.zombodb.query_parser;

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
    public static final String DEFAULT_DELIMITERS = " \t\r\n\f";

    private String _input;
    private char[] _delim;
    private boolean _returnDelims;
    private int _pos = 0;
    private char _lastDelimiter = 0;
    private boolean _isDelimiter = false;
    private StringBuffer _lastTokenEscaped = new StringBuffer();

    protected EscapingStringTokenizer() {
        // noop
    }

    public EscapingStringTokenizer(String str) {
        this(str, DEFAULT_DELIMITERS, false);
    }

    public EscapingStringTokenizer(String str, String delim) {
        this(str, delim, false);
    }

    public EscapingStringTokenizer(String str, String delim, boolean returnDelims) {
        _input = str;
        _delim = delim.toCharArray();
        _returnDelims = returnDelims;
    }

    public boolean hasMoreTokens() {
        int oldpos = _pos;
        char olddelim = _lastDelimiter;
        try {
            nextToken();
            return true;
        }
        catch (NoSuchElementException nsee) {
            return false;
        }
        finally {
            _pos = oldpos;
            _lastDelimiter = olddelim;
        }
    }

    public String getLastTokenEscaped() {
        return _lastTokenEscaped.toString();
    }

    public String nextToken() {
        if (_pos >= _input.length())
            throw new NoSuchElementException();

        StringBuilder sb = new StringBuilder();
        char prevch = 0;
        char ch;
        _lastTokenEscaped.setLength(0);
        while (_pos < _input.length()) {
            _lastDelimiter = ch = _input.charAt(_pos);
            if (isDelimiter(ch, prevch))
                break;
            if (isDelimiter(ch, (char) 0) && prevch == '\\')
                sb.setLength(sb.length() - 1);    // trim backslash

            sb.append(ch);
            _lastTokenEscaped.append(ch);
            prevch = ch;
            _pos++;
        }
        _isDelimiter = false;
        if (prevch == 0 && _returnDelims) {
            _isDelimiter = true;
            sb.append(_lastDelimiter);
            _lastTokenEscaped.append(_lastDelimiter);
            _pos++;
        } else if (sb.length() == 0 && !_returnDelims) {
            _pos++;
            return nextToken();
        }

        return sb.toString();
    }

    public String nextToken(String delim) {
        _delim = delim.toCharArray();
        return nextToken();
    }

    public int countTokens() {
        // save current state
        int oldpos = _pos;
        char olddelim = _lastDelimiter;

        int cnt = 0;
        while (hasMoreTokens()) {
            nextToken();
            cnt++;
        }

        // restore state
        _pos = oldpos;
        _lastDelimiter = olddelim;

        return cnt;
    }

    /**
     * is the last token returned a delimiter?
     */
    public boolean isDelimiter() {
        return _isDelimiter;
    }


    private boolean isDelimiter(char ch, char prevch) {
        for (char currentChar : _delim) {
            if (ch == currentChar && prevch != '\\') {
                return true;
            }
        }

        return false;
    }

}
