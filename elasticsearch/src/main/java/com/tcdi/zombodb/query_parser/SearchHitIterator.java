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

import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by e_ridge on 5/1/15.
 */
public class SearchHitIterator<T> implements Iterator<T>, Iterable<T> {

    private final String fieldname;
    private List<Iterator<SearchHit>> iterators = new ArrayList<>();
    private Iterator<Iterator<SearchHit>> _it;
    private Iterator<SearchHit> _currentIterator;
    private Iterator _currentValues;

    public SearchHitIterator(String fieldname) {
        this.fieldname = fieldname;
    }

    public void add(Iterator<SearchHit> it) {
        iterators.add(it);
    }

    public int size() {
        return iterators.size();
    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        if (_currentValues != null && _currentValues.hasNext())
            return true;
        else
            _currentValues = null;

        if (_it == null) _it = iterators.iterator();
        if (_currentIterator == null && _it.hasNext()) _currentIterator = _it.next();
        while (_currentIterator != null && !_currentIterator.hasNext() && _it.hasNext())
            _currentIterator = _it.next();

        if (_currentIterator != null && !_currentIterator.hasNext() && !_it.hasNext())
            _currentIterator = null;

        if (_currentIterator != null) {
            while (_currentValues == null && _currentIterator.hasNext()) {
                SearchHit hit = _currentIterator.next();
                _currentValues = hit.field(fieldname).iterator();
                if (!_currentValues.hasNext())
                    _currentValues = null;
            }
        }

        return _currentValues != null && _currentValues.hasNext(); // _currentIterator != null && _currentIterator.hasNext();
    }

    @Override
    public T next() {
        return (T) _currentValues.next();
    }

    @Override
    public void remove() {

    }
}
