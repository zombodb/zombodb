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
package org.apache.lucene.search.join;

/**
 * Exists in this package to provide a <b>public</b> version of {@link TermsCollector}
 * that ZomboDB can implement
 */
public abstract class ZomboDBTermsCollector extends TermsCollector {
    protected ZomboDBTermsCollector(String field) {
        super(field);
    }
}
