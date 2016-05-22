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

class FieldAndIndexPair {
    ASTIndexLink link;
    String fieldname;
    IndexMetadata md;

    FieldAndIndexPair(ASTIndexLink link, String fieldname, IndexMetadata md) {
        this.link = link;
        this.fieldname = fieldname;
        this.md = md;
    }

    @Override
    public String toString() {
        return fieldname + "; " + link;
    }
}
