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

import com.tcdi.zombodb.query_parser.metadata.IndexRelationshipManager;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestIndexRelationshipManager {
    private static final IndexRelationshipManager irm = new IndexRelationshipManager();

    private static final ASTIndexLink main = ASTIndexLink.create("id", "main", null, "id");
    private static final ASTIndexLink main_ft = ASTIndexLink.create("ft_id", "main_ft", null, "ft_id");
    private static final ASTIndexLink main_other = ASTIndexLink.create("other_id", "main_other", null, "other_id");
    private static final ASTIndexLink main_vol = ASTIndexLink.create("vol_id", "main_vol", null, "vol_id");
    private static final ASTIndexLink docs = ASTIndexLink.create("docs_id", "docs", null, "docs_id");
    private static final ASTIndexLink case_profile = ASTIndexLink.create("cp_id", "case_profile", null, "cp_id");

    @BeforeClass
    public static void setup() {

        irm.addRelationship(main, "id", main_ft, "ft_id");
        irm.addRelationship(main, "id", main_other, "other_id");
        irm.addRelationship(main, "id", main_vol, "vol_id");
        irm.addRelationship(main_other, "custodian", docs, "custodian");
        irm.addRelationship(docs, "case_profile", case_profile, "case_profile");
    }

    @Test
    public void testPathFinding() {
        assertEquals(
                "[null@main_ft, null@main_ft:ft_id, null@main:id, null@main_other:other_id, null@main_other, null@main_other:custodian, null@docs:custodian, null@docs, null@docs:case_profile, null@case_profile:case_profile, null@case_profile]",
                irm.calcPath(main_ft, case_profile).toString()
        );

        assertEquals(
                "[null@case_profile, null@case_profile:case_profile, null@docs:case_profile, null@docs, null@docs:custodian, null@main_other:custodian, null@main_other, null@main_other:other_id, null@main:id, null@main_ft:ft_id, null@main_ft]",
                irm.calcPath(case_profile, main_ft).toString()
        );

        assertEquals(
                "[null@main, null@main:id, null@main_ft:ft_id, null@main_ft]",
                irm.calcPath(main, main_ft).toString()
        );
    }
}
