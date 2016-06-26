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

    @BeforeClass
    public static void setup() {
        irm.addRelationship("main", "id", "main_ft", "ft_id");
        irm.addRelationship("main", "id", "main_other", "other_id");
        irm.addRelationship("main", "id", "main_vol", "vol_id");
        irm.addRelationship("main_other", "custodian", "docs", "custodian");
        irm.addRelationship("docs", "case_profile", "case_profile", "case_profile");
    }

    @Test
    public void testPathFinding() {
        assertEquals(
                "[main_ft, main_ft:ft_id, main:id, main_other:other_id, main_other, main_other:custodian, docs:custodian, docs, docs:case_profile, case_profile:case_profile, case_profile]",
                irm.calcPath("main_ft", "case_profile").toString()
        );

        assertEquals(
                "[case_profile, case_profile:case_profile, docs:case_profile, docs, docs:custodian, main_other:custodian, main_other, main_other:other_id, main:id, main_ft:ft_id, main_ft]",
                irm.calcPath("case_profile", "main_ft").toString()
        );

        assertEquals(
                "[main, main:id, main_ft:ft_id, main_ft]",
                irm.calcPath("main", "main_ft").toString()
        );
    }

    @Test
    public void testEquivalencies() {
        assertTrue(irm.areFieldsEquivalent("main:id", "main_ft:ft_id"));
        assertTrue(irm.areFieldsEquivalent("main_ft:ft_id", "main:id"));
        assertTrue(irm.areFieldsEquivalent("docs:case_profile", "case_profile:case_profile"));
    }


}
