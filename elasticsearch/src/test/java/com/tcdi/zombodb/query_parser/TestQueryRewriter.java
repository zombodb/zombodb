/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015 ZomboDB, LLC
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcdi.zombodb.highlight.AnalyzedField;
import com.tcdi.zombodb.highlight.DocumentHighlighter;
import com.tcdi.zombodb.test.ZomboDBTestCase;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link QueryRewriter}
 */
public class TestQueryRewriter extends ZomboDBTestCase {
    String query = "#options(id=<table.index>id, id=<table.index>id, id=<table.index>id, other:(left=<table.index>right)) #extended_stats(custodian) #tally(subject, '^.*', 1000, '_term', #significant_terms(author, '^.*', 1000))  " +
            "#field_lists(field1=[a,b,c], field2=[d,e,f], field3=[a,b,c,d,e,f]) " +
            "#child<data>(" +
            "fulltext=[beer] meeting not staff not cancelled not risk " +
            "#expand<left_field = <table.index>right_field>(the subquery) " +
            "#child<data>(some query) #parent<xact>(other query) #child<data>(())" +
            "long.dotted.field:foo " +
            "fuzzy~32 '1-2' " +
            "subject:['beer'] " +
            "prefix* *wildcard *wildcard2* *wild*card3* wild*card4 " +
            "'prefix* *wildcard *wildcard2* *wild*card3* wild*card4' " +
            "( ( (( ( custodian = \"QUERTY, V\" AND recordtype = \"EMAIL ATTACHMENT\" AND fileextension = PDF ) )) AND fileextension = pdf ) ) " +
            "001 123 0 000 0.23 " +
            "$$ doc['id'].value > 1024 $$ " +
            "field:~'^.*' " +
            "subject:[[ a, b, c, d ]] or title:[[1,2,3,4,5]] " +
            "'this is a sloppy phrase~'~11 \"so is this\"~42 " +
            "( ( ((review_data.owner_username=E_RIDGE AND review_data.status_name:[\"REVIEW_UPDATED\",\"REVIEW_CHECKED_OUT\"]) OR (review_data.status_name:REVIEW_READY)) AND review_data.project_id = 1040 ) ) " +
            "'this is an unordered sloppy phrase~'~!11 " +
            "field:null or fuzzy~1" +
            "[1,2,3,beer,'foo',\"blah\", true, 123.99, null] or " +
            "field<>http*\\/\\/www\\.\\*tcdi\\.com\\/ " +
            "field:value~ " +
            "review_data.assigned_reviewers:e_ridge " +
            "field:(a w/123 b w/2 c ^2.0 id=one /to/ two) and " +
            "foo /to/ three a\\-b\\-c\\-d a\\b\\c\\d a\\/b\\/c\\/d " +
            "http\\:\\/\\/www\\.tcdi\\.com\\/\\?id\\=42  blah:1 to 10 a to z " +
            "field1:(word1* word2*** w*ld?card* field2:wo**rd3 or " +
            "(review_data.subject:(first wine cheese food foo bar) and field:drink and review_data.subject:wine and review_data.subject:last and field:food) " +
            "(review_data.subject:(first, wine, cheese, food, foo, bar) or review_data.subject:wine or review_data.subject:last) " +
            "review_data.review_set_name:zipper or (review_data.review_set_name:food or beer) " +
            "(this or merges or arrays or [1,2,3] or [x,y,z, 'some phrase']) " +
            "field3:(beer^2 and wine) not *gr*avy*) " +
            "nested_field.fielda:beer with nested_field.fieldb:wine with (nested_field.fieldc:(food or cheese)) " +
            "  ((_xmin = 42  AND                      " + //  inserted by the current transaction
            "     _cmin < 42  AND                     " + //  before this command, and
            "     (_xmax = 0 OR                       " + //  the row has not been deleted, or
            "      (_xmax = 42  AND                   " + //  it was deleted by the current transaction
            "       _cmax >= 42)))                    " + //  but not before this command,
            "  OR                                     " + //                   or
            "    (_xmin_is_committed = true  AND      " + //  the row was inserted by a committed transaction, and
            "       (_xmax = 0 OR                     " + //  the row has not been deleted, or
            "        (_xmax = 42  AND                 " + //  the row is being deleted by this transaction
            "         _cmax >= 42) OR                 " + //  but it's not deleted \"yet\", or
            "        (_xmax <> 42  AND                " + //  the row was deleted by another transaction
            "         _xmax_is_committed = false))))  " + //  that has not been committed
            ")";

    @Test
    public void testComplexQueryJson() throws Exception {
        assertJson(query, resource(this.getClass(), "testComplexQueryJson.expected"));
    }

    @Test
    public void testComplexQueryAST() throws Exception {
        assertAST(query, resource(this.getClass(), "testComplexQueryAST.expected"));
    }

    @Test
    public void testSingleOption() throws Exception {
        assertAST("#options(left=<table.index>right)",
                "QueryTree\n" +
                        "   Options\n" +
                        "      left=<db.schema.table.index>right\n" +
                        "         LeftField (value=left)\n" +
                        "         IndexName (value=db.schema.table.index)\n" +
                        "         RightField (value=right)\n"
        );
    }

    @Test
    public void testMultipleOptions() throws Exception {
        assertAST("#options(left=<table.index>right, left2=<table2.index2>right2)",
                "QueryTree\n" +
                        "   Options\n" +
                        "      left=<db.schema.table.index>right\n" +
                        "         LeftField (value=left)\n" +
                        "         IndexName (value=db.schema.table.index)\n" +
                        "         RightField (value=right)\n" +
                        "      left2=<db.schema.table2.index2>right2\n" +
                        "         LeftField (value=left2)\n" +
                        "         IndexName (value=db.schema.table2.index2)\n" +
                        "         RightField (value=right2)\n"
        );
    }

    @Test
    public void testSingleNamedOption() throws Exception {
        assertAST("#options(f_name:(left=<table.index>right))",
                "QueryTree\n" +
                        "   Options\n" +
                        "      f_name:(left=<db.schema.table.index>right)\n" +
                        "         LeftField (value=left)\n" +
                        "         IndexName (value=db.schema.table.index)\n" +
                        "         RightField (value=right)\n"
        );
    }

    @Test
    public void testMultipleNamedOptions() throws Exception {
        assertAST("#options(f_name:(left=<table.index>right), f_name2:(left2=<table2.index2>right2))",
                "QueryTree\n" +
                        "   Options\n" +
                        "      f_name:(left=<db.schema.table.index>right)\n" +
                        "         LeftField (value=left)\n" +
                        "         IndexName (value=db.schema.table.index)\n" +
                        "         RightField (value=right)\n" +
                        "      f_name2:(left2=<db.schema.table2.index2>right2)\n" +
                        "         LeftField (value=left2)\n" +
                        "         IndexName (value=db.schema.table2.index2)\n" +
                        "         RightField (value=right2)\n"
        );
    }

    @Test
    public void testMultipleMixedOptions() throws Exception {
        assertAST("#options(left=<table.index>right, f_name2:(left2=<table2.index2>right2))",
                "QueryTree\n" +
                        "   Options\n" +
                        "      left=<db.schema.table.index>right\n" +
                        "         LeftField (value=left)\n" +
                        "         IndexName (value=db.schema.table.index)\n" +
                        "         RightField (value=right)\n" +
                        "      f_name2:(left2=<db.schema.table2.index2>right2)\n" +
                        "         LeftField (value=left2)\n" +
                        "         IndexName (value=db.schema.table2.index2)\n" +
                        "         RightField (value=right2)\n"
        );
    }

    @Test
    public void test_allFieldExpansion() throws Exception {
        assertAST("beer or wine or cheese and fulltext:bob",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Or\n" +
                        "         And\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=cheese, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=cheese, index=db.schema.table.index)\n" +
                        "            Word (fieldname=fulltext, operator=CONTAINS, value=bob, index=db.schema.table.index)\n" +
                        "         Array (fieldname=fulltext_field, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=beer, index=db.schema.table.index)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=wine, index=db.schema.table.index)\n" +
                        "         Array (fieldname=_all, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=beer, index=db.schema.table.index)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=wine, index=db.schema.table.index)"
        );
    }

    @Test
    public void testASTExpansionInjection() throws Exception {
        assertAST("#options(id=<main_ft.idxmain_ft>ft_id, id=<main_vol.idxmain_vol>vol_id, id=<main_other.idxmain_other>other_id) (((_xmin = 6250261 AND _cmin < 0 AND (_xmax = 0 OR (_xmax = 6250261 AND _cmax >= 0))) OR (_xmin_is_committed = true AND (_xmax = 0 OR (_xmax = 6250261 AND _cmax >= 0) OR (_xmax <> 6250261 AND _xmax_is_committed = false))))) AND (#child<data>((phrase_field:(beer w/500 a))))",
                "QueryTree\n" +
                        "   Options\n" +
                        "      id=<db.schema.main_ft.idxmain_ft>ft_id\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=db.schema.main_ft.idxmain_ft)\n" +
                        "         RightField (value=ft_id)\n" +
                        "      id=<db.schema.main_vol.idxmain_vol>vol_id\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=db.schema.main_vol.idxmain_vol)\n" +
                        "         RightField (value=vol_id)\n" +
                        "      id=<db.schema.main_other.idxmain_other>other_id\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=db.schema.main_other.idxmain_other)\n" +
                        "         RightField (value=other_id)\n" +
                        "   And\n" +
                        "      Or\n" +
                        "         And\n" +
                        "            Number (fieldname=_xmin, operator=EQ, value=6250261)\n" +
                        "            Number (fieldname=_cmin, operator=LT, value=0)\n" +
                        "            Or\n" +
                        "               Number (fieldname=_xmax, operator=EQ, value=0)\n" +
                        "               And\n" +
                        "                  Number (fieldname=_xmax, operator=EQ, value=6250261)\n" +
                        "                  Number (fieldname=_cmax, operator=GTE, value=0)\n" +
                        "         And\n" +
                        "            Boolean (fieldname=_xmin_is_committed, operator=EQ, value=true)\n" +
                        "            Or\n" +
                        "               Number (fieldname=_xmax, operator=EQ, value=0)\n" +
                        "               And\n" +
                        "                  Number (fieldname=_xmax, operator=EQ, value=6250261)\n" +
                        "                  Number (fieldname=_cmax, operator=GTE, value=0)\n" +
                        "               And\n" +
                        "                  Number (fieldname=_xmax, operator=NE, value=6250261)\n" +
                        "                  Boolean (fieldname=_xmax_is_committed, operator=EQ, value=false)\n" +
                        "      Child (type=data)\n" +
                        "         Expansion\n" +
                        "            id=<db.schema.table.index>id\n" +
                        "            Proximity (fieldname=phrase_field, operator=CONTAINS, distance=500, ordered=false, index=db.schema.table.index)\n" +
                        "               Word (fieldname=phrase_field, operator=CONTAINS, value=beer, index=db.schema.table.index)\n" +
                        "               Word (fieldname=phrase_field, operator=CONTAINS, value=a, index=db.schema.table.index)"
        );
    }

    @Test
    public void testASTExpansionInjection2() throws Exception {
        assertAST("#options(id=<so_users.idxso_users>ft_id, id=<so_users.idxso_users>vol_id, id=<so_users.idxso_users>other_id) (((_xmin = 6250507 AND _cmin < 0 AND (_xmax = 0 OR (_xmax = 6250507 AND _cmax >= 0))) OR (_xmin_is_committed = true AND (_xmax = 0 OR (_xmax = 6250507 AND _cmax >= 0) OR (_xmax <> 6250507 AND _xmax_is_committed = false))))) AND (#child<data>((( #expand<data_cv_group_id=<this.index>data_cv_group_id> ( ( (( ( data_client_name = ANTHEM AND data_duplicate_resource = NO ) )) AND " +
                        "( (data_custodian = \"Querty, AMY\" OR data_custodian = \"QWERTY, COLIN\" OR data_custodian = \"QWERTY, KEITH\" OR data_custodian = \"QWERTY, PERRY\" OR data_custodian = \"QWERTY, NORM\" OR data_custodian = \"QWERTY, MIKE\" OR " +
                        "data_custodian = \"QWERTY,MIKE\" OR data_custodian = \"QWERTY, DAN\" OR data_custodian = \"QWERTY,DAN\") AND data_filter_06b = \"QWERTY*\" AND NOT data_moved_to = \"*\" ) ) ) ))))",
                "QueryTree\n" +
                        "   Options\n" +
                        "      id=<db.schema.so_users.idxso_users>ft_id\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=db.schema.so_users.idxso_users)\n" +
                        "         RightField (value=ft_id)\n" +
                        "      id=<db.schema.so_users.idxso_users>vol_id\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=db.schema.so_users.idxso_users)\n" +
                        "         RightField (value=vol_id)\n" +
                        "      id=<db.schema.so_users.idxso_users>other_id\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=db.schema.so_users.idxso_users)\n" +
                        "         RightField (value=other_id)\n" +
                        "   And\n" +
                        "      Or\n" +
                        "         And\n" +
                        "            Number (fieldname=_xmin, operator=EQ, value=6250507)\n" +
                        "            Number (fieldname=_cmin, operator=LT, value=0)\n" +
                        "            Or\n" +
                        "               Number (fieldname=_xmax, operator=EQ, value=0)\n" +
                        "               And\n" +
                        "                  Number (fieldname=_xmax, operator=EQ, value=6250507)\n" +
                        "                  Number (fieldname=_cmax, operator=GTE, value=0)\n" +
                        "         And\n" +
                        "            Boolean (fieldname=_xmin_is_committed, operator=EQ, value=true)\n" +
                        "            Or\n" +
                        "               Number (fieldname=_xmax, operator=EQ, value=0)\n" +
                        "               And\n" +
                        "                  Number (fieldname=_xmax, operator=EQ, value=6250507)\n" +
                        "                  Number (fieldname=_cmax, operator=GTE, value=0)\n" +
                        "               And\n" +
                        "                  Number (fieldname=_xmax, operator=NE, value=6250507)\n" +
                        "                  Boolean (fieldname=_xmax_is_committed, operator=EQ, value=false)\n" +
                        "      Child (type=data)\n" +
                        "         Or\n" +
                        "            Expansion\n" +
                        "               data_cv_group_id=<db.schema.table.index>data_cv_group_id\n" +
                        "               Expansion\n" +
                        "                  id=<db.schema.table.index>id\n" +
                        "                  And\n" +
                        "                     Word (fieldname=data_client_name, operator=EQ, value=anthem, index=db.schema.table.index)\n" +
                        "                     Word (fieldname=data_duplicate_resource, operator=EQ, value=no, index=db.schema.table.index)\n" +
                        "                     Or\n" +
                        "                        Word (fieldname=data_custodian, operator=EQ, value=querty, amy, index=db.schema.table.index)\n" +
                        "                        Word (fieldname=data_custodian, operator=EQ, value=qwerty, colin, index=db.schema.table.index)\n" +
                        "                        Word (fieldname=data_custodian, operator=EQ, value=qwerty, keith, index=db.schema.table.index)\n" +
                        "                        Word (fieldname=data_custodian, operator=EQ, value=qwerty, perry, index=db.schema.table.index)\n" +
                        "                        Word (fieldname=data_custodian, operator=EQ, value=qwerty, norm, index=db.schema.table.index)\n" +
                        "                        Word (fieldname=data_custodian, operator=EQ, value=qwerty, mike, index=db.schema.table.index)\n" +
                        "                        Word (fieldname=data_custodian, operator=EQ, value=qwerty,mike, index=db.schema.table.index)\n" +
                        "                        Word (fieldname=data_custodian, operator=EQ, value=qwerty, dan, index=db.schema.table.index)\n" +
                        "                        Word (fieldname=data_custodian, operator=EQ, value=qwerty,dan, index=db.schema.table.index)\n" +
                        "                     Prefix (fieldname=data_filter_06b, operator=EQ, value=qwerty, index=db.schema.table.index)\n" +
                        "                     Not\n" +
                        "                        NotNull (fieldname=data_moved_to, operator=EQ, index=db.schema.table.index)\n" +
                        "            Expansion\n" +
                        "               id=<db.schema.table.index>id\n" +
                        "               And\n" +
                        "                  Word (fieldname=data_client_name, operator=EQ, value=anthem, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_duplicate_resource, operator=EQ, value=no, index=db.schema.table.index)\n" +
                        "                  Or\n" +
                        "                     Word (fieldname=data_custodian, operator=EQ, value=querty, amy, index=db.schema.table.index)\n" +
                        "                     Word (fieldname=data_custodian, operator=EQ, value=qwerty, colin, index=db.schema.table.index)\n" +
                        "                     Word (fieldname=data_custodian, operator=EQ, value=qwerty, keith, index=db.schema.table.index)\n" +
                        "                     Word (fieldname=data_custodian, operator=EQ, value=qwerty, perry, index=db.schema.table.index)\n" +
                        "                     Word (fieldname=data_custodian, operator=EQ, value=qwerty, norm, index=db.schema.table.index)\n" +
                        "                     Word (fieldname=data_custodian, operator=EQ, value=qwerty, mike, index=db.schema.table.index)\n" +
                        "                     Word (fieldname=data_custodian, operator=EQ, value=qwerty,mike, index=db.schema.table.index)\n" +
                        "                     Word (fieldname=data_custodian, operator=EQ, value=qwerty, dan, index=db.schema.table.index)\n" +
                        "                     Word (fieldname=data_custodian, operator=EQ, value=qwerty,dan, index=db.schema.table.index)\n" +
                        "                  Prefix (fieldname=data_filter_06b, operator=EQ, value=qwerty, index=db.schema.table.index)\n" +
                        "                  Not\n" +
                        "                     NotNull (fieldname=data_moved_to, operator=EQ, index=db.schema.table.index)"
        );
    }

    @Test
    public void testSimplePhrase() throws Exception {
        assertJson("phrase_field:(\"this is a phrase\")",
                "{\n" +
                        "  \"match\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"query\" : \"this is a phrase\",\n" +
                        "      \"type\" : \"phrase\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testPhraseWithEscapedWildcards() throws Exception {
        assertJson("_all:'\\* this phrase has \\?escaped\\~ wildcards\\*'",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"should\" : [ {\n" +
                        "      \"match\" : {\n" +
                        "        \"fulltext_field\" : {\n" +
                        "          \"query\" : \"* this phrase has ?escaped~ wildcards*\",\n" +
                        "          \"type\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"match\" : {\n" +
                        "        \"_all\" : {\n" +
                        "          \"query\" : \"* this phrase has ?escaped~ wildcards*\",\n" +
                        "          \"type\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}");
    }

    @Test
    public void testPhraseWithFuzzyTerms() throws Exception {
        assertJson("phrase_field:'Here~ is~ fuzzy~ words'",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"fuzzy\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"here\",\n" +
                        "              \"prefix_length\" : 3\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"fuzzy\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"is\",\n" +
                        "              \"prefix_length\" : 3\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"fuzzy\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"fuzzy\",\n" +
                        "              \"prefix_length\" : 3\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"words\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}");
    }

    @Test
    public void testPhraseWithEscapedFuzzyCharacters() throws Exception {
        assertJson("phrase_field:'Here\\~ is\\~ fuzzy\\~ words'",
                "{\n" +
                        "  \"match\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"query\" : \"Here~ is~ fuzzy~ words\",\n" +
                        "      \"type\" : \"phrase\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}");
    }

    @Test
    public void testPhraseWithMetaCharacters() throws Exception {
        assertJson("phrase_field:'this* should\\* sub:parse :\\- into a sp\\?n~'",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"prefix\" : {\n" +
                        "            \"phrase_field\" : \"this\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"should\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"sub:parse\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"into\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"a\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"sp\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"fuzzy\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"n\",\n" +
                        "              \"prefix_length\" : 3\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}");
    }

    @Test
    public void testPhraseWithSlop() throws Exception {
        assertJson("phrase_field:'some phrase containing slop'~2",
                "{\n" +
                        "  \"match\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"query\" : \"some phrase containing slop\",\n" +
                        "      \"type\" : \"phrase\",\n" +
                        "      \"slop\" : 2\n" +
                        "    }\n" +
                        "  }\n" +
                        "}");
    }

    @Test
    public void testPhraseWithWildcards() throws Exception {
        assertJson("phrase_field:'phrase containing wild*card'",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"containing\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"wildcard\" : {\n" +
                        "            \"phrase_field\" : \"wild*card\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}");
    }

    @Test
    public void testAndedWildcardPhrases() throws Exception {
        assertJson("phrase_field:'phrase containing wild*card AND w*ns'",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"containing\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"wildcard\" : {\n" +
                        "            \"phrase_field\" : \"wild*card\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"and\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"wildcard\" : {\n" +
                        "            \"phrase_field\" : \"w*ns\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}");
    }

    @Test
    public void testOredWildcardPhrases() throws Exception {
        assertJson("phrase_field:'phrase containing wild*card OR w*ns'",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"containing\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"wildcard\" : {\n" +
                        "            \"phrase_field\" : \"wild*card\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"or\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"wildcard\" : {\n" +
                        "            \"phrase_field\" : \"w*ns\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}");
    }

    @Test
    public void testCVSIX939_boolean_simpilification() throws Exception {
        assertJson("id:1 and id<>1",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"term\" : {\n" +
                        "        \"id\" : 1\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"filtered\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"match_all\" : { }\n" +
                        "        },\n" +
                        "        \"filter\" : {\n" +
                        "          \"not\" : {\n" +
                        "            \"filter\" : {\n" +
                        "              \"query\" : {\n" +
                        "                \"term\" : {\n" +
                        "                  \"id\" : 1\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}");
    }

    @Test
    public void testTermAndPhraseEqual() throws Exception {
        assertEquals(toJson("phrase_field:'\\*test\\~'"), toJson("phrase_field:\\*test\\~"));
    }

    @Test
    public void testSimpleNestedGroup() throws Exception {
        assertJson("nested_group.field_a:value and nested_group.field_b:value",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"nested_group.field_a\" : \"value\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"nested_group\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"nested_group.field_b\" : \"value\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"nested_group\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}");
    }

    @Test
    public void testSimpleNestedGroup2() throws Exception {
        assertJson("nested_group.field_a:value ",
                "{\n" +
                        "  \"nested\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"term\" : {\n" +
                        "        \"nested_group.field_a\" : \"value\"\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"path\" : \"nested_group\"\n" +
                        "  }\n" +
                        "}");
    }

    @Test
    public void testExternalNestedGroup() throws Exception {
        assertJson(
                "#options(witness_data:(id=<table.index>id)) witness_data.wit_first_name = 'mark' and witness_data.wit_last_name = 'matte'",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"witness_data.wit_first_name\" : \"mark\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"witness_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"witness_data.wit_last_name\" : \"matte\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"witness_data\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2551() throws Exception {
        assertJson(
                "phrase_field:'c-note'",
                "{\n" +
                        "  \"match\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"query\" : \"c-note\",\n" +
                        "      \"type\" : \"phrase\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2551_PrefixWildcard() throws Exception {
        assertJson(
                "phrase_field:'c-note*'",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"c\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"prefix\" : {\n" +
                        "            \"phrase_field\" : \"note\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2551_EmbeddedWildcard() throws Exception {
        assertJson(
                "phrase_field:'c-note*s'",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"c\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"wildcard\" : {\n" +
                        "            \"phrase_field\" : \"note*s\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2551_Fuzzy() throws Exception {
        assertJson(
                "phrase_field:'c-note'~2",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"c\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"fuzzy\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"note\",\n" +
                        "              \"prefix_length\" : 2\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testReverseOfCVSIX_2551() throws Exception {
        assertJson(
                "exact_field:'c-note'",
                "{\n" +
                        "  \"term\" : {\n" +
                        "    \"exact_field\" : \"c-note\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2551_WithProximity() throws Exception {
        assertJson("phrase_field:('c-note' w/3 beer)",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_near\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"c\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"note\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ],\n" +
                        "        \"slop\" : 0,\n" +
                        "        \"in_order\" : true\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"beer\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 3,\n" +
                        "    \"in_order\" : false\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2551_WithFuzzyProximity() throws Exception {
        assertJson("phrase_field:('c-note'~10 w/3 beer)",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_near\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"c\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_multi\" : {\n" +
                        "            \"match\" : {\n" +
                        "              \"fuzzy\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"note\",\n" +
                        "                  \"prefix_length\" : 10\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ],\n" +
                        "        \"slop\" : 0,\n" +
                        "        \"in_order\" : true\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"beer\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 3,\n" +
                        "    \"in_order\" : false\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2551_subjectsStarsSymbol002() throws Exception {
        assertJson("( phrase_field : \"Qwerty \\*FREE Samples\\*\" )",
                "{\n" +
                        "  \"match\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"query\" : \"Qwerty *FREE Samples*\",\n" +
                        "      \"type\" : \"phrase\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2577() throws Exception {
        assertJson("( phrase_field: \"cut-over\" OR phrase_field: \"get-prices\" )",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"should\" : [ {\n" +
                        "      \"match\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"query\" : \"cut-over\",\n" +
                        "          \"type\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"match\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"query\" : \"get-prices\",\n" +
                        "          \"type\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testTermRollups() throws Exception {
        assertJson("id: 100 OR id: 200",
                "{\n" +
                        "  \"terms\" : {\n" +
                        "    \"id\" : [ 100, 200 ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2521() throws Exception {
        assertJson("id < 100 OR id < 100",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"should\" : [ {\n" +
                        "      \"range\" : {\n" +
                        "        \"id\" : {\n" +
                        "          \"from\" : null,\n" +
                        "          \"to\" : 100,\n" +
                        "          \"include_lower\" : true,\n" +
                        "          \"include_upper\" : false\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"range\" : {\n" +
                        "        \"id\" : {\n" +
                        "          \"from\" : null,\n" +
                        "          \"to\" : 100,\n" +
                        "          \"include_lower\" : true,\n" +
                        "          \"include_upper\" : false\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2578_AnyBareWildcard() throws Exception {
        assertJson("phrase_field:'V - A - C - A - T - I - O * N'",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"v\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"a\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"c\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"a\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"t\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"i\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"o\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"wildcard\" : {\n" +
                        "            \"phrase_field\" : \"*\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"n\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}");
    }

    @Test
    public void testCVSIX_2578_SingleBareWildcard() throws Exception {
        assertJson("phrase_field:'V - A - C - A -?'",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"v\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"a\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"c\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"a\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"wildcard\" : {\n" +
                        "            \"phrase_field\" : \"?\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2578_AnyWildcard() throws Exception {
        assertJson("phrase_field:'V - A - C - A - T - I - O* N'",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"v\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"a\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"c\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"a\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"t\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"i\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"prefix\" : {\n" +
                        "            \"phrase_field\" : \"o\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"n\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2578_SingleWildcard() throws Exception {
        assertJson("phrase_field:'V - A - C - A - T - I?'",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"v\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"a\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"c\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"a\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"t\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"wildcard\" : {\n" +
                        "            \"phrase_field\" : \"i?\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_subjectsFuzzyPhrase_001() throws Exception {
        assertJson("( phrase_field : \"choose prize your~2\"~!0 )",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"choose\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"prize\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"fuzzy\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"your\",\n" +
                        "              \"prefix_length\" : 2\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : false\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2579() throws Exception {
        assertJson("phrase_field:('4-13-01*' w/15 Weekend w/15 outage)",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_near\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"4\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"13\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_multi\" : {\n" +
                        "            \"match\" : {\n" +
                        "              \"prefix\" : {\n" +
                        "                \"phrase_field\" : \"01\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ],\n" +
                        "        \"slop\" : 0,\n" +
                        "        \"in_order\" : true\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_near\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"weekend\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"outage\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ],\n" +
                        "        \"slop\" : 15,\n" +
                        "        \"in_order\" : false\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 15,\n" +
                        "    \"in_order\" : false\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2591() throws Exception {
        assertJson("exact_field:\"2001-*\"",
                "{\n" +
                        "  \"prefix\" : {\n" +
                        "    \"exact_field\" : \"2001-\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2835_prefix() throws Exception {
        assertJson("(exact_field = \"bob dol*\")",
                "{\n" +
                        "  \"prefix\" : {\n" +
                        "    \"exact_field\" : \"bob dol\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2835_wildcard() throws Exception {
        assertJson("(exact_field = \"bob* dol*\")",
                "{\n" +
                        "  \"wildcard\" : {\n" +
                        "    \"exact_field\" : \"bob* dol*\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2835_fuzzy() throws Exception {
        assertJson("(exact_field = \"bob dol~\")",
                "{\n" +
                        "  \"fuzzy\" : {\n" +
                        "    \"exact_field\" : {\n" +
                        "      \"value\" : \"bob dol\",\n" +
                        "      \"prefix_length\" : 3\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2807() throws Exception {
        assertJson("(exact_field <> \"bob*\")",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"not\" : {\n" +
                        "        \"filter\" : {\n" +
                        "          \"query\" : {\n" +
                        "            \"prefix\" : {\n" +
                        "              \"exact_field\" : \"bob\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2807_phrase() throws Exception {
        assertJson("(phrase_field <> \"bob*\")",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"not\" : {\n" +
                        "        \"filter\" : {\n" +
                        "          \"query\" : {\n" +
                        "            \"prefix\" : {\n" +
                        "              \"phrase_field\" : \"bob\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testMoreLikeThis() throws Exception {
        assertJson("phrase_field:@'this is a test'",
                "{\n" +
                        "  \"mlt\" : {\n" +
                        "    \"fields\" : [ \"phrase_field\" ],\n" +
                        "    \"like_text\" : \"this is a test\",\n" +
                        "    \"min_term_freq\" : 1,\n" +
                        "    \"max_query_terms\" : 80,\n" +
                        "    \"stop_words\" : [ \"http\", \"span\", \"class\", \"flashtext\", \"let\", \"its\", \"may\", \"well\", \"got\", \"too\", \"them\", \"really\", \"new\", \"set\", \"please\", \"how\", \"our\", \"from\", \"sent\", \"subject\", \"sincerely\", \"thank\", \"thanks\", \"just\", \"get\", \"going\", \"were\", \"much\", \"can\", \"also\", \"she\", \"her\", \"him\", \"his\", \"has\", \"been\", \"ok\", \"still\", \"okay\", \"does\", \"did\", \"about\", \"yes\", \"you\", \"your\", \"when\", \"know\", \"have\", \"who\", \"what\", \"where\", \"sir\", \"page\", \"a\", \"an\", \"and\", \"are\", \"as\", \"at\", \"be\", \"but\", \"by\", \"for\", \"if\", \"in\", \"into\", \"is\", \"it\", \"no\", \"not\", \"of\", \"on\", \"or\", \"such\", \"that\", \"the\", \"their\", \"than\", \"then\", \"there\", \"these\", \"they\", \"this\", \"to\", \"was\", \"will\", \"with\" ],\n" +
                        "    \"min_word_length\" : 3\n" +
                        "  }\n" +
                        "}"
                );
    }

    @Test
    public void testFuzzyLikeThis() throws Exception {
        assertJson("phrase_field:@~'this is a test'",
                "{\n" +
                        "  \"flt_field\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"like_text\" : \"this is a test\",\n" +
                        "      \"max_query_terms\" : 80,\n" +
                        "      \"fuzziness\" : \"AUTO\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testScript() throws Exception {
        assertJson("$$ this.is.a.script[12] = 42; $$",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"script\" : {\n" +
                        "        \"script\" : \" this.is.a.script[12] = 42; \"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2682() throws Exception {
        assertJson("( phrase_field:(more w/10 \"food\\*\") )",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"more\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"food\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 10,\n" +
                        "    \"in_order\" : false\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2579_WithQuotes() throws Exception {
        assertJson("phrase_field:(\"4-13-01*\" w/15 Weekend w/15 outage)",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_near\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"4\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"13\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_multi\" : {\n" +
                        "            \"match\" : {\n" +
                        "              \"prefix\" : {\n" +
                        "                \"phrase_field\" : \"01\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ],\n" +
                        "        \"slop\" : 0,\n" +
                        "        \"in_order\" : true\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_near\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"weekend\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"outage\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ],\n" +
                        "        \"slop\" : 15,\n" +
                        "        \"in_order\" : false\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 15,\n" +
                        "    \"in_order\" : false\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_MergeLiterals_AND() throws Exception {
        assertJson("exact_field:(one & two & three)",
                "{\n" +
                        "  \"terms\" : {\n" +
                        "    \"exact_field\" : [ \"one\", \"two\", \"three\" ],\n" +
                        "    \"minimum_should_match\" : \"3\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_MergeLiterals_AND_NE() throws Exception {
        assertJson("exact_field<>(one & two & three)",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"not\" : {\n" +
                        "        \"filter\" : {\n" +
                        "          \"query\" : {\n" +
                        "            \"terms\" : {\n" +
                        "              \"exact_field\" : [ \"one\", \"two\", \"three\" ]\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_MergeLiterals_OR_NE() throws Exception {
        assertJson("exact_field<>(one , two , three)",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"not\" : {\n" +
                        "        \"filter\" : {\n" +
                        "          \"query\" : {\n" +
                        "            \"terms\" : {\n" +
                        "              \"exact_field\" : [ \"one\", \"two\", \"three\" ],\n" +
                        "              \"minimum_should_match\" : \"3\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_MergeLiterals_OR() throws Exception {
        assertJson("exact_field:(one , two , three)",
                "{\n" +
                        "  \"terms\" : {\n" +
                        "    \"exact_field\" : [ \"one\", \"two\", \"three\" ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_MergeLiterals_AND_WithArrays() throws Exception {
        assertJson("exact_field:(one & two & three & [four,five,six])",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"terms\" : {\n" +
                        "        \"exact_field\" : [ \"one\", \"two\", \"three\" ],\n" +
                        "        \"minimum_should_match\" : \"3\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"terms\" : {\n" +
                        "        \"exact_field\" : [ \"four\", \"five\", \"six\" ]\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_MergeLiterals_OR_WithArrays() throws Exception {
        assertJson("exact_field:(one , two , three , [four,five,six])",
                "{\n" +
                        "  \"terms\" : {\n" +
                        "    \"exact_field\" : [ \"one\", \"two\", \"three\", \"four\", \"five\", \"six\" ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_MergeLiterals_AND_OR_WithArrays() throws Exception {
        assertJson("exact_field:(one , two , three , [four,five,six] & one & two & three & [four, five, six])",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"should\" : [ {\n" +
                        "      \"terms\" : {\n" +
                        "        \"exact_field\" : [ \"one\", \"two\", \"three\" ]\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"terms\" : {\n" +
                        "            \"exact_field\" : [ \"four\", \"five\", \"six\", \"four\", \"five\", \"six\" ]\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"terms\" : {\n" +
                        "            \"exact_field\" : [ \"one\", \"two\", \"three\" ],\n" +
                        "            \"minimum_should_match\" : \"3\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2941_DoesntWork() throws Exception {
        assertJson("( ( ( review_data_ben.coding.responsiveness = Responsive OR review_data_ben.coding.responsiveness = \"Potentially Responsive\" OR review_data_ben.coding.responsiveness = \"Not Responsive\" OR review_data_ben.coding.responsiveness = Unreviewable ) AND review_data_ben.review_data_id = 67115 ) )",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"bool\" : {\n" +
                        "        \"should\" : [ {\n" +
                        "          \"nested\" : {\n" +
                        "            \"query\" : {\n" +
                        "              \"terms\" : {\n" +
                        "                \"review_data_ben.coding.responsiveness\" : [ \"responsive\", \"unreviewable\" ]\n" +
                        "              }\n" +
                        "            },\n" +
                        "            \"path\" : \"review_data_ben\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"nested\" : {\n" +
                        "            \"query\" : {\n" +
                        "              \"term\" : {\n" +
                        "                \"review_data_ben.coding.responsiveness\" : \"potentially responsive\"\n" +
                        "              }\n" +
                        "            },\n" +
                        "            \"path\" : \"review_data_ben\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"nested\" : {\n" +
                        "            \"query\" : {\n" +
                        "              \"term\" : {\n" +
                        "                \"review_data_ben.coding.responsiveness\" : \"not responsive\"\n" +
                        "              }\n" +
                        "            },\n" +
                        "            \"path\" : \"review_data_ben\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data_ben.review_data_id\" : 67115\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data_ben\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2941_DoesWork() throws Exception {
        assertJson("( (review_data_ben.review_data_id = 67115 WITH ( review_data_ben.coding.responsiveness = Responsive OR review_data_ben.coding.responsiveness = \"Potentially Responsive\" OR review_data_ben.coding.responsiveness = \"Not Responsive\" OR review_data_ben.coding.responsiveness = Unreviewable  ) ) )",
                "{\n" +
                        "  \"nested\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data_ben.review_data_id\" : 67115\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"bool\" : {\n" +
                        "            \"should\" : [ {\n" +
                        "              \"terms\" : {\n" +
                        "                \"review_data_ben.coding.responsiveness\" : [ \"responsive\", \"unreviewable\" ]\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"review_data_ben.coding.responsiveness\" : \"potentially responsive\"\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"review_data_ben.coding.responsiveness\" : \"not responsive\"\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"path\" : \"review_data_ben\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2941_NestedGroupRollupWithNonNestedFields_AND() throws Exception {
        assertJson("(review_data.subject:(first wine cheese food foo bar) and field:drink and review_data.subject:wine and review_data.subject:last and field:food) ",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"terms\" : {\n" +
                        "        \"field\" : [ \"drink\", \"food\" ],\n" +
                        "        \"minimum_should_match\" : \"2\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"wine\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"last\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"first\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"wine\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"cheese\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"food\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"foo\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"bar\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2941_NestedGroupRollupWithNonNestedFields_OR() throws Exception {
        assertJson("(review_data.subject:(first, wine, cheese, food, foo, bar) or field:food and review_data.subject:wine or review_data.subject:last) or field:drink ",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"should\" : [ {\n" +
                        "      \"term\" : {\n" +
                        "        \"field\" : \"drink\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"term\" : {\n" +
                        "            \"field\" : \"food\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"nested\" : {\n" +
                        "            \"query\" : {\n" +
                        "              \"term\" : {\n" +
                        "                \"review_data.subject\" : \"wine\"\n" +
                        "              }\n" +
                        "            },\n" +
                        "            \"path\" : \"review_data\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"terms\" : {\n" +
                        "            \"review_data.subject\" : [ \"last\", \"first\", \"wine\", \"cheese\", \"food\", \"foo\", \"bar\" ]\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2941_NestedGroupRollupWithNonNestedFields_BOTH() throws Exception {
        assertJson("(review_data.subject:(first wine cheese food foo bar) and review_data.subject:wine and review_data.subject:last and field:food) (review_data.subject:(first, wine, cheese, food, foo, bar) or review_data.subject:wine or review_data.subject:last)",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"terms\" : {\n" +
                        "            \"review_data.subject\" : [ \"wine\", \"last\", \"first\", \"wine\", \"cheese\", \"food\", \"foo\", \"bar\" ]\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"wine\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"last\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"field\" : \"food\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"first\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"wine\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"cheese\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"food\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"foo\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.subject\" : \"bar\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2941_NestedGroupRollup() throws Exception {
        assertJson("beer and ( ( ((review_data.owner_username=E_RIDGE AND review_data.status_name:[\"REVIEW_UPDATED\",\"REVIEW_CHECKED_OUT\"]) OR (review_data.status_name:REVIEW_READY)) AND review_data.project_id = 1040 ) ) ",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"bool\" : {\n" +
                        "        \"should\" : [ {\n" +
                        "          \"term\" : {\n" +
                        "            \"fulltext_field\" : \"beer\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"term\" : {\n" +
                        "            \"_all\" : \"beer\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"bool\" : {\n" +
                        "        \"should\" : [ {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"nested\" : {\n" +
                        "                \"query\" : {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"review_data.owner_username\" : \"e_ridge\"\n" +
                        "                  }\n" +
                        "                },\n" +
                        "                \"path\" : \"review_data\"\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"nested\" : {\n" +
                        "                \"query\" : {\n" +
                        "                  \"terms\" : {\n" +
                        "                    \"review_data.status_name\" : [ \"review_updated\", \"review_checked_out\" ]\n" +
                        "                  }\n" +
                        "                },\n" +
                        "                \"path\" : \"review_data\"\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"nested\" : {\n" +
                        "            \"query\" : {\n" +
                        "              \"term\" : {\n" +
                        "                \"review_data.status_name\" : \"review_ready\"\n" +
                        "              }\n" +
                        "            },\n" +
                        "            \"path\" : \"review_data\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data.project_id\" : 1040\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2941_NestedGroupRollupInChild() throws Exception {
        assertJson("#child<data>(( ( ((review_data.owner_username=E_RIDGE WITH review_data.status_name:[\"REVIEW_UPDATED\",\"REVIEW_CHECKED_OUT\"]) OR (review_data.status_name:REVIEW_READY)) WITH review_data.project_id = 1040 ) ) )",
                "{\n" +
                        "  \"has_child\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"bool\" : {\n" +
                        "                \"should\" : [ {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : [ {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"review_data.owner_username\" : \"e_ridge\"\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"terms\" : {\n" +
                        "                        \"review_data.status_name\" : [ \"review_updated\", \"review_checked_out\" ]\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"review_data.status_name\" : \"review_ready\"\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"review_data.project_id\" : 1040\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data\"\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"child_type\" : \"data\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2941_Aggregate() throws Exception {
        assertJson("#tally(review_data_ridge.coding.responsiveness, \"^.*\", 5000, \"term\") #options(id=<table.index>ft_id, id=<table.index>vol_id, id=<table.index>other_id) #parent<xact>((((_xmin = 6249019 AND _cmin < 0 AND (_xmax = 0 OR (_xmax = 6249019 AND _cmax >= 0))) OR (_xmin_is_committed = true AND (_xmax = 0 OR (_xmax = 6249019 AND _cmax >= 0) OR (_xmax <> 6249019 AND _xmax_is_committed = false)))))) AND ((review_data_ridge.review_set_name:\"test\"))",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"has_parent\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"bool\" : {\n" +
                        "            \"should\" : [ {\n" +
                        "              \"bool\" : {\n" +
                        "                \"must\" : [ {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"_xmin\" : 6249019\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"range\" : {\n" +
                        "                    \"_cmin\" : {\n" +
                        "                      \"from\" : null,\n" +
                        "                      \"to\" : 0,\n" +
                        "                      \"include_lower\" : true,\n" +
                        "                      \"include_upper\" : false\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"should\" : [ {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmax\" : 0\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"must\" : [ {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"_xmax\" : 6249019\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"range\" : {\n" +
                        "                            \"_cmax\" : {\n" +
                        "                              \"from\" : 0,\n" +
                        "                              \"to\" : null,\n" +
                        "                              \"include_lower\" : true,\n" +
                        "                              \"include_upper\" : true\n" +
                        "                            }\n" +
                        "                          }\n" +
                        "                        } ]\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"bool\" : {\n" +
                        "                \"must\" : [ {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"_xmin_is_committed\" : true\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"should\" : [ {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmax\" : 0\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"must\" : [ {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"_xmax\" : 6249019\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"range\" : {\n" +
                        "                            \"_cmax\" : {\n" +
                        "                              \"from\" : 0,\n" +
                        "                              \"to\" : null,\n" +
                        "                              \"include_lower\" : true,\n" +
                        "                              \"include_upper\" : true\n" +
                        "                            }\n" +
                        "                          }\n" +
                        "                        } ]\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"must\" : [ {\n" +
                        "                          \"filtered\" : {\n" +
                        "                            \"query\" : {\n" +
                        "                              \"match_all\" : { }\n" +
                        "                            },\n" +
                        "                            \"filter\" : {\n" +
                        "                              \"not\" : {\n" +
                        "                                \"filter\" : {\n" +
                        "                                  \"query\" : {\n" +
                        "                                    \"term\" : {\n" +
                        "                                      \"_xmax\" : 6249019\n" +
                        "                                    }\n" +
                        "                                  }\n" +
                        "                                }\n" +
                        "                              }\n" +
                        "                            }\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"_xmax_is_committed\" : false\n" +
                        "                          }\n" +
                        "                        } ]\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"parent_type\" : \"xact\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"review_data_ridge.review_set_name\" : \"test\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data_ridge\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_RegexWord() throws Exception {
        assertJson("phrase_field:~'\\d{2}'",
                "{\n" +
                        "  \"regexp\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"value\" : \"\\\\d{2}\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_RegexPhrase() throws Exception {
        assertJson("phrase_field:~'\\d{2} \\d{3}'",
                "{\n" +
                        "  \"regexp\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"value\" : \"\\\\d{2} \\\\d{3}\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_PhraseFieldRegexEndingInWildcard() throws Exception {
        assertJson("phrase_field:~'^.*'",
                "{\n" +
                        "  \"regexp\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"value\" : \"^.*\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_ExactFieldRegexEndingInWildcard() throws Exception {
        assertJson("exact_field:~'^.*'",
                "{\n" +
                        "  \"regexp\" : {\n" +
                        "    \"exact_field\" : {\n" +
                        "      \"value\" : \"^.*\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_RegexProximity() throws Exception {
        assertJson("phrase_field:~ ('[0-9]{2}' w/3 '[0-9]{3}')",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"regexp\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"[0-9]{2}\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"regexp\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"[0-9]{3}\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 3,\n" +
                        "    \"in_order\" : false\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2755() throws Exception {
        assertJson("exact_field = \"CRAZY W/5 PEOPLE W/15 FOREVER W/25 (\\\"EYE UP\\\")\"",
                "{\n" +
                        "  \"term\" : {\n" +
                        "    \"exact_field\" : \"crazy w/5 people w/15 forever w/25 (\\\"eye up\\\")\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2755_WithSingleQuotes() throws Exception {
        assertJson("exact_field = 'CRAZY W/5 PEOPLE W/15 FOREVER W/25 (\"EYE UP\")'",
                "{\n" +
                        "  \"term\" : {\n" +
                        "    \"exact_field\" : \"crazy w/5 people w/15 forever w/25 (\\\"eye up\\\")\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2770_exact() throws Exception {
        assertJson("exact_field = \"\\\"NOTES:KARO\\?\\?\\?\\?\\?\\?\\?\"  ",
                "{\n" +
                        "  \"term\" : {\n" +
                        "    \"exact_field\" : \"\\\"notes:karo???????\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2770_phrase() throws Exception {
        assertJson("phrase_field = \"\\\"NOTES:KARO\\?\\?\\?\\?\\?\\?\\?\"  ",
                "{\n" +
                        "  \"term\" : {\n" +
                        "    \"phrase_field\" : \"notes:karo\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2766() throws Exception {
        assertJson("exact_field:\"7 KING\\'S BENCH WALK\"",
                "{\n" +
                        "  \"term\" : {\n" +
                        "    \"exact_field\" : \"7 king's bench walk\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_914() throws Exception {
        assertJson("\"xxxx17.0000000001.0000000001.0000000001.M.00.0000000-0000000\"",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"should\" : [ {\n" +
                        "      \"match\" : {\n" +
                        "        \"fulltext_field\" : {\n" +
                        "          \"query\" : \"xxxx17.0000000001.0000000001.0000000001.M.00.0000000-0000000\",\n" +
                        "          \"type\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"match\" : {\n" +
                        "        \"_all\" : {\n" +
                        "          \"query\" : \"xxxx17.0000000001.0000000001.0000000001.M.00.0000000-0000000\",\n" +
                        "          \"type\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_FlattenParentChild() throws Exception {
        assertJson("a:1 and #child<data>(field:value or field2:value or field:value2)",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"should\" : [ {\n" +
                        "      \"terms\" : {\n" +
                        "        \"field\" : [ \"value\", \"value2\" ]\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"field2\" : \"value\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}",
                false
        );
    }

    @Test
    public void testMixedFieldnamesNoProx() throws Exception {
        assertAST("outside:(one and inside:two)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      And\n" +
                        "         Word (fieldname=outside, operator=CONTAINS, value=one, index=db.schema.table.index)\n" +
                        "         Word (fieldname=inside, operator=CONTAINS, value=two, index=db.schema.table.index)"
        );
    }

    @Test
    public void testMixedFieldnamesWithProx() throws Exception {
        try {
            qr("outside:(one w/4 (inside:two))");
            fail("Should not be here");
        } catch (RuntimeException re) {
            assertEquals(re.getMessage(), "Cannot mix fieldnames in PROXIMITY expression");
        }
    }

    @Test
    public void testProximalProximity() throws Exception {
        String query = "phrase_field:( ('1 3' w/2 '2 3') w/4 (get w/8 out) )";

        assertAST(query,
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Proximity (fieldname=phrase_field, operator=CONTAINS, distance=4, ordered=false, index=db.schema.table.index)\n" +
                        "         Proximity (fieldname=phrase_field, operator=CONTAINS, distance=2, ordered=false, index=db.schema.table.index)\n" +
                        "            Phrase (fieldname=phrase_field, operator=CONTAINS, value=1 3, index=db.schema.table.index)\n" +
                        "            Phrase (fieldname=phrase_field, operator=CONTAINS, value=2 3, index=db.schema.table.index)\n" +
                        "         Proximity (fieldname=phrase_field, operator=CONTAINS, distance=8, ordered=false, index=db.schema.table.index)\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=get, index=db.schema.table.index)\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=out, index=db.schema.table.index)"
        );

        assertJson(query,
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_near\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_near\" : {\n" +
                        "            \"clauses\" : [ {\n" +
                        "              \"span_term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"1\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"span_term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"3\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            } ],\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"in_order\" : true\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_near\" : {\n" +
                        "            \"clauses\" : [ {\n" +
                        "              \"span_term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"2\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"span_term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"3\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            } ],\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"in_order\" : true\n" +
                        "          }\n" +
                        "        } ],\n" +
                        "        \"slop\" : 2,\n" +
                        "        \"in_order\" : false\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_near\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"get\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"out\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ],\n" +
                        "        \"slop\" : 8,\n" +
                        "        \"in_order\" : false\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 4,\n" +
                        "    \"in_order\" : false\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testProximalProximity2() throws Exception {
        assertAST("fulltext:( (\"K F\" w/1 \"D T\") w/7 \"KN\" )",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Proximity (fieldname=fulltext, operator=CONTAINS, distance=7, ordered=false, index=db.schema.table.index)\n" +
                        "         Proximity (fieldname=fulltext, operator=CONTAINS, distance=1, ordered=false, index=db.schema.table.index)\n" +
                        "            Word (fieldname=fulltext, operator=CONTAINS, value=k f, index=db.schema.table.index)\n" +
                        "            Word (fieldname=fulltext, operator=CONTAINS, value=d t, index=db.schema.table.index)\n" +
                        "         Word (fieldname=fulltext, operator=CONTAINS, value=kn, index=db.schema.table.index)"
        );
    }

    @Test
    public void test_AggregateQuery() throws Exception {
        assertJson("#tally(cp_case_name, \"^.*\", 5000, \"term\") #options(fk_doc_cp_link_doc = <table.index>pk_doc, fk_doc_cp_link_cp = <table.index>pk_cp) #parent<xact>((((_xmin = 5353919 AND _cmin < 0 AND (_xmax = 0 OR (_xmax = 5353919 AND _cmax >= 0))) OR (_xmin_is_committed = true AND (_xmax = 0 OR (_xmax = 5353919 AND _cmax >= 0) OR (_xmax <> 5353919 AND _xmax_is_committed = false)))))) AND ((( ( pk_doc_cp = \"*\" ) )))",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"has_parent\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"bool\" : {\n" +
                        "            \"should\" : [ {\n" +
                        "              \"bool\" : {\n" +
                        "                \"must\" : [ {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"_xmin\" : 5353919\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"range\" : {\n" +
                        "                    \"_cmin\" : {\n" +
                        "                      \"from\" : null,\n" +
                        "                      \"to\" : 0,\n" +
                        "                      \"include_lower\" : true,\n" +
                        "                      \"include_upper\" : false\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"should\" : [ {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmax\" : 0\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"must\" : [ {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"_xmax\" : 5353919\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"range\" : {\n" +
                        "                            \"_cmax\" : {\n" +
                        "                              \"from\" : 0,\n" +
                        "                              \"to\" : null,\n" +
                        "                              \"include_lower\" : true,\n" +
                        "                              \"include_upper\" : true\n" +
                        "                            }\n" +
                        "                          }\n" +
                        "                        } ]\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"bool\" : {\n" +
                        "                \"must\" : [ {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"_xmin_is_committed\" : true\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"should\" : [ {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmax\" : 0\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"must\" : [ {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"_xmax\" : 5353919\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"range\" : {\n" +
                        "                            \"_cmax\" : {\n" +
                        "                              \"from\" : 0,\n" +
                        "                              \"to\" : null,\n" +
                        "                              \"include_lower\" : true,\n" +
                        "                              \"include_upper\" : true\n" +
                        "                            }\n" +
                        "                          }\n" +
                        "                        } ]\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"must\" : [ {\n" +
                        "                          \"filtered\" : {\n" +
                        "                            \"query\" : {\n" +
                        "                              \"match_all\" : { }\n" +
                        "                            },\n" +
                        "                            \"filter\" : {\n" +
                        "                              \"not\" : {\n" +
                        "                                \"filter\" : {\n" +
                        "                                  \"query\" : {\n" +
                        "                                    \"term\" : {\n" +
                        "                                      \"_xmax\" : 5353919\n" +
                        "                                    }\n" +
                        "                                  }\n" +
                        "                                }\n" +
                        "                              }\n" +
                        "                            }\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"_xmax_is_committed\" : false\n" +
                        "                          }\n" +
                        "                        } ]\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"parent_type\" : \"xact\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"filtered\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"match_all\" : { }\n" +
                        "        },\n" +
                        "        \"filter\" : {\n" +
                        "          \"exists\" : {\n" +
                        "            \"field\" : \"pk_doc_cp\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2886() throws Exception {
        assertAST("phrase_field:[\"RESPONSIVE BATCH EDIT\"]",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Phrase (fieldname=phrase_field, operator=CONTAINS, value=RESPONSIVE BATCH EDIT, index=db.schema.table.index)"
        );

        assertAST("phrase_field:[\"RESPONSIVE BATCH EDIT\", \"Insider Trading\"]",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Or\n" +
                        "         Phrase (fieldname=phrase_field, operator=CONTAINS, value=RESPONSIVE BATCH EDIT, index=db.schema.table.index)\n" +
                        "         Phrase (fieldname=phrase_field, operator=CONTAINS, value=Insider Trading, index=db.schema.table.index)"
        );
    }

    @Test
    public void testCVSIX_2874() throws Exception {
        assertAST("( #expand<cvgroupid=<this.index>cvgroupid> ( ( ( review_data_ridge.review_set_name:*BEER* ) ) ) )",
                "QueryTree\n" +
                        "   Or\n" +
                        "      Expansion\n" +
                        "         cvgroupid=<db.schema.table.index>cvgroupid\n" +
                        "         Expansion\n" +
                        "            id=<db.schema.table.index>id\n" +
                        "            Wildcard (fieldname=review_data_ridge.review_set_name, operator=CONTAINS, value=*beer*, index=db.schema.table.index)\n" +
                        "      Expansion\n" +
                        "         id=<db.schema.table.index>id\n" +
                        "         Wildcard (fieldname=review_data_ridge.review_set_name, operator=CONTAINS, value=*beer*, index=db.schema.table.index)"
        );
    }

    @Test
    public void testCVSIX_2792() throws Exception {
        assertAST("( ( (cp_agg_wit.cp_wit_link_witness_status:[\"ALIVE\",\"ICU\"]) AND ( (cars.cid:[\"2\",\"3\",\"1\"]) AND (cars.make:[\"BUICK\",\"CHEVY\",\"FORD\",\"VOLVO\"]) ) ) )",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      And\n" +
                        "         Array (fieldname=cp_agg_wit.cp_wit_link_witness_status, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=cp_agg_wit.cp_wit_link_witness_status, operator=CONTAINS, value=alive, index=db.schema.table.index)\n" +
                        "            Word (fieldname=cp_agg_wit.cp_wit_link_witness_status, operator=CONTAINS, value=icu, index=db.schema.table.index)\n" +
                        "         Array (fieldname=cars.cid, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=cars.cid, operator=CONTAINS, value=2, index=db.schema.table.index)\n" +
                        "            Word (fieldname=cars.cid, operator=CONTAINS, value=3, index=db.schema.table.index)\n" +
                        "            Word (fieldname=cars.cid, operator=CONTAINS, value=1, index=db.schema.table.index)\n" +
                        "         Array (fieldname=cars.make, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=cars.make, operator=CONTAINS, value=buick, index=db.schema.table.index)\n" +
                        "            Word (fieldname=cars.make, operator=CONTAINS, value=chevy, index=db.schema.table.index)\n" +
                        "            Word (fieldname=cars.make, operator=CONTAINS, value=ford, index=db.schema.table.index)\n" +
                        "            Word (fieldname=cars.make, operator=CONTAINS, value=volvo, index=db.schema.table.index)"
        );
    }

    @Test
    public void testCVSIX_2990() throws Exception {
        assertJson("phrase_field:(beer wo/5 wine)",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"beer\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"wine\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 5,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_3030_ast() throws Exception {
        assertAST("( ( custodian = \"QUERTY, SUSAN\" AND NOT NOT review_data_cv623beta.state = CAAT NOT review_data_cv623beta.state = DOOG ) )",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      And\n" +
                        "         Word (fieldname=custodian, operator=EQ, value=querty, susan, index=db.schema.table.index)\n" +
                        "         Not\n" +
                        "            Not\n" +
                        "               Array (fieldname=review_data_cv623beta.state, operator=EQ, index=db.schema.table.index) (OR)\n" +
                        "                  Word (fieldname=review_data_cv623beta.state, operator=EQ, value=caat, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=review_data_cv623beta.state, operator=EQ, value=doog, index=db.schema.table.index)"
        );
    }

    @Test
    public void testCVSIX_3030_json() throws Exception {
        assertJson("( ( custodian = \"QUERTY, SUSAN\" AND NOT NOT review_data_cv623beta.state = CAAT NOT review_data_cv623beta.state = DOOG ) )",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"term\" : {\n" +
                        "        \"custodian\" : \"querty, susan\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must_not\" : {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must_not\" : {\n" +
                        "              \"nested\" : {\n" +
                        "                \"query\" : {\n" +
                        "                  \"terms\" : {\n" +
                        "                    \"review_data_cv623beta.state\" : [ \"caat\", \"doog\" ]\n" +
                        "                  }\n" +
                        "                },\n" +
                        "                \"path\" : \"review_data_cv623beta\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCVSIX_2748() {
        Utils.convertToProximity("field", Arrays.asList(new AnalyzeResponse.AnalyzeToken("you", 0, 0, 0, null), new AnalyzeResponse.AnalyzeToken("not", 0, 0, 0, null), new AnalyzeResponse.AnalyzeToken("i", 0, 0, 0, null)));
        Utils.convertToProximity("field", Arrays.asList(new AnalyzeResponse.AnalyzeToken("you", 0, 0, 0, null), new AnalyzeResponse.AnalyzeToken("and", 0, 0, 0, null), new AnalyzeResponse.AnalyzeToken("i", 0, 0, 0, null)));
        Utils.convertToProximity("field", Arrays.asList(new AnalyzeResponse.AnalyzeToken("you", 0, 0, 0, null), new AnalyzeResponse.AnalyzeToken("or", 0, 0, 0, null), new AnalyzeResponse.AnalyzeToken("i", 0, 0, 0, null)));
    }

    @Test
    public void testSimpleTokenize() {
        assertEquals(Arrays.asList("a", "b", "c"), Utils.simpleTokenize("a b c"));
        assertEquals(Arrays.asList("a", "b", "c"), Utils.simpleTokenize("a-b-c"));
        assertEquals(Arrays.asList("a", "b", "c*"), Utils.simpleTokenize("a-b-c*"));
        assertEquals(Arrays.asList("a", "b", "c*", "d"), Utils.simpleTokenize("a-b-c* d"));
        assertEquals(Arrays.asList("V", "A", "C", "A", "T", "I", "O", "*", "N"), Utils.simpleTokenize("V - A - C - A - T - I - O * N"));
    }

    @Test
    public void testIssue_13() throws Exception {
        assertAST("#options(user_data:(owner_user_id=<so_users.idxso_users>id), comment_data:(id=<so_comments.idxso_comments>post_id)) " +
                        "(user_data.display_name:j* and comment_data.user_display_name:j*)",
                "QueryTree\n" +
                        "   Options\n" +
                        "      user_data:(owner_user_id=<db.schema.so_users.idxso_users>id)\n" +
                        "         LeftField (value=owner_user_id)\n" +
                        "         IndexName (value=db.schema.so_users.idxso_users)\n" +
                        "         RightField (value=id)\n" +
                        "      comment_data:(id=<db.schema.so_comments.idxso_comments>post_id)\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=db.schema.so_comments.idxso_comments)\n" +
                        "         RightField (value=post_id)\n" +
                        "   And\n" +
                        "      Expansion\n" +
                        "         user_data:(owner_user_id=<db.schema.so_users.idxso_users>id)\n" +
                        "            LeftField (value=owner_user_id)\n" +
                        "            IndexName (value=db.schema.so_users.idxso_users)\n" +
                        "            RightField (value=id)\n" +
                        "         Prefix (fieldname=user_data.display_name, operator=CONTAINS, value=j, index=db.schema.so_users.idxso_users)\n" +
                        "      Expansion\n" +
                        "         comment_data:(id=<db.schema.so_comments.idxso_comments>post_id)\n" +
                        "            LeftField (value=id)\n" +
                        "            IndexName (value=db.schema.so_comments.idxso_comments)\n" +
                        "            RightField (value=post_id)\n" +
                        "         Prefix (fieldname=comment_data.user_display_name, operator=CONTAINS, value=j, index=db.schema.so_comments.idxso_comments)\n"
        );
    }

    @Test
    public void testIssue_37_RangeAggregateParsing() throws Exception {
        assertEquals("testIssue_37_RangeAggregateParsing",
                "\n\"page_count\"{\n" +
                        "  \"range\" : {\n" +
                        "    \"field\" : \"page_count\",\n" +
                        "    \"ranges\" : [ {\n" +
                        "      \"key\" : \"first\",\n" +
                        "      \"to\" : 100.0\n" +
                        "    }, {\n" +
                        "      \"from\" : 100.0,\n" +
                        "      \"to\" : 150.0\n" +
                        "    }, {\n" +
                        "      \"from\" : 150.0\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}",
                qr("#range(page_count, '[{\"key\":\"first\", \"to\":100}, {\"from\":100, \"to\":150}, {\"from\":150}]')")
                        .rewriteAggregations()
                        .toXContent(JsonXContent.contentBuilder().prettyPrint(), null).string()
        );
    }

    @Test
    public void testIssue_46_DateRangeAggregateParsing() throws Exception {
        assertEquals("testIssue_99_DateRangeAggregateParsing",
                "\n\"date_field\"{\n" +
                        "  \"date_range\" : {\n" +
                        "    \"field\" : \"date_field.date\",\n" +
                        "    \"ranges\" : [ {\n" +
                        "      \"key\" : \"early\",\n" +
                        "      \"to\" : \"2009-01-01 00:00:00\"\n" +
                        "    }, {\n" +
                        "      \"from\" : \"2009-01-01 00:00:00\",\n" +
                        "      \"to\" : \"2010-01-01 00:00:00\"\n" +
                        "    }, {\n" +
                        "      \"from\" : \"2010-01-01 00:00:00\"\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}",
                qr("#range(date_field, '[{\"key\": \"early\", \"to\":\"2009-01-01 00:00:00\"}, {\"from\":\"2009-01-01 00:00:00\", \"to\":\"2010-01-01 00:00:00\"}, {\"from\":\"2010-01-01 00:00:00\"}]')")
                        .rewriteAggregations()
                        .toXContent(JsonXContent.contentBuilder().prettyPrint(), null).string());
    }

    @Test
    public void testIssue_56() throws Exception {
        assertAST("#expand<parent_id=<this.index>parent_id>(phrase_field:beer)",
                "QueryTree\n" +
                        "   Or\n" +
                        "      Expansion\n" +
                        "         parent_id=<db.schema.table.index>parent_id\n" +
                        "         Expansion\n" +
                        "            id=<db.schema.table.index>id\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=beer, index=db.schema.table.index)\n" +
                        "      Expansion\n" +
                        "         id=<db.schema.table.index>id\n" +
                        "         Word (fieldname=phrase_field, operator=CONTAINS, value=beer, index=db.schema.table.index)");
    }

    @Test
    public void testFieldedProximity() throws Exception {
        assertAST("phrase_field:beer w/500 phrase_field:a",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Proximity (fieldname=phrase_field, operator=CONTAINS, distance=500, ordered=false, index=db.schema.table.index)\n" +
                        "         Word (fieldname=phrase_field, operator=CONTAINS, value=beer, index=db.schema.table.index)\n" +
                        "         Word (fieldname=phrase_field, operator=CONTAINS, value=a, index=db.schema.table.index)"
        );
    }

    @Test
    public void testWithOperatorAST() throws Exception {
        assertAST("nested.exact_field:(a with b with (c or d with e)) and nested2.exact_field:(a with b)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      And\n" +
                        "         With\n" +
                        "            Array (fieldname=nested.exact_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "               Word (fieldname=nested.exact_field, operator=CONTAINS, value=a, index=db.schema.table.index)\n" +
                        "               Word (fieldname=nested.exact_field, operator=CONTAINS, value=b, index=db.schema.table.index)\n" +
                        "            Or\n" +
                        "               Word (fieldname=nested.exact_field, operator=CONTAINS, value=c, index=db.schema.table.index)\n" +
                        "               With\n" +
                        "                  Array (fieldname=nested.exact_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "                     Word (fieldname=nested.exact_field, operator=CONTAINS, value=d, index=db.schema.table.index)\n" +
                        "                     Word (fieldname=nested.exact_field, operator=CONTAINS, value=e, index=db.schema.table.index)\n" +
                        "         With\n" +
                        "            Array (fieldname=nested2.exact_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "               Word (fieldname=nested2.exact_field, operator=CONTAINS, value=a, index=db.schema.table.index)\n" +
                        "               Word (fieldname=nested2.exact_field, operator=CONTAINS, value=b, index=db.schema.table.index)");
    }

    @Test
    public void testExternalWithOperatorAST() throws Exception {
        assertAST("#options(nested:(id=<so_users.idxso_users>other_id)) nested.exact_field:(a with b with (c or d with e)) and nested2.exact_field:(a with b)",
                "QueryTree\n" +
                        "   Options\n" +
                        "      nested:(id=<db.schema.so_users.idxso_users>other_id)\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=db.schema.so_users.idxso_users)\n" +
                        "         RightField (value=other_id)\n" +
                        "   And\n" +
                        "      Expansion\n" +
                        "         nested:(id=<db.schema.so_users.idxso_users>other_id)\n" +
                        "            LeftField (value=id)\n" +
                        "            IndexName (value=db.schema.so_users.idxso_users)\n" +
                        "            RightField (value=other_id)\n" +
                        "         With\n" +
                        "            Array (fieldname=nested.exact_field, operator=CONTAINS, index=db.schema.so_users.idxso_users) (AND)\n" +
                        "               Word (fieldname=nested.exact_field, operator=CONTAINS, value=a, index=db.schema.so_users.idxso_users)\n" +
                        "               Word (fieldname=nested.exact_field, operator=CONTAINS, value=b, index=db.schema.so_users.idxso_users)\n" +
                        "            Or\n" +
                        "               Word (fieldname=nested.exact_field, operator=CONTAINS, value=c, index=db.schema.so_users.idxso_users)\n" +
                        "               With\n" +
                        "                  Array (fieldname=nested.exact_field, operator=CONTAINS, index=db.schema.so_users.idxso_users) (AND)\n" +
                        "                     Word (fieldname=nested.exact_field, operator=CONTAINS, value=d, index=db.schema.so_users.idxso_users)\n" +
                        "                     Word (fieldname=nested.exact_field, operator=CONTAINS, value=e, index=db.schema.so_users.idxso_users)\n" +
                        "      Expansion\n" +
                        "         id=<db.schema.table.index>id\n" +
                        "         With\n" +
                        "            Array (fieldname=nested2.exact_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "               Word (fieldname=nested2.exact_field, operator=CONTAINS, value=a, index=db.schema.table.index)\n" +
                        "               Word (fieldname=nested2.exact_field, operator=CONTAINS, value=b, index=db.schema.table.index)");
    }

    @Test
    public void testWithOperatorJSON() throws Exception {
        assertJson("nested.exact_field:(a with b with (c or d with e)) and nested2.exact_field:(a with b)",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"terms\" : {\n" +
                        "                \"nested.exact_field\" : [ \"a\", \"b\" ],\n" +
                        "                \"minimum_should_match\" : \"2\"\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"bool\" : {\n" +
                        "                \"should\" : [ {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"nested.exact_field\" : \"c\"\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : {\n" +
                        "                      \"terms\" : {\n" +
                        "                        \"nested.exact_field\" : [ \"d\", \"e\" ],\n" +
                        "                        \"minimum_should_match\" : \"2\"\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"nested\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : {\n" +
                        "              \"terms\" : {\n" +
                        "                \"nested2.exact_field\" : [ \"a\", \"b\" ],\n" +
                        "                \"minimum_should_match\" : \"2\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"nested2\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue60() throws Exception {
        assertJson("details.state:NC and details.state:SC",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"details.state\" : \"nc\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"details\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"details.state\" : \"sc\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"details\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue60_WITH() throws Exception {
        assertJson("details.state:NC WITH details.state:SC",
                "{\n" +
                        "  \"nested\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : {\n" +
                        "          \"terms\" : {\n" +
                        "            \"details.state\" : [ \"nc\", \"sc\" ],\n" +
                        "            \"minimum_should_match\" : \"2\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"path\" : \"details\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testMikeWeber() throws Exception {
        assertJson("( " +
                        " ( " +
                        "  (  " +
                        "   (internal_data.internal_set_tag_id = 369 OR internal_data.internal_set_tag_id = 370 OR internal_data.internal_set_tag_id = 371 OR internal_data.internal_set_tag_id = 298 OR internal_data.internal_set_tag_id = 367 OR internal_data.internal_set_tag_id = 295 OR internal_data.internal_set_tag_id = 296) " +
                        "   WITH internal_data.state_id = 4424  " +
                        "   WITH internal_data.assigned_reviewers = \"J_WEBER\"  " +
                        "   WITH (internal_data.status_name:[\"internal_CHECKED_OUT\",\"internal_READY\"]) " +
                        "  ) " +
                        "   OR  " +
                        "  (  " +
                        "   (internal_data.internal_set_tag_id = 369 OR internal_data.internal_set_tag_id = 370 OR internal_data.internal_set_tag_id = 371 OR internal_data.internal_set_tag_id = 298 OR internal_data.internal_set_tag_id = 367 OR internal_data.internal_set_tag_id = 295 OR internal_data.internal_set_tag_id = 296)  " +
                        "   WITH internal_data.state_id = 4424  " +
                        "   WITH (internal_data.status_name:[\"internal_READY\",\"internal_UPDATED\",\"internal_CHECKED_OUT\",\"EXCEPTION\"]) " +
                        "   WITH internal_data.owner_username = \"J_WEBER\"  " +
                        "  )  " +
                        " )  " +
                        ")",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"should\" : [ {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"terms\" : {\n" +
                        "                \"internal_data.internal_set_tag_id\" : [ 369, 370, 371, 298, 367, 295, 296 ]\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"internal_data.state_id\" : 4424\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"internal_data.assigned_reviewers\" : \"j_weber\"\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"terms\" : {\n" +
                        "                \"internal_data.status_name\" : [ \"internal_checked_out\", \"internal_ready\" ]\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"internal_data\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"terms\" : {\n" +
                        "                \"internal_data.internal_set_tag_id\" : [ 369, 370, 371, 298, 367, 295, 296 ]\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"internal_data.state_id\" : 4424\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"terms\" : {\n" +
                        "                \"internal_data.status_name\" : [ \"internal_ready\", \"internal_updated\", \"internal_checked_out\", \"exception\" ]\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"internal_data.owner_username\" : \"j_weber\"\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"internal_data\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue_20() throws Exception {
        assertJson("fulltext_field:\"bob.dole*\"",
                "{\n" +
                        "  \"prefix\" : {\n" +
                        "    \"fulltext_field\" : \"bob.dole\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue_20_ValidateParsing() throws Exception {
        assertJson("exact_field:literal_term exact_field:'quoted_term' extact_field:prefix* exact_field:*wild*card* exact_field:fuzzy~ exact_field:'phrase value' exact_field:'phrase with *wildcard*' " +
                        "phrase_field:literal_term phrase_field:'quoted_term' extact_field:prefix* phrase_field:*wild*card* phrase_field:fuzzy~ phrase_field:'phrase value' phrase_field:'phrase with *wildcard*' ",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"terms\" : {\n" +
                        "        \"exact_field\" : [ \"literal_term\", \"quoted_term\" ],\n" +
                        "        \"minimum_should_match\" : \"2\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"prefix\" : {\n" +
                        "        \"extact_field\" : \"prefix\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"wildcard\" : {\n" +
                        "        \"exact_field\" : \"*wild*card*\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"fuzzy\" : {\n" +
                        "        \"exact_field\" : {\n" +
                        "          \"value\" : \"fuzzy\",\n" +
                        "          \"prefix_length\" : 3\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"exact_field\" : \"phrase value\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"wildcard\" : {\n" +
                        "        \"exact_field\" : \"phrase with *wildcard*\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"terms\" : {\n" +
                        "        \"phrase_field\" : [ \"literal_term\", \"quoted_term\" ],\n" +
                        "        \"minimum_should_match\" : \"2\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"prefix\" : {\n" +
                        "        \"extact_field\" : \"prefix\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"wildcard\" : {\n" +
                        "        \"phrase_field\" : \"*wild*card*\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"fuzzy\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"fuzzy\",\n" +
                        "          \"prefix_length\" : 3\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"match\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"query\" : \"phrase value\",\n" +
                        "          \"type\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_near\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"phrase\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"with\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_multi\" : {\n" +
                        "            \"match\" : {\n" +
                        "              \"wildcard\" : {\n" +
                        "                \"phrase_field\" : \"*wildcard*\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ],\n" +
                        "        \"slop\" : 0,\n" +
                        "        \"in_order\" : true\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue_55_v1() throws Exception {
        assertJson("fulltext_field: \"foo''bar*\"",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"fulltext_field\" : {\n" +
                        "          \"value\" : \"foo\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"prefix\" : {\n" +
                        "            \"fulltext_field\" : \"bar\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue_55_v2() throws Exception {
        assertJson("fulltext_field: \"test_file.xl*\"",
                "{\n" +
                        "  \"prefix\" : {\n" +
                        "    \"fulltext_field\" : \"test_file.xl\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testQueryBoosting() throws Exception {
        assertJson("phrase_field:(term^1.0 field:term^2.0 a^3.0 w/2 b^4.0 'some phrase'^5 fuzzy~^6 wildcard*^7)",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"term\",\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"field\" : {\n" +
                        "          \"value\" : \"term\",\n" +
                        "          \"boost\" : 2.0\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_near\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"a\",\n" +
                        "              \"boost\" : 3.0\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"b\",\n" +
                        "              \"boost\" : 4.0\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ],\n" +
                        "        \"slop\" : 2,\n" +
                        "        \"in_order\" : false\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"match\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"query\" : \"some phrase\",\n" +
                        "          \"type\" : \"phrase\",\n" +
                        "          \"boost\" : 5.0\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"fuzzy\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"fuzzy\",\n" +
                        "          \"boost\" : 6.0,\n" +
                        "          \"prefix_length\" : 3\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"prefix\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"prefix\" : \"wildcard\",\n" +
                        "          \"boost\" : 7.0\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testUnescape() {
        String input = "\\\\\\\\\\\\\\\\FooBar";
        assertEquals("\\\\\\\\FooBar", Utils.unescape(input));
    }

    @Test
    public void testEscaping() throws Exception {
        assertJson("exact_field:'\\\\\\\\Begings with four backslashes'",
                "{\n" +
                        "  \"term\" : {\n" +
                        "    \"exact_field\" : \"\\\\begings with four backslashes\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testEscapingAsPrefix() throws Exception {
        assertJson("exact_field:'This is a prefix query ending in four backslashes\\\\\\\\*'",
                "{\n" +
                        "  \"prefix\" : {\n" +
                        "    \"exact_field\" : \"this is a prefix query ending in four backslashes\\\\\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testEscapingAsWildcard() throws Exception {
        assertJson("exact_field:'This is a wildcard query ending in four backslashes\\\\\\\\?'",
                "{\n" +
                        "  \"wildcard\" : {\n" +
                        "    \"exact_field\" : \"this is a wildcard query ending in four backslashes\\\\?\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testFieldListParsing() throws Exception {
        assertAST("#field_lists()",
                "QueryTree\n" +
                        "   FieldLists"
        );

        assertAST("#field_lists(field1=[a,b,c])",
                "QueryTree\n" +
                        "   FieldLists\n" +
                        "      FieldListEntry (fieldname=field1)\n" +
                        "         Array (index=db.schema.table.index) (OR)\n" +
                        "            Word (value=a, index=db.schema.table.index)\n" +
                        "            Word (value=b, index=db.schema.table.index)\n" +
                        "            Word (value=c, index=db.schema.table.index)"
        );

        assertAST("#field_lists(field1=[a,b,c], field2=[d,e,f])",
                "QueryTree\n" +
                        "   FieldLists\n" +
                        "      FieldListEntry (fieldname=field1)\n" +
                        "         Array (index=db.schema.table.index) (OR)\n" +
                        "            Word (value=a, index=db.schema.table.index)\n" +
                        "            Word (value=b, index=db.schema.table.index)\n" +
                        "            Word (value=c, index=db.schema.table.index)\n" +
                        "      FieldListEntry (fieldname=field2)\n" +
                        "         Array (index=db.schema.table.index) (OR)\n" +
                        "            Word (value=d, index=db.schema.table.index)\n" +
                        "            Word (value=e, index=db.schema.table.index)\n" +
                        "            Word (value=f, index=db.schema.table.index)"
        );

        ASTQueryTree tree = new QueryParser(new StringReader("#field_lists(field1=[a,b,c], field2=[d,e,f])")).parse(true);

        Map<String, ASTFieldListEntry> fieldLists = tree.getFieldLists();
        assertEquals(2, tree.getFieldLists().size());

        assertEquals("FieldListEntry (fieldname=field1)", fieldLists.get("field1").toString());
        assertEquals("[a, b, c]", fieldLists.get("field1").getFields().toString());

        assertEquals("FieldListEntry (fieldname=field2)", fieldLists.get("field2").toString());
        assertEquals("[d, e, f]", fieldLists.get("field2").getFields().toString());
    }

    @Test
    public void testJapaneseCharacters() throws Exception {
        assertJson("phrase_field:物質の総称である。",
                "{\n" +
                        "  \"match\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"query\" : \"物質の総称である。\",\n" +
                        "      \"type\" : \"phrase\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testFieldEqualsDottedIdentifier() throws Exception {
        assertJson("exact_field:some.other.field",
                "{\n" +
                        "  \"term\" : {\n" +
                        "    \"exact_field\" : \"some.other.field\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testRandomStringsAST() throws Exception {
        assertAST("exact_field:(asdflkj234-132asdfuj asiodfja;sdf #487adqerydfskf0230 &@#$23)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      And\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=asdflkj234-132asdfuj, index=db.schema.table.index)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=asiodfja;sdf, index=db.schema.table.index)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=#487adqerydfskf0230, index=db.schema.table.index)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=@#$23, index=db.schema.table.index)"
        );
    }

    @Test
    public void testRandomStringsJson() throws Exception {
        assertJson("phrase_field:(asdflkj234-132asdfuj asiodfja;sdf #487adqerydfskf0230 &@#$23)",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"match\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"query\" : \"asdflkj234-132asdfuj\",\n" +
                        "          \"type\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"match\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"query\" : \"asiodfja;sdf\",\n" +
                        "          \"type\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"phrase_field\" : \"487adqerydfskf0230\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"phrase_field\" : \"23\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testParsePrefixAST_exactField() throws Exception {
        assertAST("exact_field:VALUE*",
                "QueryTree\n" +
                "   Expansion\n" +
                "      id=<db.schema.table.index>id\n" +
                "      Prefix (fieldname=exact_field, operator=CONTAINS, value=value, index=db.schema.table.index)"
        );

        assertAST("exact_field:'VALUE*'",
                "QueryTree\n" +
                "   Expansion\n" +
                "      id=<db.schema.table.index>id\n" +
                "      Prefix (fieldname=exact_field, operator=CONTAINS, value=value, index=db.schema.table.index)"
        );
    }

    @Test
    public void testParsePrefixJSON_exactField() throws Exception {
        assertJson("exact_field:VALUE*",
                "{\n" +
                        "  \"prefix\" : {\n" +
                        "    \"exact_field\" : \"value\"\n" +
                        "  }\n" +
                        "}"
        );

        assertJson("exact_field:'VALUE*'",
                "{\n" +
                        "  \"prefix\" : {\n" +
                        "    \"exact_field\" : \"value\"\n" +
                        "  }\n" +
                        "}"
        );
    }
    
    @Test
    public void testParsePrefixAST_phraseField() throws Exception {
        assertAST("phrase_field:VALUE*",
                "QueryTree\n" +
                "   Expansion\n" +
                "      id=<db.schema.table.index>id\n" +
                "      Prefix (fieldname=phrase_field, operator=CONTAINS, value=value, index=db.schema.table.index)"
        );

        assertAST("phrase_field:'VALUE*'",
                "QueryTree\n" +
                "   Expansion\n" +
                "      id=<db.schema.table.index>id\n" +
                "      Prefix (fieldname=phrase_field, operator=CONTAINS, value=value, index=db.schema.table.index)"
        );
    }

    @Test
    public void testParsePrefixJSON_phraseField() throws Exception {
        assertJson("phrase_field:value*",
                "{\n" +
                        "  \"prefix\" : {\n" +
                        "    \"phrase_field\" : \"value\"\n" +
                        "  }\n" +
                        "}"
        );

        assertJson("phrase_field:'value*'", 
                "{\n" +
                        "  \"prefix\" : {\n" +
                        "    \"phrase_field\" : \"value\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testLeftTruncationWildcardAST() throws Exception {
        assertAST("phrase_field:(*wildcard)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Wildcard (fieldname=phrase_field, operator=CONTAINS, value=*wildcard, index=db.schema.table.index)"
        );
    }

    @Test
    public void testDoubleBrackets() throws Exception {
        assertJson("exact_field:[[a,b,c,d]]",
                "{\n" +
                        "  \"terms\" : {\n" +
                        "    \"exact_field\" : [ \"a\", \"b\", \"c\", \"d\" ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue62AST() throws Exception {
        assertAST("phrase_field:\"* non * programmers\"",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Proximity (fieldname=phrase_field, operator=CONTAINS, index=db.schema.table.index)\n" +
                        "         NotNull (fieldname=phrase_field, operator=CONTAINS, value=*)\n" +
                        "         Word (fieldname=phrase_field, operator=CONTAINS, value=non)\n" +
                        "         NotNull (fieldname=phrase_field, operator=CONTAINS, value=*)\n" +
                        "         Word (fieldname=phrase_field, operator=CONTAINS, value=programmers)"
        );
    }

    @Test
    public void testIssue62Json() throws Exception {
        assertJson("phrase_field:\"* non * programmers\"",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"wildcard\" : {\n" +
                        "            \"phrase_field\" : \"*\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"non\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"wildcard\" : {\n" +
                        "            \"phrase_field\" : \"*\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"programmers\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 0,\n" +
                        "    \"in_order\" : true\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCzech() throws Exception {
        assertJson("czech_field:'toto je test'",
                "{\n" +
                        "  \"match\" : {\n" +
                        "    \"czech_field\" : {\n" +
                        "      \"query\" : \"toto je test\",\n" +
                        "      \"type\" : \"phrase\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue62Highlighting() throws Exception {
        Map<String, Object> data = new HashMap<>();

        DocumentHighlighter highlighter;
        List<AnalyzedField.Token> highlights;

        data.put("phrase_field", "getting non-programmers to understand the development process");
        highlighter = new DocumentHighlighter(client(),
                DEFAULT_INDEX_NAME,
                "id",
                data,
                "phrase_field:\"* non * programmers\"");
        highlights = highlighter.highlight();
        sortHighlightTokens(highlights);

        assertEquals("[{\"term\":\"getting\",\"startOffset\":0,\"endOffset\":7,\"position\":1,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"null\\\"\"},{\"term\":\"non\",\"startOffset\":8,\"endOffset\":11,\"position\":2,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"null\\\"\"},{\"term\":\"programmers\",\"startOffset\":12,\"endOffset\":23,\"position\":3,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"null\\\"\"}]",
                new ObjectMapper().writeValueAsString(highlights));
    }

    @Test
    public void testIssue87() throws Exception {
        Map<String, Object> data = new HashMap<>();


        data.put("phrase_field", "getting non-programmers to understand the development process");

        for (String s : new String[] { "~", ":",  "*",  "?",
                "!",  "%",  "&",  "(",  ")", ",",
                "<",  "=",  ">",  "[",  "]", "^", "@", "#" }) {
            DocumentHighlighter highlighter;
            List<AnalyzedField.Token> highlights;

            highlighter = new DocumentHighlighter(client(),
                    DEFAULT_INDEX_NAME,
                    "id",
                    data,
                    "phrase_field:'" + s + "getting'");
            highlights = highlighter.highlight();
            sortHighlightTokens(highlights);

            assertEquals("[{\"term\":\"getting\",\"startOffset\":0,\"endOffset\":7,\"position\":1,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"" + s + "getting\\\"\"}]",
                    new ObjectMapper().writeValueAsString(highlights));
        }
    }

    @Test
    public void testTermMergingWithBoots() throws Exception {
        assertJson("phrase_field:(beer^3 wine)",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"term\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"value\" : \"beer\",\n" +
                        "          \"boost\" : 3.0\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"phrase_field\" : \"wine\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue69_REGEX_Clause() throws Exception {
        assertAST("phrase_field:~'A.*'",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Word (fieldname=phrase_field, operator=REGEX, value=A.*, index=db.schema.table.index)"
        );

        assertAST("phrase_field:~'^A.*'",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Phrase (fieldname=phrase_field, operator=REGEX, value=^A.*, index=db.schema.table.index)"
        );
    }

    @Test
    public void testBoolQueryAST_Issue75() throws Exception {
        assertAST("#bool( #must(here, there and everywhere)  #should(phrase_field:abc title:xyz stuff)  #must_not(foo bar) )",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      BoolQuery\n" +
                        "         Must\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=here, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=here, index=db.schema.table.index)\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=there, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=there, index=db.schema.table.index)\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=everywhere, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=everywhere, index=db.schema.table.index)\n" +
                        "         Should\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=abc, index=db.schema.table.index)\n" +
                        "            Word (fieldname=title, operator=CONTAINS, value=xyz, index=db.schema.table.index)\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=stuff, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=stuff, index=db.schema.table.index)\n" +
                        "         MustNot\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=foo, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=foo, index=db.schema.table.index)\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=bar, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=bar, index=db.schema.table.index)"
        );
    }

    @Test
    public void testBoolQueryJSON_Issue75() throws Exception {
        assertJson("#bool( #must(a:here, b:there and c:everywhere)  #should(phrase_field:abc title:xyz stuff)  #must_not(x:foo y:bar) )",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"term\" : {\n" +
                        "        \"a\" : \"here\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"b\" : \"there\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"c\" : \"everywhere\"\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"must_not\" : [ {\n" +
                        "      \"term\" : {\n" +
                        "        \"x\" : \"foo\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"y\" : \"bar\"\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"should\" : [ {\n" +
                        "      \"term\" : {\n" +
                        "        \"phrase_field\" : \"abc\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"title\" : \"xyz\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"bool\" : {\n" +
                        "        \"should\" : [ {\n" +
                        "          \"term\" : {\n" +
                        "            \"fulltext_field\" : \"stuff\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"term\" : {\n" +
                        "            \"_all\" : \"stuff\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue75Highlighting() throws Exception {
        Map<String, Object> data = new HashMap<>();

        DocumentHighlighter highlighter;
        List<AnalyzedField.Token> highlights;

        data.put("phrase_field", "a b c d e");
        highlighter = new DocumentHighlighter(client(),
                DEFAULT_INDEX_NAME,
                "id",
                data,
                "#bool(#must(phrase_field:a) #should(phrase_field:b phrase_field:c phrase_field:d) #must_not(phrase_field:e))");
        highlights = highlighter.highlight();
        sortHighlightTokens(highlights);

        assertEquals("[{\"term\":\"a\",\"startOffset\":0,\"endOffset\":1,\"position\":1,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"a\\\"\"},{\"term\":\"b\",\"startOffset\":2,\"endOffset\":3,\"position\":2,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"b\\\"\"},{\"term\":\"c\",\"startOffset\":4,\"endOffset\":5,\"position\":3,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"c\\\"\"},{\"term\":\"d\",\"startOffset\":6,\"endOffset\":7,\"position\":4,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"d\\\"\"}]",
                new ObjectMapper().writeValueAsString(highlights));
    }

    @Test
    public void testIssue75_Connectors() throws Exception {
        assertAST("#bool(#must() #should() #must_not())",
                "QueryTree\n" +
                        "   BoolQuery\n" +
                        "      Must\n" +
                        "      Should\n" +
                        "      MustNot"
        );

        assertAST("#bool(#must(and))",
                "QueryTree\n" +
                        "   BoolQuery\n" +
                        "      Must"
        );

        assertAST("#bool(#must(and or not with , & ! %))",
                "QueryTree\n" +
                        "   BoolQuery\n" +
                        "      Must"
        );

        assertAST("#bool(#must(and or not with , & ! % phrase_field:food))",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      BoolQuery\n" +
                        "         Must\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=food, index=db.schema.table.index)"
        );

        assertAST("#bool(#must(phrase_field:food and or not with , & ! %))",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      BoolQuery\n" +
                        "         Must\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=food, index=db.schema.table.index)"
        );
    }

    @Test
    public void testIssue75_IgnoreParenthesis() throws Exception {
        assertAST("#bool(#must( and,or,not! phrase_field:(this (is title:a) test) () ) #must_not( phrase_field:(this (is title:a) test) () ) #should( phrase_field:(this (is title:a) test) () ))",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      BoolQuery\n" +
                        "         Must\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=this, index=db.schema.table.index)\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=is, index=db.schema.table.index)\n" +
                        "            Word (fieldname=title, operator=CONTAINS, value=a, index=db.schema.table.index)\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=test, index=db.schema.table.index)\n" +
                        "         MustNot\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=this, index=db.schema.table.index)\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=is, index=db.schema.table.index)\n" +
                        "            Word (fieldname=title, operator=CONTAINS, value=a, index=db.schema.table.index)\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=test, index=db.schema.table.index)\n" +
                        "         Should\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=this, index=db.schema.table.index)\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=is, index=db.schema.table.index)\n" +
                        "            Word (fieldname=title, operator=CONTAINS, value=a, index=db.schema.table.index)\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=test, index=db.schema.table.index)"
        );
    }

    @Test
    public void testIssue75_SupportProximityAST() throws Exception {
        assertAST("#bool(#must( phrase_field:(a w/3 b w/7 c) )  #should( phrase_field:(a w/3 b w/7 c) ) #must_not( phrase_field:(a w/3 b w/7 c) ))",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      BoolQuery\n" +
                        "         Must\n" +
                        "            Proximity (fieldname=phrase_field, operator=CONTAINS, distance=3, ordered=false, index=db.schema.table.index)\n" +
                        "               Word (fieldname=phrase_field, operator=CONTAINS, value=a, index=db.schema.table.index)\n" +
                        "               Proximity (fieldname=phrase_field, operator=CONTAINS, distance=7, ordered=false, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=phrase_field, operator=CONTAINS, value=b, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=phrase_field, operator=CONTAINS, value=c, index=db.schema.table.index)\n" +
                        "         Should\n" +
                        "            Proximity (fieldname=phrase_field, operator=CONTAINS, distance=3, ordered=false, index=db.schema.table.index)\n" +
                        "               Word (fieldname=phrase_field, operator=CONTAINS, value=a, index=db.schema.table.index)\n" +
                        "               Proximity (fieldname=phrase_field, operator=CONTAINS, distance=7, ordered=false, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=phrase_field, operator=CONTAINS, value=b, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=phrase_field, operator=CONTAINS, value=c, index=db.schema.table.index)\n" +
                        "         MustNot\n" +
                        "            Proximity (fieldname=phrase_field, operator=CONTAINS, distance=3, ordered=false, index=db.schema.table.index)\n" +
                        "               Word (fieldname=phrase_field, operator=CONTAINS, value=a, index=db.schema.table.index)\n" +
                        "               Proximity (fieldname=phrase_field, operator=CONTAINS, distance=7, ordered=false, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=phrase_field, operator=CONTAINS, value=b, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=phrase_field, operator=CONTAINS, value=c, index=db.schema.table.index)"
        );
    }

    @Test
    public void testIssue75_SupportProximityJson() throws Exception {
        assertJson("#bool(#must( phrase_field:(a w/3 b w/7 c) ))",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : {\n" +
                        "      \"span_near\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"a\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_near\" : {\n" +
                        "            \"clauses\" : [ {\n" +
                        "              \"span_term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"b\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"span_term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"c\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            } ],\n" +
                        "            \"slop\" : 7,\n" +
                        "            \"in_order\" : false\n" +
                        "          }\n" +
                        "        } ],\n" +
                        "        \"slop\" : 3,\n" +
                        "        \"in_order\" : false\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue80_analyzedExactField() throws Exception {
        String q = "exact_field =[[\"12/31/1999\",\"2/3/1999\", \"12/31/2016\", \"UNKNOWN\", \"2/2/2016\"]]";

        assertJson(q,
                "{\n" +
                        "  \"terms\" : {\n" +
                        "    \"exact_field\" : [ \"12/31/1999\", \"2/3/1999\", \"12/31/2016\", \"unknown\", \"2/2/2016\" ]\n" +
                        "  }\n" +
                        "}"
        );

        assertAST(q,
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Array (fieldname=exact_field, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=12/31/1999, index=db.schema.table.index)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=2/3/1999, index=db.schema.table.index)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=12/31/2016, index=db.schema.table.index)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=unknown, index=db.schema.table.index)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=2/2/2016, index=db.schema.table.index)"
        );
    }

    @Test
    public void testIssue80_unanalyzedField() throws Exception {
        String q = "unanalyzed_field =[[\"12/31/1999\",\"2/3/1999\", \"12/31/2016\", \"UNKNOWN\", \"2/2/2016\"]]";
        assertJson(q,
                "{\n" +
                        "  \"terms\" : {\n" +
                        "    \"unanalyzed_field\" : [ \"12/31/1999\", \"2/3/1999\", \"12/31/2016\", \"UNKNOWN\", \"2/2/2016\" ]\n" +
                        "  }\n" +
                        "}"
        );

        assertAST(q,
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      ArrayData (fieldname=unanalyzed_field, operator=EQ, value=$0, index=db.schema.table.index)"
        );
    }

    @Test
    public void testIssue80_analyzedPhraseField() throws Exception {
        String q = "phrase_field =[[\"This is a mIxEDcAsE PHRASE\", \"UNKNOWN\", \"12/31/1999\"]]";

        assertJson(q,
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"should\" : [ {\n" +
                        "      \"match\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"query\" : \"This is a mIxEDcAsE PHRASE\",\n" +
                        "          \"type\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"match\" : {\n" +
                        "        \"phrase_field\" : {\n" +
                        "          \"query\" : \"12/31/1999\",\n" +
                        "          \"type\" : \"phrase\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"terms\" : {\n" +
                        "        \"phrase_field\" : [ \"unknown\" ]\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );

        assertAST(q,
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Or\n" +
                        "         Phrase (fieldname=phrase_field, operator=CONTAINS, value=This is a mIxEDcAsE PHRASE, index=db.schema.table.index)\n" +
                        "         Phrase (fieldname=phrase_field, operator=CONTAINS, value=12/31/1999, index=db.schema.table.index)\n" +
                        "         Array (fieldname=phrase_field, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=unknown, index=db.schema.table.index)"
        );
    }

    @Test
    public void testIssue80_pkeyColumn() throws Exception {
        String q = "id:[[1,2,3,4,5,6,7,8,9,10]]";

        assertJson(q,
                "{\n" +
                        "  \"terms\" : {\n" +
                        "    \"id\" : [ \"1\", \"2\", \"3\", \"4\", \"5\", \"6\", \"7\", \"8\", \"9\", \"10\" ]\n" +
                        "  }\n" +
                        "}"
        );

        assertAST(q,
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      ArrayData (fieldname=id, operator=CONTAINS, value=$0, index=db.schema.table.index)"
        );
    }

    @Test
    public void testStopWordRemoval_IndividualTerms() throws Exception {
        assertAST("english_field:(now is the time)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Array (fieldname=english_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "         Word (fieldname=english_field, operator=CONTAINS, value=now, index=db.schema.table.index)\n" +
                        "         Word (fieldname=english_field, operator=CONTAINS, value=time, index=db.schema.table.index)"
        );
    }

    @Test
    public void testStopWordRemoval_Phrase() throws Exception {
        assertAST("english_field:'now is the time'",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Phrase (fieldname=english_field, operator=CONTAINS, value=now is the time, index=db.schema.table.index)"
        );
    }

    @Test
    public void testStopWordRemoval_allField() throws Exception {
        assertAST("(now is the time) OR english_field:(now is the time)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Or\n" +
                        "         And\n" +
                        "            Array (fieldname=english_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "               Word (fieldname=english_field, operator=CONTAINS, value=now, index=db.schema.table.index)\n" +
                        "               Word (fieldname=english_field, operator=CONTAINS, value=time, index=db.schema.table.index)\n" +
                        "         Array (fieldname=fulltext_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=now, index=db.schema.table.index)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=is, index=db.schema.table.index)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=the, index=db.schema.table.index)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=time, index=db.schema.table.index)\n" +
                        "         Array (fieldname=_all, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=now, index=db.schema.table.index)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=is, index=db.schema.table.index)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=the, index=db.schema.table.index)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=time, index=db.schema.table.index)"
        );
    }
}

