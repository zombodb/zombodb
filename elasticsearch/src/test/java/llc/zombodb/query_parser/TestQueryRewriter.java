/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2019 ZomboDB, LLC
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

import llc.zombodb.highlight.AnalyzedField;
import llc.zombodb.highlight.DocumentHighlighter;
import llc.zombodb.query_parser.metadata.IndexMetadataManager;
import llc.zombodb.query_parser.rewriters.QueryRewriter;
import llc.zombodb.query_parser.utils.Utils;
import llc.zombodb.test.ZomboDBTestCase;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Tests for {@link QueryRewriter}
 */
public class TestQueryRewriter extends ZomboDBTestCase {
    String query = "#options(id=<table.index>id, id=<table.index>id, id=<table.index>id, other:(left=<table.index>right)) #extended_stats(custodian) #tally(subject, '^.*', 1000, '_term', #significant_terms(author, '^.*', 1000))  " +
            "#field_lists(field1=[a,b,c], field2=[d,e,f], field3=[a,b,c,d,e,f]) " +
            "(" +
            "fulltext=[beer] meeting not staff not cancelled not risk " +
            "#expand<left_field = <this.index>right_field>(the subquery) " +
            "(some query) (other query) (())" +
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
        assertAST("#options(id=<main_ft.idxmain_ft>ft_id, id=<main_vol.idxmain_vol>vol_id, id=<main_other.idxmain_other>other_id) (((_xmin = 6250261 AND _cmin < 0 AND (_xmax = 0 OR (_xmax = 6250261 AND _cmax >= 0))) OR (_xmin_is_committed = true AND (_xmax = 0 OR (_xmax = 6250261 AND _cmax >= 0) OR (_xmax <> 6250261 AND _xmax_is_committed = false))))) AND (((phrase_field:(beer w/500 a))))",
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
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      And\n" +
                        "         Or\n" +
                        "            And\n" +
                        "               Number (fieldname=_xmin, operator=EQ, value=6250261)\n" +
                        "               Number (fieldname=_cmin, operator=LT, value=0)\n" +
                        "               Or\n" +
                        "                  Number (fieldname=_xmax, operator=EQ, value=0)\n" +
                        "                  And\n" +
                        "                     Number (fieldname=_xmax, operator=EQ, value=6250261)\n" +
                        "                     Number (fieldname=_cmax, operator=GTE, value=0)\n" +
                        "            And\n" +
                        "               Boolean (fieldname=_xmin_is_committed, operator=EQ, value=true)\n" +
                        "               Or\n" +
                        "                  Number (fieldname=_xmax, operator=EQ, value=0)\n" +
                        "                  And\n" +
                        "                     Number (fieldname=_xmax, operator=EQ, value=6250261)\n" +
                        "                     Number (fieldname=_cmax, operator=GTE, value=0)\n" +
                        "                  And\n" +
                        "                     Number (fieldname=_xmax, operator=NE, value=6250261)\n" +
                        "                     Boolean (fieldname=_xmax_is_committed, operator=EQ, value=false)\n" +
                        "         Proximity (fieldname=phrase_field, operator=CONTAINS, distance=500, ordered=false, index=db.schema.table.index)\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=beer, index=db.schema.table.index)\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=a, index=db.schema.table.index)"
        );
    }

    @Test
    public void testASTExpansionInjection2() throws Exception {
        assertAST("#options(id=<so_users.idxso_users>ft_id, id=<so_users.idxso_users>vol_id, id=<so_users.idxso_users>other_id) (((_xmin = 6250507 AND _cmin < 0 AND (_xmax = 0 OR (_xmax = 6250507 AND _cmax >= 0))) OR (_xmin_is_committed = true AND (_xmax = 0 OR (_xmax = 6250507 AND _cmax >= 0) OR (_xmax <> 6250507 AND _xmax_is_committed = false))))) AND (((( #expand<data_cv_group_id=<this.index>data_cv_group_id> ( ( (( ( data_client_name = ANTHEM AND data_duplicate_resource = NO ) )) AND " +
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
                        "      Or\n" +
                        "         Expansion\n" +
                        "            data_cv_group_id=<db.schema.table.index>data_cv_group_id\n" +
                        "            And\n" +
                        "               Word (fieldname=data_client_name, operator=EQ, value=anthem, index=db.schema.table.index)\n" +
                        "               Word (fieldname=data_duplicate_resource, operator=EQ, value=no, index=db.schema.table.index)\n" +
                        "               Array (fieldname=data_custodian, operator=EQ, index=db.schema.table.index) (OR)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=querty, amy, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty, colin, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty, keith, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty, perry, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty, norm, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty, mike, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty,mike, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty, dan, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty,dan, index=db.schema.table.index)\n" +
                        "               Prefix (fieldname=data_filter_06b, operator=EQ, value=qwerty, index=db.schema.table.index)\n" +
                        "               Not\n" +
                        "                  NotNull (fieldname=data_moved_to, operator=EQ, index=db.schema.table.index)\n" +
                        "         Expansion\n" +
                        "            id=<db.schema.table.index>id\n" +
                        "            And\n" +
                        "               Word (fieldname=data_client_name, operator=EQ, value=anthem, index=db.schema.table.index)\n" +
                        "               Word (fieldname=data_duplicate_resource, operator=EQ, value=no, index=db.schema.table.index)\n" +
                        "               Array (fieldname=data_custodian, operator=EQ, index=db.schema.table.index) (OR)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=querty, amy, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty, colin, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty, keith, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty, perry, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty, norm, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty, mike, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty,mike, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty, dan, index=db.schema.table.index)\n" +
                        "                  Word (fieldname=data_custodian, operator=EQ, value=qwerty,dan, index=db.schema.table.index)\n" +
                        "               Prefix (fieldname=data_filter_06b, operator=EQ, value=qwerty, index=db.schema.table.index)\n" +
                        "               Not\n" +
                        "                  NotNull (fieldname=data_moved_to, operator=EQ, index=db.schema.table.index)"
        );
    }

    @Test
    public void testSimplePhrase() throws Exception {
        assertJson("phrase_field:(\"this is a phrase\")",
                "{\n" +
                        "  \"match_phrase\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"query\" : \"this is a phrase\",\n" +
                        "      \"slop\" : 0,\n" +
                        "      \"boost\" : 1.0\n" +
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
                        "    \"should\" : [\n" +
                        "      {\n" +
                        "        \"match_phrase\" : {\n" +
                        "          \"fulltext_field\" : {\n" +
                        "            \"query\" : \"* this phrase has ?escaped~ wildcards*\",\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"match_phrase\" : {\n" +
                        "          \"_all\" : {\n" +
                        "            \"query\" : \"* this phrase has ?escaped~ wildcards*\",\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"disable_coord\" : false,\n" +
                        "    \"adjust_pure_negative\" : true,\n" +
                        "    \"boost\" : 1.0\n" +
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
                        "  \"match_phrase\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"query\" : \"Here~ is~ fuzzy~ words\",\n" +
                        "      \"slop\" : 0,\n" +
                        "      \"boost\" : 1.0\n" +
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
                        "          \"value\" : \"should*\"\n" +
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
                        "          \"value\" : \"\\\\\"\n" +
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
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"fuzzy\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"sp?n\",\n" +
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
                        "  \"match_phrase\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"query\" : \"some phrase containing slop\",\n" +
                        "      \"slop\" : 2,\n" +
                        "      \"boost\" : 1.0\n" +
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
                        "      \"bool\" : {\n" +
                        "        \"must_not\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"id\" : 1\n" +
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
                        "      \"term\" : {\n" +
                        "        \"wit_first_name\" : \"mark\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"wit_last_name\" : \"matte\"\n" +
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
                        "  \"match_phrase\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"query\" : \"c-note\",\n" +
                        "      \"slop\" : 0,\n" +
                        "      \"boost\" : 1.0\n" +
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
                        "  \"match_phrase\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"query\" : \"Qwerty *FREE Samples*\",\n" +
                        "      \"slop\" : 0,\n" +
                        "      \"boost\" : 1.0\n" +
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
                        "    \"should\" : [\n" +
                        "      {\n" +
                        "        \"match_phrase\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"query\" : \"cut-over\",\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"match_phrase\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"query\" : \"get-prices\",\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"disable_coord\" : false,\n" +
                        "    \"adjust_pure_negative\" : true,\n" +
                        "    \"boost\" : 1.0\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testTermRollups() throws Exception {
        assertJson("id: 100 OR id: 200",
                "{\n" +
                        "  \"constant_score\" : {\n" +
                        "    \"filter\" : {\n" +
                        "      \"terms\" : {\n" +
                        "        \"id\" : [ 100, 200 ]\n" +
                        "      }\n" +
                        "    }\n" +
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
                        "  \"bool\" : {\n" +
                        "    \"must_not\" : {\n" +
                        "      \"prefix\" : {\n" +
                        "        \"exact_field\" : \"bob\"\n" +
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
                        "  \"bool\" : {\n" +
                        "    \"must_not\" : {\n" +
                        "      \"prefix\" : {\n" +
                        "        \"phrase_field\" : \"bob\"\n" +
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
                        "    \"like\" : [ \"this is a test\" ],\n" +
                        "    \"max_query_terms\" : 80,\n" +
                        "    \"min_term_freq\" : 1,\n" +
                        "    \"min_word_length\" : 3,\n" +
                        "    \"stop_words\" : [ \"http\", \"span\", \"class\", \"flashtext\", \"let\", \"its\", \"may\", \"well\", \"got\", \"too\", \"them\", \"really\", \"new\", \"set\", \"please\", \"how\", \"our\", \"from\", \"sent\", \"subject\", \"sincerely\", \"thank\", \"thanks\", \"just\", \"get\", \"going\", \"were\", \"much\", \"can\", \"also\", \"she\", \"her\", \"him\", \"his\", \"has\", \"been\", \"ok\", \"still\", \"okay\", \"does\", \"did\", \"about\", \"yes\", \"you\", \"your\", \"when\", \"know\", \"have\", \"who\", \"what\", \"where\", \"sir\", \"page\", \"a\", \"an\", \"and\", \"are\", \"as\", \"at\", \"be\", \"but\", \"by\", \"for\", \"if\", \"in\", \"into\", \"is\", \"it\", \"no\", \"not\", \"of\", \"on\", \"or\", \"such\", \"that\", \"the\", \"their\", \"than\", \"then\", \"there\", \"these\", \"they\", \"this\", \"to\", \"was\", \"will\", \"with\" ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testFuzzyLikeThis() throws Exception {
        assertJson("phrase_field:@~'this is a test'",
                "{\n" +
                        "  \"fuzzy\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"value\" : \"this is a test\",\n" +
                        "      \"fuzziness\" : \"AUTO\",\n" +
                        "      \"max_expansions\" : 80\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testScript() throws Exception {
        assertJson("$$ this.is.a.script[12] = 42; $$",
                "{\n" +
                        "  \"script\" : {\n" +
                        "    \"script\" : {\n" +
                        "      \"inline\" : \" this.is.a.script[12] = 42; \"\n" +
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
                        "          \"value\" : \"food*\"\n" +
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
        // since ES 2.4, "minimum_should_match" isn't supported
        // as such, ANDed literals don't get merged in JSON
        // only in AST
        assertJson("exact_field:(one & two & three)",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"term\" : {\n" +
                        "        \"exact_field\" : \"one\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"exact_field\" : \"two\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"exact_field\" : \"three\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );

        assertAST(
                "exact_field:(one & two & three)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Array (fieldname=exact_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=one, index=db.schema.table.index)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=two, index=db.schema.table.index)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=three, index=db.schema.table.index)"
        );
    }

    @Test
    public void test_MergeLiterals_AND_NE() throws Exception {
        assertJson("exact_field<>(one & two & three)",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must_not\" : {\n" +
                        "      \"terms\" : {\n" +
                        "        \"exact_field\" : [ \"one\", \"two\", \"three\" ]\n" +
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
                        "  \"bool\" : {\n" +
                        "    \"must_not\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"term\" : {\n" +
                        "            \"exact_field\" : \"one\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"term\" : {\n" +
                        "            \"exact_field\" : \"two\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"term\" : {\n" +
                        "            \"exact_field\" : \"three\"\n" +
                        "          }\n" +
                        "        } ]\n" +
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
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"term\" : {\n" +
                        "            \"exact_field\" : \"one\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"term\" : {\n" +
                        "            \"exact_field\" : \"two\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"term\" : {\n" +
                        "            \"exact_field\" : \"three\"\n" +
                        "          }\n" +
                        "        } ]\n" +
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
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"term\" : {\n" +
                        "                \"exact_field\" : \"one\"\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"exact_field\" : \"two\"\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"exact_field\" : \"three\"\n" +
                        "              }\n" +
                        "            } ]\n" +
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
                        "      \"nested\" : {\n" +
                        "        \"query\" : {\n" +
                        "          \"terms\" : {\n" +
                        "            \"review_data_ben.coding.responsiveness\" : [ \"responsive\", \"potentially responsive\", \"not responsive\", \"unreviewable\" ]\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"path\" : \"review_data_ben\"\n" +
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
                        "        \"must\" : [\n" +
                        "          {\n" +
                        "            \"term\" : {\n" +
                        "              \"review_data_ben.review_data_id\" : {\n" +
                        "                \"value\" : 67115,\n" +
                        "                \"boost\" : 1.0\n" +
                        "              }\n" +
                        "            }\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"terms\" : {\n" +
                        "              \"review_data_ben.coding.responsiveness\" : [\n" +
                        "                \"responsive\",\n" +
                        "                \"potentially responsive\",\n" +
                        "                \"not responsive\",\n" +
                        "                \"unreviewable\"\n" +
                        "              ],\n" +
                        "              \"boost\" : 1.0\n" +
                        "            }\n" +
                        "          }\n" +
                        "        ],\n" +
                        "        \"disable_coord\" : false,\n" +
                        "        \"adjust_pure_negative\" : true,\n" +
                        "        \"boost\" : 1.0\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"path\" : \"review_data_ben\",\n" +
                        "    \"ignore_unmapped\" : false,\n" +
                        "    \"score_mode\" : \"avg\",\n" +
                        "    \"boost\" : 1.0\n" +
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
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"term\" : {\n" +
                        "            \"field\" : \"drink\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"term\" : {\n" +
                        "            \"field\" : \"food\"\n" +
                        "          }\n" +
                        "        } ]\n" +
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
        assertJson("(( ( ((review_data.owner_username=E_RIDGE WITH review_data.status_name:[\"REVIEW_UPDATED\",\"REVIEW_CHECKED_OUT\"]) OR (review_data.status_name:REVIEW_READY)) WITH review_data.project_id = 1040 ) ) )",
                "{\n" +
                        "  \"nested\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [\n" +
                        "          {\n" +
                        "            \"bool\" : {\n" +
                        "              \"should\" : [\n" +
                        "                {\n" +
                        "                  \"nested\" : {\n" +
                        "                    \"query\" : {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"must\" : [\n" +
                        "                          {\n" +
                        "                            \"term\" : {\n" +
                        "                              \"review_data.owner_username\" : {\n" +
                        "                                \"value\" : \"e_ridge\",\n" +
                        "                                \"boost\" : 1.0\n" +
                        "                              }\n" +
                        "                            }\n" +
                        "                          },\n" +
                        "                          {\n" +
                        "                            \"terms\" : {\n" +
                        "                              \"review_data.status_name\" : [\n" +
                        "                                \"review_updated\",\n" +
                        "                                \"review_checked_out\"\n" +
                        "                              ],\n" +
                        "                              \"boost\" : 1.0\n" +
                        "                            }\n" +
                        "                          }\n" +
                        "                        ],\n" +
                        "                        \"disable_coord\" : false,\n" +
                        "                        \"adjust_pure_negative\" : true,\n" +
                        "                        \"boost\" : 1.0\n" +
                        "                      }\n" +
                        "                    },\n" +
                        "                    \"path\" : \"review_data\",\n" +
                        "                    \"ignore_unmapped\" : false,\n" +
                        "                    \"score_mode\" : \"avg\",\n" +
                        "                    \"boost\" : 1.0\n" +
                        "                  }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                  \"nested\" : {\n" +
                        "                    \"query\" : {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"review_data.status_name\" : {\n" +
                        "                          \"value\" : \"review_ready\",\n" +
                        "                          \"boost\" : 1.0\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    },\n" +
                        "                    \"path\" : \"review_data\",\n" +
                        "                    \"ignore_unmapped\" : false,\n" +
                        "                    \"score_mode\" : \"avg\",\n" +
                        "                    \"boost\" : 1.0\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              ],\n" +
                        "              \"disable_coord\" : false,\n" +
                        "              \"adjust_pure_negative\" : true,\n" +
                        "              \"boost\" : 1.0\n" +
                        "            }\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"term\" : {\n" +
                        "              \"review_data.project_id\" : {\n" +
                        "                \"value\" : 1040,\n" +
                        "                \"boost\" : 1.0\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        ],\n" +
                        "        \"disable_coord\" : false,\n" +
                        "        \"adjust_pure_negative\" : true,\n" +
                        "        \"boost\" : 1.0\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"path\" : \"review_data\",\n" +
                        "    \"ignore_unmapped\" : false,\n" +
                        "    \"score_mode\" : \"avg\",\n" +
                        "    \"boost\" : 1.0\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_CVSIX_2941_Aggregate() throws Exception {
        assertJson("#tally(review_data_ridge.coding.responsiveness, \"^.*\", 5000, \"term\") #options(id=<table.index>ft_id, id=<table.index>vol_id, id=<table.index>other_id) ((((_xmin = 6249019 AND _cmin < 0 AND (_xmax = 0 OR (_xmax = 6249019 AND _cmax >= 0))) OR (_xmin_is_committed = true AND (_xmax = 0 OR (_xmax = 6249019 AND _cmax >= 0) OR (_xmax <> 6249019 AND _xmax_is_committed = false)))))) AND ((review_data_ridge.review_set_name:\"test\"))",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"bool\" : {\n" +
                        "        \"should\" : [ {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"term\" : {\n" +
                        "                \"_xmin\" : 6249019\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"range\" : {\n" +
                        "                \"_cmin\" : {\n" +
                        "                  \"from\" : null,\n" +
                        "                  \"to\" : 0,\n" +
                        "                  \"include_lower\" : true,\n" +
                        "                  \"include_upper\" : false\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"bool\" : {\n" +
                        "                \"should\" : [ {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"_xmax\" : 0\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : [ {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmax\" : 6249019\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"range\" : {\n" +
                        "                        \"_cmax\" : {\n" +
                        "                          \"from\" : 0,\n" +
                        "                          \"to\" : null,\n" +
                        "                          \"include_lower\" : true,\n" +
                        "                          \"include_upper\" : true\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"term\" : {\n" +
                        "                \"_xmin_is_committed\" : true\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"bool\" : {\n" +
                        "                \"should\" : [ {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"_xmax\" : 0\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : [ {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmax\" : 6249019\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"range\" : {\n" +
                        "                        \"_cmax\" : {\n" +
                        "                          \"from\" : 0,\n" +
                        "                          \"to\" : null,\n" +
                        "                          \"include_lower\" : true,\n" +
                        "                          \"include_upper\" : true\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : [ {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"must_not\" : {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"_xmax\" : 6249019\n" +
                        "                          }\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmax_is_committed\" : false\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        } ]\n" +
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
                        "      \"value\" : \"\\\\d{2}\",\n" +
                        "      \"flags_value\" : 65535\n" +
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
                        "      \"value\" : \"\\\\d{2} \\\\d{3}\",\n" +
                        "      \"flags_value\" : 65535\n" +
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
                        "      \"value\" : \"^.*\",\n" +
                        "      \"flags_value\" : 65535\n" +
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
                        "      \"value\" : \"^.*\",\n" +
                        "      \"flags_value\" : 65535\n" +
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
                        "              \"value\" : \"[0-9]{2}\",\n" +
                        "              \"flags_value\" : 65535\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"regexp\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"[0-9]{3}\",\n" +
                        "              \"flags_value\" : 65535\n" +
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
                        "  \"match_phrase\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"query\" : \"\\\"NOTES:KARO???????\",\n" +
                        "      \"slop\" : 0,\n" +
                        "      \"boost\" : 1.0\n" +
                        "    }\n" +
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
                        "    \"should\" : [\n" +
                        "      {\n" +
                        "        \"match_phrase\" : {\n" +
                        "          \"fulltext_field\" : {\n" +
                        "            \"query\" : \"xxxx17.0000000001.0000000001.0000000001.M.00.0000000-0000000\",\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"match_phrase\" : {\n" +
                        "          \"_all\" : {\n" +
                        "            \"query\" : \"xxxx17.0000000001.0000000001.0000000001.M.00.0000000-0000000\",\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"disable_coord\" : false,\n" +
                        "    \"adjust_pure_negative\" : true,\n" +
                        "    \"boost\" : 1.0\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void test_FlattenParentChild() throws Exception {
        assertJson("a:1 and (field:value or field2:value or field:value2)",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"term\" : {\n" +
                        "        \"a\" : 1\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"bool\" : {\n" +
                        "        \"should\" : [ {\n" +
                        "          \"terms\" : {\n" +
                        "            \"field\" : [ \"value\", \"value2\" ]\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"term\" : {\n" +
                        "            \"field2\" : \"value\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
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
        assertJson("#tally(cp_case_name, \"^.*\", 5000, \"term\") #options(fk_doc_cp_link_doc = <table.index>pk_doc, fk_doc_cp_link_cp = <table.index>pk_cp) ((((_xmin = 5353919 AND _cmin < 0 AND (_xmax = 0 OR (_xmax = 5353919 AND _cmax >= 0))) OR (_xmin_is_committed = true AND (_xmax = 0 OR (_xmax = 5353919 AND _cmax >= 0) OR (_xmax <> 5353919 AND _xmax_is_committed = false)))))) AND ((( ( pk_doc_cp = \"*\" ) )))",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"bool\" : {\n" +
                        "        \"should\" : [ {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"term\" : {\n" +
                        "                \"_xmin\" : 5353919\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"range\" : {\n" +
                        "                \"_cmin\" : {\n" +
                        "                  \"from\" : null,\n" +
                        "                  \"to\" : 0,\n" +
                        "                  \"include_lower\" : true,\n" +
                        "                  \"include_upper\" : false\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"bool\" : {\n" +
                        "                \"should\" : [ {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"_xmax\" : 0\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : [ {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmax\" : 5353919\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"range\" : {\n" +
                        "                        \"_cmax\" : {\n" +
                        "                          \"from\" : 0,\n" +
                        "                          \"to\" : null,\n" +
                        "                          \"include_lower\" : true,\n" +
                        "                          \"include_upper\" : true\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"term\" : {\n" +
                        "                \"_xmin_is_committed\" : true\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"bool\" : {\n" +
                        "                \"should\" : [ {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"_xmax\" : 0\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : [ {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmax\" : 5353919\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"range\" : {\n" +
                        "                        \"_cmax\" : {\n" +
                        "                          \"from\" : 0,\n" +
                        "                          \"to\" : null,\n" +
                        "                          \"include_lower\" : true,\n" +
                        "                          \"include_upper\" : true\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : [ {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"must_not\" : {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"_xmax\" : 5353919\n" +
                        "                          }\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmax_is_committed\" : false\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"exists\" : {\n" +
                        "        \"field\" : \"pk_doc_cp\"\n" +
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
                        "         Wildcard (fieldname=review_data_ridge.review_set_name, operator=CONTAINS, value=*beer*, index=db.schema.table.index)\n" +
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
        Utils.convertToProximity("field", Arrays.asList(new AnalyzeResponse.AnalyzeToken("you", 0, 0, 0, 0, null, Collections.EMPTY_MAP), new AnalyzeResponse.AnalyzeToken("not", 0, 0, 0, 0, null, Collections.EMPTY_MAP), new AnalyzeResponse.AnalyzeToken("i", 0, 0, 0, 0, null, Collections.EMPTY_MAP)));
        Utils.convertToProximity("field", Arrays.asList(new AnalyzeResponse.AnalyzeToken("you", 0, 0, 0, 0, null, Collections.EMPTY_MAP), new AnalyzeResponse.AnalyzeToken("and", 0, 0, 0, 0, null, Collections.EMPTY_MAP), new AnalyzeResponse.AnalyzeToken("i", 0, 0, 0, 0, null, Collections.EMPTY_MAP)));
        Utils.convertToProximity("field", Arrays.asList(new AnalyzeResponse.AnalyzeToken("you", 0, 0, 0, 0, null, Collections.EMPTY_MAP), new AnalyzeResponse.AnalyzeToken("or", 0, 0, 0, 0, null, Collections.EMPTY_MAP), new AnalyzeResponse.AnalyzeToken("i", 0, 0, 0, 0, null, Collections.EMPTY_MAP)));
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
                        "         Prefix (fieldname=display_name, operator=CONTAINS, value=j, index=db.schema.so_users.idxso_users)\n" +
                        "      Expansion\n" +
                        "         comment_data:(id=<db.schema.so_comments.idxso_comments>post_id)\n" +
                        "            LeftField (value=id)\n" +
                        "            IndexName (value=db.schema.so_comments.idxso_comments)\n" +
                        "            RightField (value=post_id)\n" +
                        "         Prefix (fieldname=user_display_name, operator=CONTAINS, value=j, index=db.schema.so_comments.idxso_comments)"
        );
    }

    @Test
    public void testIssue_37_RangeAggregateParsing() throws Exception {
        Assert.assertEquals("testIssue_37_RangeAggregateParsing",
                "\n" +
                        "\"page_count\"{\n" +
                        "  \"range\" : {\n" +
                        "    \"field\" : \"page_count\",\n" +
                        "    \"ranges\" : [\n" +
                        "      {\n" +
                        "        \"key\" : \"first\",\n" +
                        "        \"to\" : 100.0\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"from\" : 100.0,\n" +
                        "        \"to\" : 150.0\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"from\" : 150.0\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"keyed\" : false\n" +
                        "  }\n" +
                        "}",
                qr("#range(page_count, '[{\"key\":\"first\", \"to\":100}, {\"from\":100, \"to\":150}, {\"from\":150}]')")
                        .rewriteAggregations()
                        .toXContent(JsonXContent.contentBuilder().prettyPrint(), null).string().replaceAll("\r", "")
        );
    }

    @Test
    public void testIssue_46_DateRangeAggregateParsing() throws Exception {
        Assert.assertEquals("testIssue_99_DateRangeAggregateParsing",
                "\n" +
                        "\"date_field\"{\n" +
                        "  \"date_range\" : {\n" +
                        "    \"field\" : \"date_field.date\",\n" +
                        "    \"ranges\" : [\n" +
                        "      {\n" +
                        "        \"key\" : \"early\",\n" +
                        "        \"to\" : \"2009-01-01 00:00:00\"\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"from\" : \"2009-01-01 00:00:00\",\n" +
                        "        \"to\" : \"2010-01-01 00:00:00\"\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"from\" : \"2010-01-01 00:00:00\"\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"keyed\" : false\n" +
                        "  }\n" +
                        "}",
                qr("#range(date_field, '[{\"key\": \"early\", \"to\":\"2009-01-01 00:00:00\"}, {\"from\":\"2009-01-01 00:00:00\", \"to\":\"2010-01-01 00:00:00\"}, {\"from\":\"2010-01-01 00:00:00\"}]')")
                        .rewriteAggregations()
                        .toXContent(JsonXContent.contentBuilder().prettyPrint(), null).string()
        );
    }

    @Test
    public void testIssue_56() throws Exception {
        assertAST("#expand<parent_id=<this.index>parent_id>(phrase_field:beer)",
                "QueryTree\n" +
                        "   Or\n" +
                        "      Expansion\n" +
                        "         parent_id=<db.schema.table.index>parent_id\n" +
                        "         Word (fieldname=phrase_field, operator=CONTAINS, value=beer, index=db.schema.table.index)\n" +
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
    public void testWithOperatorJSON() throws Exception {
        assertJson("nested.exact_field:(a with b with (c or d with e)) and nested2.exact_field:(a with b)",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [\n" +
                        "      {\n" +
                        "        \"nested\" : {\n" +
                        "          \"query\" : {\n" +
                        "            \"bool\" : {\n" +
                        "              \"must\" : [\n" +
                        "                {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : [\n" +
                        "                      {\n" +
                        "                        \"term\" : {\n" +
                        "                          \"nested.exact_field\" : {\n" +
                        "                            \"value\" : \"a\",\n" +
                        "                            \"boost\" : 1.0\n" +
                        "                          }\n" +
                        "                        }\n" +
                        "                      },\n" +
                        "                      {\n" +
                        "                        \"term\" : {\n" +
                        "                          \"nested.exact_field\" : {\n" +
                        "                            \"value\" : \"b\",\n" +
                        "                            \"boost\" : 1.0\n" +
                        "                          }\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    ],\n" +
                        "                    \"disable_coord\" : false,\n" +
                        "                    \"adjust_pure_negative\" : true,\n" +
                        "                    \"boost\" : 1.0\n" +
                        "                  }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"should\" : [\n" +
                        "                      {\n" +
                        "                        \"term\" : {\n" +
                        "                          \"nested.exact_field\" : {\n" +
                        "                            \"value\" : \"c\",\n" +
                        "                            \"boost\" : 1.0\n" +
                        "                          }\n" +
                        "                        }\n" +
                        "                      },\n" +
                        "                      {\n" +
                        "                        \"nested\" : {\n" +
                        "                          \"query\" : {\n" +
                        "                            \"bool\" : {\n" +
                        "                              \"must\" : [\n" +
                        "                                {\n" +
                        "                                  \"bool\" : {\n" +
                        "                                    \"must\" : [\n" +
                        "                                      {\n" +
                        "                                        \"term\" : {\n" +
                        "                                          \"nested.exact_field\" : {\n" +
                        "                                            \"value\" : \"d\",\n" +
                        "                                            \"boost\" : 1.0\n" +
                        "                                          }\n" +
                        "                                        }\n" +
                        "                                      },\n" +
                        "                                      {\n" +
                        "                                        \"term\" : {\n" +
                        "                                          \"nested.exact_field\" : {\n" +
                        "                                            \"value\" : \"e\",\n" +
                        "                                            \"boost\" : 1.0\n" +
                        "                                          }\n" +
                        "                                        }\n" +
                        "                                      }\n" +
                        "                                    ],\n" +
                        "                                    \"disable_coord\" : false,\n" +
                        "                                    \"adjust_pure_negative\" : true,\n" +
                        "                                    \"boost\" : 1.0\n" +
                        "                                  }\n" +
                        "                                }\n" +
                        "                              ],\n" +
                        "                              \"disable_coord\" : false,\n" +
                        "                              \"adjust_pure_negative\" : true,\n" +
                        "                              \"boost\" : 1.0\n" +
                        "                            }\n" +
                        "                          },\n" +
                        "                          \"path\" : \"nested\",\n" +
                        "                          \"ignore_unmapped\" : false,\n" +
                        "                          \"score_mode\" : \"avg\",\n" +
                        "                          \"boost\" : 1.0\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    ],\n" +
                        "                    \"disable_coord\" : false,\n" +
                        "                    \"adjust_pure_negative\" : true,\n" +
                        "                    \"boost\" : 1.0\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              ],\n" +
                        "              \"disable_coord\" : false,\n" +
                        "              \"adjust_pure_negative\" : true,\n" +
                        "              \"boost\" : 1.0\n" +
                        "            }\n" +
                        "          },\n" +
                        "          \"path\" : \"nested\",\n" +
                        "          \"ignore_unmapped\" : false,\n" +
                        "          \"score_mode\" : \"avg\",\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"nested\" : {\n" +
                        "          \"query\" : {\n" +
                        "            \"bool\" : {\n" +
                        "              \"must\" : [\n" +
                        "                {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : [\n" +
                        "                      {\n" +
                        "                        \"term\" : {\n" +
                        "                          \"nested2.exact_field\" : {\n" +
                        "                            \"value\" : \"a\",\n" +
                        "                            \"boost\" : 1.0\n" +
                        "                          }\n" +
                        "                        }\n" +
                        "                      },\n" +
                        "                      {\n" +
                        "                        \"term\" : {\n" +
                        "                          \"nested2.exact_field\" : {\n" +
                        "                            \"value\" : \"b\",\n" +
                        "                            \"boost\" : 1.0\n" +
                        "                          }\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    ],\n" +
                        "                    \"disable_coord\" : false,\n" +
                        "                    \"adjust_pure_negative\" : true,\n" +
                        "                    \"boost\" : 1.0\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              ],\n" +
                        "              \"disable_coord\" : false,\n" +
                        "              \"adjust_pure_negative\" : true,\n" +
                        "              \"boost\" : 1.0\n" +
                        "            }\n" +
                        "          },\n" +
                        "          \"path\" : \"nested2\",\n" +
                        "          \"ignore_unmapped\" : false,\n" +
                        "          \"score_mode\" : \"avg\",\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"disable_coord\" : false,\n" +
                        "    \"adjust_pure_negative\" : true,\n" +
                        "    \"boost\" : 1.0\n" +
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
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"term\" : {\n" +
                        "                \"details.state\" : \"nc\"\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"details.state\" : \"sc\"\n" +
                        "              }\n" +
                        "            } ]\n" +
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
                        "    \"should\" : [\n" +
                        "      {\n" +
                        "        \"nested\" : {\n" +
                        "          \"query\" : {\n" +
                        "            \"bool\" : {\n" +
                        "              \"must\" : [\n" +
                        "                {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"internal_data.assigned_reviewers\" : {\n" +
                        "                      \"value\" : \"j_weber\",\n" +
                        "                      \"boost\" : 1.0\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                  \"terms\" : {\n" +
                        "                    \"internal_data.internal_set_tag_id\" : [\n" +
                        "                      369,\n" +
                        "                      370,\n" +
                        "                      371,\n" +
                        "                      298,\n" +
                        "                      367,\n" +
                        "                      295,\n" +
                        "                      296\n" +
                        "                    ],\n" +
                        "                    \"boost\" : 1.0\n" +
                        "                  }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"internal_data.state_id\" : {\n" +
                        "                      \"value\" : 4424,\n" +
                        "                      \"boost\" : 1.0\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                  \"terms\" : {\n" +
                        "                    \"internal_data.status_name\" : [\n" +
                        "                      \"internal_checked_out\",\n" +
                        "                      \"internal_ready\"\n" +
                        "                    ],\n" +
                        "                    \"boost\" : 1.0\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              ],\n" +
                        "              \"disable_coord\" : false,\n" +
                        "              \"adjust_pure_negative\" : true,\n" +
                        "              \"boost\" : 1.0\n" +
                        "            }\n" +
                        "          },\n" +
                        "          \"path\" : \"internal_data\",\n" +
                        "          \"ignore_unmapped\" : false,\n" +
                        "          \"score_mode\" : \"avg\",\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"nested\" : {\n" +
                        "          \"query\" : {\n" +
                        "            \"bool\" : {\n" +
                        "              \"must\" : [\n" +
                        "                {\n" +
                        "                  \"terms\" : {\n" +
                        "                    \"internal_data.internal_set_tag_id\" : [\n" +
                        "                      369,\n" +
                        "                      370,\n" +
                        "                      371,\n" +
                        "                      298,\n" +
                        "                      367,\n" +
                        "                      295,\n" +
                        "                      296\n" +
                        "                    ],\n" +
                        "                    \"boost\" : 1.0\n" +
                        "                  }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"internal_data.owner_username\" : {\n" +
                        "                      \"value\" : \"j_weber\",\n" +
                        "                      \"boost\" : 1.0\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"internal_data.state_id\" : {\n" +
                        "                      \"value\" : 4424,\n" +
                        "                      \"boost\" : 1.0\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                  \"terms\" : {\n" +
                        "                    \"internal_data.status_name\" : [\n" +
                        "                      \"internal_ready\",\n" +
                        "                      \"internal_updated\",\n" +
                        "                      \"internal_checked_out\",\n" +
                        "                      \"exception\"\n" +
                        "                    ],\n" +
                        "                    \"boost\" : 1.0\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              ],\n" +
                        "              \"disable_coord\" : false,\n" +
                        "              \"adjust_pure_negative\" : true,\n" +
                        "              \"boost\" : 1.0\n" +
                        "            }\n" +
                        "          },\n" +
                        "          \"path\" : \"internal_data\",\n" +
                        "          \"ignore_unmapped\" : false,\n" +
                        "          \"score_mode\" : \"avg\",\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"disable_coord\" : false,\n" +
                        "    \"adjust_pure_negative\" : true,\n" +
                        "    \"boost\" : 1.0\n" +
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
                        "    \"must\" : [\n" +
                        "      {\n" +
                        "        \"bool\" : {\n" +
                        "          \"must\" : [\n" +
                        "            {\n" +
                        "              \"wildcard\" : {\n" +
                        "                \"exact_field\" : {\n" +
                        "                  \"wildcard\" : \"phrase with *wildcard*\",\n" +
                        "                  \"boost\" : 1.0\n" +
                        "                }\n" +
                        "              }\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"bool\" : {\n" +
                        "                \"must\" : [\n" +
                        "                  {\n" +
                        "                    \"term\" : {\n" +
                        "                      \"exact_field\" : {\n" +
                        "                        \"value\" : \"literal_term\",\n" +
                        "                        \"boost\" : 1.0\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  },\n" +
                        "                  {\n" +
                        "                    \"term\" : {\n" +
                        "                      \"exact_field\" : {\n" +
                        "                        \"value\" : \"quoted_term\",\n" +
                        "                        \"boost\" : 1.0\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  },\n" +
                        "                  {\n" +
                        "                    \"term\" : {\n" +
                        "                      \"exact_field\" : {\n" +
                        "                        \"value\" : \"phrase value\",\n" +
                        "                        \"boost\" : 1.0\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                ],\n" +
                        "                \"disable_coord\" : false,\n" +
                        "                \"adjust_pure_negative\" : true,\n" +
                        "                \"boost\" : 1.0\n" +
                        "              }\n" +
                        "            }\n" +
                        "          ],\n" +
                        "          \"disable_coord\" : false,\n" +
                        "          \"adjust_pure_negative\" : true,\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"prefix\" : {\n" +
                        "          \"extact_field\" : {\n" +
                        "            \"value\" : \"prefix\",\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"wildcard\" : {\n" +
                        "          \"exact_field\" : {\n" +
                        "            \"wildcard\" : \"*wild*card*\",\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"fuzzy\" : {\n" +
                        "          \"exact_field\" : {\n" +
                        "            \"value\" : \"fuzzy\",\n" +
                        "            \"fuzziness\" : \"AUTO\",\n" +
                        "            \"prefix_length\" : 3,\n" +
                        "            \"max_expansions\" : 50,\n" +
                        "            \"transpositions\" : false,\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"bool\" : {\n" +
                        "          \"must\" : [\n" +
                        "            {\n" +
                        "              \"term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"literal_term\",\n" +
                        "                  \"boost\" : 1.0\n" +
                        "                }\n" +
                        "              }\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"quoted_term\",\n" +
                        "                  \"boost\" : 1.0\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          ],\n" +
                        "          \"disable_coord\" : false,\n" +
                        "          \"adjust_pure_negative\" : true,\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"prefix\" : {\n" +
                        "          \"extact_field\" : {\n" +
                        "            \"value\" : \"prefix\",\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"wildcard\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"wildcard\" : \"*wild*card*\",\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"fuzzy\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"value\" : \"fuzzy\",\n" +
                        "            \"fuzziness\" : \"AUTO\",\n" +
                        "            \"prefix_length\" : 3,\n" +
                        "            \"max_expansions\" : 50,\n" +
                        "            \"transpositions\" : false,\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"match_phrase\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"query\" : \"phrase value\",\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [\n" +
                        "            {\n" +
                        "              \"span_term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"phrase\",\n" +
                        "                  \"boost\" : 1.0\n" +
                        "                }\n" +
                        "              }\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"span_term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"with\",\n" +
                        "                  \"boost\" : 1.0\n" +
                        "                }\n" +
                        "              }\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"span_multi\" : {\n" +
                        "                \"match\" : {\n" +
                        "                  \"wildcard\" : {\n" +
                        "                    \"phrase_field\" : {\n" +
                        "                      \"wildcard\" : \"*wildcard*\",\n" +
                        "                      \"boost\" : 1.0\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                },\n" +
                        "                \"boost\" : 1.0\n" +
                        "              }\n" +
                        "            }\n" +
                        "          ],\n" +
                        "          \"slop\" : 0,\n" +
                        "          \"in_order\" : true,\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"disable_coord\" : false,\n" +
                        "    \"adjust_pure_negative\" : true,\n" +
                        "    \"boost\" : 1.0\n" +
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
                        "    \"must\" : [\n" +
                        "      {\n" +
                        "        \"term\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"value\" : \"term\",\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"term\" : {\n" +
                        "          \"field\" : {\n" +
                        "            \"value\" : \"term\",\n" +
                        "            \"boost\" : 2.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [\n" +
                        "            {\n" +
                        "              \"span_term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"a\",\n" +
                        "                  \"boost\" : 3.0\n" +
                        "                }\n" +
                        "              }\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"span_term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"b\",\n" +
                        "                  \"boost\" : 4.0\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          ],\n" +
                        "          \"slop\" : 2,\n" +
                        "          \"in_order\" : false,\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"match_phrase\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"query\" : \"some phrase\",\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"boost\" : 5.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"fuzzy\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"value\" : \"fuzzy\",\n" +
                        "            \"fuzziness\" : \"AUTO\",\n" +
                        "            \"prefix_length\" : 3,\n" +
                        "            \"max_expansions\" : 50,\n" +
                        "            \"transpositions\" : false,\n" +
                        "            \"boost\" : 6.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"prefix\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"value\" : \"wildcard\",\n" +
                        "            \"boost\" : 7.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"disable_coord\" : false,\n" +
                        "    \"adjust_pure_negative\" : true,\n" +
                        "    \"boost\" : 1.0\n" +
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
                        "    \"exact_field\" : \"\\\\\\\\begings with four backslashes\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testEscapingAsPrefix() throws Exception {
        assertJson("exact_field:'This is a prefix query ending in four backslashes\\\\\\\\*'",
                "{\n" +
                        "  \"prefix\" : {\n" +
                        "    \"exact_field\" : \"this is a prefix query ending in four backslashes\\\\\\\\\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testEscapingAsWildcard() throws Exception {
        assertJson("exact_field:'This is a wildcard query ending in four backslashes\\\\\\\\?'",
                "{\n" +
                        "  \"wildcard\" : {\n" +
                        "    \"exact_field\" : \"this is a wildcard query ending in four backslashes\\\\\\\\?\"\n" +
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

        ASTQueryTree tree = new QueryParser(new StringReader("#field_lists(field1=[a,b,c], field2=[d,e,f])")).parse(new IndexMetadataManager(client(), DEFAULT_INDEX_NAME), true);

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
                        "  \"match_phrase\" : {\n" +
                        "    \"phrase_field\" : {\n" +
                        "      \"query\" : \"物質の総称である。\",\n" +
                        "      \"slop\" : 0,\n" +
                        "      \"boost\" : 1.0\n" +
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
                        "      Array (fieldname=exact_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
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
                        "    \"must\" : [\n" +
                        "      {\n" +
                        "        \"match_phrase\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"query\" : \"asdflkj234-132asdfuj\",\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"match_phrase\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"query\" : \"asiodfja;sdf\",\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"bool\" : {\n" +
                        "          \"must\" : [\n" +
                        "            {\n" +
                        "              \"term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"487adqerydfskf0230\",\n" +
                        "                  \"boost\" : 1.0\n" +
                        "                }\n" +
                        "              }\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"term\" : {\n" +
                        "                \"phrase_field\" : {\n" +
                        "                  \"value\" : \"23\",\n" +
                        "                  \"boost\" : 1.0\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          ],\n" +
                        "          \"disable_coord\" : false,\n" +
                        "          \"adjust_pure_negative\" : true,\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"disable_coord\" : false,\n" +
                        "    \"adjust_pure_negative\" : true,\n" +
                        "    \"boost\" : 1.0\n" +
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
                        "  \"constant_score\" : {\n" +
                        "    \"filter\" : {\n" +
                        "      \"terms\" : {\n" +
                        "        \"exact_field\" : [ \"a\", \"b\", \"c\", \"d\" ]\n" +
                        "      }\n" +
                        "    }\n" +
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
                        "         NotNull (fieldname=phrase_field, operator=CONTAINS, value=*, index=db.schema.table.index)\n" +
                        "         Word (fieldname=phrase_field, operator=CONTAINS, value=non, index=db.schema.table.index)\n" +
                        "         NotNull (fieldname=phrase_field, operator=CONTAINS, value=*, index=db.schema.table.index)\n" +
                        "         Word (fieldname=phrase_field, operator=CONTAINS, value=programmers, index=db.schema.table.index)"
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
                        "  \"match_phrase\" : {\n" +
                        "    \"czech_field\" : {\n" +
                        "      \"query\" : \"toto je test\",\n" +
                        "      \"slop\" : 0,\n" +
                        "      \"boost\" : 1.0\n" +
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

        assertEquals("[{\"term\":\"getting\",\"startOffset\":0,\"endOffset\":7,\"position\":1,\"positionLength\":0,\"attributes\":null,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"null\\\"\",\"fragment\":false},{\"term\":\"non\",\"startOffset\":8,\"endOffset\":11,\"position\":2,\"positionLength\":0,\"attributes\":null,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"null\\\"\",\"fragment\":false},{\"term\":\"programmers\",\"startOffset\":12,\"endOffset\":23,\"position\":3,\"positionLength\":0,\"attributes\":null,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"null\\\"\",\"fragment\":false}]",
                Utils.objectToJson(highlights));
    }

    @Test
    public void testIssue87() throws Exception {
        Map<String, Object> data = new HashMap<>();


        data.put("phrase_field", "getting non-programmers to understand the development process");

        for (String s : new String[]{"~", ":", "*", "?",
                "!", "%", "&", "(", ")", ",",
                "<", "=", ">", "[", "]", "^", "@", "#"}) {
            DocumentHighlighter highlighter;
            List<AnalyzedField.Token> highlights;

            highlighter = new DocumentHighlighter(client(),
                    DEFAULT_INDEX_NAME,
                    "id",
                    data,
                    "phrase_field:'" + s + "getting'");
            highlights = highlighter.highlight();
            sortHighlightTokens(highlights);

            assertEquals("[{\"term\":\"getting\",\"startOffset\":0,\"endOffset\":7,\"position\":1,\"positionLength\":0,\"attributes\":null,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"" + s + "getting\\\"\",\"fragment\":false}]",
                    Utils.objectToJson(highlights));
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

        assertEquals("[{\"term\":\"a\",\"startOffset\":0,\"endOffset\":1,\"position\":1,\"positionLength\":0,\"attributes\":null,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"a\\\"\",\"fragment\":false},{\"term\":\"b\",\"startOffset\":2,\"endOffset\":3,\"position\":2,\"positionLength\":0,\"attributes\":null,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"b\\\"\",\"fragment\":false},{\"term\":\"c\",\"startOffset\":4,\"endOffset\":5,\"position\":3,\"positionLength\":0,\"attributes\":null,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"c\\\"\",\"fragment\":false},{\"term\":\"d\",\"startOffset\":6,\"endOffset\":7,\"position\":4,\"positionLength\":0,\"attributes\":null,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"phrase_field CONTAINS \\\"d\\\"\",\"fragment\":false}]",
                Utils.objectToJson(highlights));
    }

    @Test
    public void testProximityHighlighting() throws Exception {
        Map<String, Object> data = new HashMap<>();

        DocumentHighlighter highlighter;
        List<AnalyzedField.Token> highlights;

        data.put("phrase_field", "attorneys have general blah blah blah blah networks");
        highlighter = new DocumentHighlighter(client(),
                DEFAULT_INDEX_NAME,
                "id",
                data,
                "( ((\"attorney*\" w/2 \"general\") w/50 \"network*\") )");
        highlights = highlighter.highlight();
        sortHighlightTokens(highlights);

        assertEquals("[{\"term\":\"attorneys\",\"startOffset\":0,\"endOffset\":9,\"position\":1,\"positionLength\":0,\"attributes\":null,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"_all CONTAINS \\\"null\\\"\",\"fragment\":false},{\"term\":\"general\",\"startOffset\":15,\"endOffset\":22,\"position\":3,\"positionLength\":0,\"attributes\":null,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"_all CONTAINS \\\"null\\\"\",\"fragment\":false},{\"term\":\"networks\",\"startOffset\":43,\"endOffset\":51,\"position\":8,\"positionLength\":0,\"attributes\":null,\"type\":\"<ALPHANUM>\",\"primaryKey\":null,\"fieldName\":\"phrase_field\",\"arrayIndex\":0,\"clause\":\"_all CONTAINS \\\"null\\\"\",\"fragment\":false}]",
                Utils.objectToJson(highlights));
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
        String q = "exact_field =[[\"12/31/1999\",\"2/3/1999\", \"12/31/2017\", \"UNKNOWN\", \"2/2/2017\"]]";

        assertJson(q,
                "{\n" +
                        "  \"constant_score\" : {\n" +
                        "    \"filter\" : {\n" +
                        "      \"terms\" : {\n" +
                        "        \"exact_field\" : [ \"12/31/1999\", \"2/3/1999\", \"12/31/2017\", \"unknown\", \"2/2/2017\" ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );

        assertAST(q,
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      ArrayData (fieldname=exact_field, operator=EQ, value=$0, index=db.schema.table.index)"
        );
    }

    @Test
    public void testIssue80_unanalyzedField() throws Exception {
        String q = "unanalyzed_field =[[\"12/31/1999\",\"2/3/1999\", \"12/31/2017\", \"UNKNOWN\", \"2/2/2017\"]]";
        assertJson(q,
                "{\n" +
                        "  \"constant_score\" : {\n" +
                        "    \"filter\" : {\n" +
                        "      \"terms\" : {\n" +
                        "        \"unanalyzed_field\" : [ \"12/31/1999\", \"2/3/1999\", \"12/31/2017\", \"UNKNOWN\", \"2/2/2017\" ]\n" +
                        "      }\n" +
                        "    }\n" +
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
                        "    \"should\" : [\n" +
                        "      {\n" +
                        "        \"match_phrase\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"query\" : \"This is a mIxEDcAsE PHRASE\",\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"match_phrase\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"query\" : \"12/31/1999\",\n" +
                        "            \"slop\" : 0,\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"terms\" : {\n" +
                        "          \"phrase_field\" : [\n" +
                        "            \"unknown\"\n" +
                        "          ],\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"disable_coord\" : false,\n" +
                        "    \"adjust_pure_negative\" : true,\n" +
                        "    \"boost\" : 1.0\n" +
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
                        "  \"constant_score\" : {\n" +
                        "    \"filter\" : {\n" +
                        "      \"terms\" : {\n" +
                        "        \"id\" : [ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ]\n" +
                        "      }\n" +
                        "    }\n" +
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
    public void testStopWordRemoval_allField_issue349() throws Exception {
        assertAST("english_field:(now is the time)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Array (fieldname=english_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "         Word (fieldname=english_field, operator=CONTAINS, value=now, index=db.schema.table.index)\n" +
                        "         Word (fieldname=english_field, operator=CONTAINS, value=time, index=db.schema.table.index)");
    }

    @Test
    public void testStopWordRemoval_allField() throws Exception {
        assertAST("(now is the time) OR english_field:(now is the time)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Or\n" +
                        "         And\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=now, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=now, index=db.schema.table.index)\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=is, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=is, index=db.schema.table.index)\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=the, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=the, index=db.schema.table.index)\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=time, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=time, index=db.schema.table.index)\n" +
                        "         Array (fieldname=english_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "            Word (fieldname=english_field, operator=CONTAINS, value=now, index=db.schema.table.index)\n" +
                        "            Word (fieldname=english_field, operator=CONTAINS, value=time, index=db.schema.table.index)"
        );
    }

    @Test
    public void testSingleQuestionMark_issue102() throws Exception {
        assertJson("exact_field:?",
                "{\n" +
                        "  \"exists\" : {\n" +
                        "    \"field\" : \"exact_field\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testMultipleQuestionMarks_issue102() throws Exception {
        assertJson("exact_field:????",
                "{\n" +
                        "  \"wildcard\" : {\n" +
                        "    \"exact_field\" : \"????\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testMultipleAsterisksMarks_issue102() throws Exception {
        assertJson("exact_field:****",
                "{\n" +
                        "  \"exists\" : {\n" +
                        "    \"field\" : \"exact_field\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testCombinationWildcardsMarks_issue102() throws Exception {
        assertJson("exact_field:?***",
                "{\n" +
                        "  \"wildcard\" : {\n" +
                        "    \"exact_field\" : \"?***\"\n" +
                        "  }\n" +
                        "}"
        );
        assertJson("exact_field:***?",
                "{\n" +
                        "  \"wildcard\" : {\n" +
                        "    \"exact_field\" : \"***?\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue105() throws Exception {
        assertJson("exact_field:((red or blue) w/3 (cat or dog))",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_or\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"exact_field\" : {\n" +
                        "              \"value\" : \"red\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"exact_field\" : {\n" +
                        "              \"value\" : \"blue\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_or\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"exact_field\" : {\n" +
                        "              \"value\" : \"cat\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"exact_field\" : {\n" +
                        "              \"value\" : \"dog\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 3,\n" +
                        "    \"in_order\" : false\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue105_complex() throws Exception {
        assertJson("phrase_field:((service*) w/2 (area*) w/10 (negotiat* OR (bargain* w/3 food) OR contract*) w/10 provider*)",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"prefix\" : {\n" +
                        "            \"phrase_field\" : \"service\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_near\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_multi\" : {\n" +
                        "            \"match\" : {\n" +
                        "              \"prefix\" : {\n" +
                        "                \"phrase_field\" : \"area\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_near\" : {\n" +
                        "            \"clauses\" : [ {\n" +
                        "              \"span_or\" : {\n" +
                        "                \"clauses\" : [ {\n" +
                        "                  \"span_multi\" : {\n" +
                        "                    \"match\" : {\n" +
                        "                      \"prefix\" : {\n" +
                        "                        \"phrase_field\" : \"negotiat\"\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"span_near\" : {\n" +
                        "                    \"clauses\" : [ {\n" +
                        "                      \"span_multi\" : {\n" +
                        "                        \"match\" : {\n" +
                        "                          \"prefix\" : {\n" +
                        "                            \"phrase_field\" : \"bargain\"\n" +
                        "                          }\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"span_term\" : {\n" +
                        "                        \"phrase_field\" : {\n" +
                        "                          \"value\" : \"food\"\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    } ],\n" +
                        "                    \"slop\" : 3,\n" +
                        "                    \"in_order\" : false\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"span_multi\" : {\n" +
                        "                    \"match\" : {\n" +
                        "                      \"prefix\" : {\n" +
                        "                        \"phrase_field\" : \"contract\"\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"span_multi\" : {\n" +
                        "                \"match\" : {\n" +
                        "                  \"prefix\" : {\n" +
                        "                    \"phrase_field\" : \"provider\"\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              }\n" +
                        "            } ],\n" +
                        "            \"slop\" : 10,\n" +
                        "            \"in_order\" : false\n" +
                        "          }\n" +
                        "        } ],\n" +
                        "        \"slop\" : 10,\n" +
                        "        \"in_order\" : false\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 2,\n" +
                        "    \"in_order\" : false\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue106() throws Exception {
        assertJson("( #expand<groupid=<this.index>groupid> ( field:value #filter(other_field:other_value and other_field:other_value2) ) )",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"should\" : [ {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"term\" : {\n" +
                        "            \"field\" : \"value\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"term\" : {\n" +
                        "                \"other_field\" : \"other_value\"\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"other_field\" : \"other_value2\"\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"field\" : \"value\"\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testExactPhrasesGetMerged() throws Exception {
        assertAST("( (( AND ( data_client_name = WELLMARK AND (exact_field = \"asdf, CATHI (sdfg)\" OR " +
                        "exact_field = \"sdfg, qwer\" OR exact_field = \"swergs, ersd\" OR exact_field = \"wergf, fsd\" OR " +
                        "exact_field = \"DHJ, hsdgf\" OR exact_field = \"dfbg, werfdvc\" OR exact_field = \"sdfg, wwwert\" OR " +
                        "exact_field = \"ersfd, KJHSA\" OR exact_field = \"AIUKSJD, kasdf\" OR exact_field = \"sdfg, werww\") AND " +
                        "data_date_combined_family <= \"2013-12-31\" AND data_duplicate_resource = NO AND " +
                        "(data_record_type = EMAIL OR data_record_type = \"EMAIL ATTACHMENT\" OR data_record_type = \"EMAIL ATTACHMENT OLE\") AND data_filter_universal = \"*\" AND data_moved_to: null ) ) ) )",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      And\n" +
                        "         Word (fieldname=data_client_name, operator=EQ, value=wellmark, index=db.schema.table.index)\n" +
                        "         Array (fieldname=exact_field, operator=EQ, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=exact_field, operator=EQ, value=asdf, cathi (sdfg), index=db.schema.table.index)\n" +
                        "            Word (fieldname=exact_field, operator=EQ, value=sdfg, qwer, index=db.schema.table.index)\n" +
                        "            Word (fieldname=exact_field, operator=EQ, value=swergs, ersd, index=db.schema.table.index)\n" +
                        "            Word (fieldname=exact_field, operator=EQ, value=wergf, fsd, index=db.schema.table.index)\n" +
                        "            Word (fieldname=exact_field, operator=EQ, value=dhj, hsdgf, index=db.schema.table.index)\n" +
                        "            Word (fieldname=exact_field, operator=EQ, value=dfbg, werfdvc, index=db.schema.table.index)\n" +
                        "            Word (fieldname=exact_field, operator=EQ, value=sdfg, wwwert, index=db.schema.table.index)\n" +
                        "            Word (fieldname=exact_field, operator=EQ, value=ersfd, kjhsa, index=db.schema.table.index)\n" +
                        "            Word (fieldname=exact_field, operator=EQ, value=aiuksjd, kasdf, index=db.schema.table.index)\n" +
                        "            Word (fieldname=exact_field, operator=EQ, value=sdfg, werww, index=db.schema.table.index)\n" +
                        "         Word (fieldname=data_date_combined_family, operator=LTE, value=2013-12-31, index=db.schema.table.index)\n" +
                        "         Word (fieldname=data_duplicate_resource, operator=EQ, value=no, index=db.schema.table.index)\n" +
                        "         Array (fieldname=data_record_type, operator=EQ, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=data_record_type, operator=EQ, value=email, index=db.schema.table.index)\n" +
                        "            Word (fieldname=data_record_type, operator=EQ, value=email attachment, index=db.schema.table.index)\n" +
                        "            Word (fieldname=data_record_type, operator=EQ, value=email attachment ole, index=db.schema.table.index)\n" +
                        "         NotNull (fieldname=data_filter_universal, operator=EQ, index=db.schema.table.index)\n" +
                        "         Null (fieldname=data_moved_to, operator=CONTAINS, index=db.schema.table.index)"
        );
        assertJson("( (( AND ( data_client_name = WELLMARK AND (exact_field = \"asdf, CATHI (sdfg)\" OR " +
                        "exact_field = \"sdfg, qwer\" OR exact_field = \"swergs, ersd\" OR exact_field = \"wergf, fsd\" OR " +
                        "exact_field = \"DHJ, hsdgf\" OR exact_field = \"dfbg, werfdvc\" OR exact_field = \"sdfg, wwwert\" OR " +
                        "exact_field = \"ersfd, KJHSA\" OR exact_field = \"AIUKSJD, kasdf\" OR exact_field = \"sdfg, werww\") AND " +
                        "data_date_combined_family <= \"2013-12-31\" AND data_duplicate_resource = NO AND " +
                        "(data_record_type = EMAIL OR data_record_type = \"EMAIL ATTACHMENT\" OR data_record_type = \"EMAIL ATTACHMENT OLE\") AND data_filter_universal = \"*\" AND data_moved_to: null ) ) ) )",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [ {\n" +
                        "      \"term\" : {\n" +
                        "        \"data_client_name\" : \"wellmark\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"terms\" : {\n" +
                        "        \"exact_field\" : [ \"asdf, cathi (sdfg)\", \"sdfg, qwer\", \"swergs, ersd\", \"wergf, fsd\", \"dhj, hsdgf\", \"dfbg, werfdvc\", \"sdfg, wwwert\", \"ersfd, kjhsa\", \"aiuksjd, kasdf\", \"sdfg, werww\" ]\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"range\" : {\n" +
                        "        \"data_date_combined_family\" : {\n" +
                        "          \"from\" : null,\n" +
                        "          \"to\" : \"2013-12-31\",\n" +
                        "          \"include_lower\" : true,\n" +
                        "          \"include_upper\" : true\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"term\" : {\n" +
                        "        \"data_duplicate_resource\" : \"no\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"terms\" : {\n" +
                        "        \"data_record_type\" : [ \"email\", \"email attachment\", \"email attachment ole\" ]\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"exists\" : {\n" +
                        "        \"field\" : \"data_filter_universal\"\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must_not\" : {\n" +
                        "          \"exists\" : {\n" +
                        "            \"field\" : \"data_moved_to\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testProximityForIssue105_1() throws Exception {
        assertJson("fulltext:('lunch meeting' w/100 (food or drink*))",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"fulltext\" : {\n" +
                        "          \"value\" : \"lunch meeting\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_or\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"fulltext\" : {\n" +
                        "              \"value\" : \"food\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_multi\" : {\n" +
                        "            \"match\" : {\n" +
                        "              \"prefix\" : {\n" +
                        "                \"fulltext\" : \"drink\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 100,\n" +
                        "    \"in_order\" : false\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testProximityForIssue105_2() throws Exception {
        assertJson("fulltext:(term1 w/3 (term2 OR term3)) w/10 (term or list)",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_near\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"fulltext\" : {\n" +
                        "              \"value\" : \"term1\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_or\" : {\n" +
                        "            \"clauses\" : [ {\n" +
                        "              \"span_term\" : {\n" +
                        "                \"fulltext\" : {\n" +
                        "                  \"value\" : \"term2\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"span_term\" : {\n" +
                        "                \"fulltext\" : {\n" +
                        "                  \"value\" : \"term3\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        } ],\n" +
                        "        \"slop\" : 3,\n" +
                        "        \"in_order\" : false\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_or\" : {\n" +
                        "        \"clauses\" : [ {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"fulltext\" : {\n" +
                        "              \"value\" : \"term\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"fulltext\" : {\n" +
                        "              \"value\" : \"term\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"fulltext\" : {\n" +
                        "              \"value\" : \"list\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"span_term\" : {\n" +
                        "            \"fulltext\" : {\n" +
                        "              \"value\" : \"list\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    } ],\n" +
                        "    \"slop\" : 10,\n" +
                        "    \"in_order\" : false\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testGIANTNumber_Issue116() throws Exception {
        assertJson("exact_field:90130715133114369814655",
                "{\n" +
                        "  \"term\" : {\n" +
                        "    \"exact_field\" : \"90130715133114369814655\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testRewritingWildcardsWithShingles_Prefix() throws Exception {
        assertJson("shingle_field:the*",
                "{\n" +
                        "  \"regexp\" : {\n" +
                        "    \"shingle_field\" : {\n" +
                        "      \"value\" : \"the[^$]*\",\n" +
                        "      \"flags_value\" : 65535\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testRewritingWildcardsWithShingles_STAR_MIDDLE() throws Exception {
        assertJson("shingle_field:t*he",
                "{\n" +
                        "  \"regexp\" : {\n" +
                        "    \"shingle_field\" : {\n" +
                        "      \"value\" : \"t[^$]*he\",\n" +
                        "      \"flags_value\" : 65535\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testRewritingWildcardsWithShingles_STAR_MIDDLE_END() throws Exception {
        assertJson("shingle_field:t*he*",
                "{\n" +
                        "  \"regexp\" : {\n" +
                        "    \"shingle_field\" : {\n" +
                        "      \"value\" : \"t[^$]*he[^$]*\",\n" +
                        "      \"flags_value\" : 65535\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testRewritingWildcardsWithShingles_QUESTION_END() throws Exception {
        assertJson("shingle_field:the?",
                "{\n" +
                        "  \"regexp\" : {\n" +
                        "    \"shingle_field\" : {\n" +
                        "      \"value\" : \"the[^$]\",\n" +
                        "      \"flags_value\" : 65535\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testRewritingWildcardsWithShingles_QUESTION_MIDDLE_END() throws Exception {
        assertJson("shingle_field:t?he?",
                "{\n" +
                        "  \"regexp\" : {\n" +
                        "    \"shingle_field\" : {\n" +
                        "      \"value\" : \"t[^$]he[^$]\",\n" +
                        "      \"flags_value\" : 65535\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testRewritingWildcardsWithShingles_Proximity() throws Exception {
        assertJson("shingle_field:(the* w/3 winner)",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"regexp\" : {\n" +
                        "            \"shingle_field\" : {\n" +
                        "              \"value\" : \"the[^$]*\",\n" +
                        "              \"flags_value\" : 65535\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_term\" : {\n" +
                        "        \"shingle_field\" : {\n" +
                        "          \"value\" : \"winner\"\n" +
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
    public void testRewritingWildcardsWithShingles_ProximityPhrase() throws Exception {
        assertJson("shingle_field:'the* winner'",
                "{\n" +
                        "  \"wildcard\" : {\n" +
                        "    \"shingle_field\" : \"the*$winner\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testRewritingWildcardsWithShingles_WildcardOnly() throws Exception {
        assertJson("shingle_field:*",
                "{\n" +
                        "  \"exists\" : {\n" +
                        "    \"field\" : \"shingle_field\"\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testRewritingWildcardsWithShingles_NE_WildcardOnly() throws Exception {
        assertJson("shingle_field<>*",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must_not\" : {\n" +
                        "      \"exists\" : {\n" +
                        "        \"field\" : \"shingle_field\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testRewritingWildcardsWithShingles_NE_ProximityPhrase() throws Exception {
        assertJson("shingle_field<>'the* winner'",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must_not\" : {\n" +
                        "      \"wildcard\" : {\n" +
                        "        \"shingle_field\" : \"the*$winner\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        );
    }


    @Test
    public void testExpansionWithNamedIndexLink() throws Exception {
        assertAST("#options(other:(left=<table.index>right)) food",
                "QueryTree\n" +
                        "   Options\n" +
                        "      other:(left=<db.schema.table.index>right)\n" +
                        "         LeftField (value=left)\n" +
                        "         IndexName (value=db.schema.table.index)\n" +
                        "         RightField (value=right)\n" +
                        "   Or\n" +
                        "      Expansion\n" +
                        "         id=<db.schema.table.index>id\n" +
                        "         Or\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=food, index=db.schema.table.index)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=food, index=db.schema.table.index)\n" +
                        "      Expansion\n" +
                        "         other:(left=<db.schema.table.index>right)\n" +
                        "            LeftField (value=left)\n" +
                        "            IndexName (value=db.schema.table.index)\n" +
                        "            RightField (value=right)\n" +
                        "         Or\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=food, index=db.schema.table.index)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=food, index=db.schema.table.index)");
    }

    @Test
    public void testRegexProximityWithAPhrase() throws Exception {
        assertJson("phrase_field:~\"a.*\" w/3 phrase_field:~\"b.* \"",
                "{\n" +
                        "  \"span_near\" : {\n" +
                        "    \"clauses\" : [ {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"regexp\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"a.*\",\n" +
                        "              \"flags_value\" : 65535\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"span_multi\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"regexp\" : {\n" +
                        "            \"phrase_field\" : {\n" +
                        "              \"value\" : \"b.*\",\n" +
                        "              \"flags_value\" : 65535\n" +
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
    public void testComplexTokenPulloutWithAND_issue349() throws Exception {
        assertAST("english_field:darling",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Word (fieldname=english_field, operator=CONTAINS, value=darl, index=db.schema.table.index)");
    }

    @Test
    public void testComplexTokenPulloutWithAND() throws Exception {
        assertAST("english_field:(\"I''ll see you later\" and darling)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      And\n" +
                        "         Phrase (fieldname=english_field, operator=CONTAINS, value=I''ll see you later, index=db.schema.table.index)\n" +
                        "         Word (fieldname=english_field, operator=CONTAINS, value=darl, index=db.schema.table.index)");
    }

    @Test
    public void testIssue35() throws Exception {
        QueryRewriter qr;

        qr = qr("#tally(field, \"^.*\", 5000, \"term\", 50)");
        assertEquals(
                "\"field\"{\"terms\":{\"field\":\"field\",\"size\":5000,\"shard_size\":50,\"min_doc_count\":1,\"shard_min_doc_count\":0,\"show_term_doc_count_error\":false,\"order\":{\"_term\":\"asc\"}}}",
                qr.rewriteAggregations().toXContent(JsonXContent.contentBuilder(), null).string()
        );

        qr = qr("#tally(field, \"^.*\", 5000, \"term\", 50, #tally(field, \"^.*\", 5000, \"term\"))");
        assertEquals(
                "\"field\"{\"terms\":{\"field\":\"field\",\"size\":5000,\"shard_size\":50,\"min_doc_count\":1,\"shard_min_doc_count\":0,\"show_term_doc_count_error\":false,\"order\":{\"_term\":\"asc\"}},\"aggregations\":{\"field\":{\"terms\":{\"field\":\"field\",\"size\":5000,\"shard_size\":2147483647,\"min_doc_count\":1,\"shard_min_doc_count\":0,\"show_term_doc_count_error\":false,\"order\":{\"_term\":\"asc\"}}}}}",
                qr.rewriteAggregations().toXContent(JsonXContent.contentBuilder(), null).string()
        );
    }

    @Test
    public void testMaxTermsZeroExpandsToIntegerMaxValue() throws Exception {
        QueryRewriter qr;

        qr = qr("#tally(field, \"^.*\", 0, \"term\", 50)");
        assertEquals(
                "\"field\"{\"terms\":{\"field\":\"field\",\"size\":" + Integer.MAX_VALUE + ",\"shard_size\":50,\"min_doc_count\":1,\"shard_min_doc_count\":0,\"show_term_doc_count_error\":false,\"order\":{\"_term\":\"asc\"}}}",
                qr.rewriteAggregations().toXContent(JsonXContent.contentBuilder(), null).string()
        );
    }

    @Test
    public void testIssue132() throws Exception {
        assertAST("#expand<group_id=<this.index>group_id>(#expand<group_id=<this.index>group_id>(pk_id:3 OR pk_id:5))",
                "QueryTree\n" +
                        "   Or\n" +
                        "      Expansion\n" +
                        "         group_id=<db.schema.table.index>group_id\n" +
                        "         Or\n" +
                        "            Array (fieldname=pk_id, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "               Number (fieldname=pk_id, operator=CONTAINS, value=3, index=db.schema.table.index)\n" +
                        "               Number (fieldname=pk_id, operator=CONTAINS, value=5, index=db.schema.table.index)\n" +
                        "            Expansion\n" +
                        "               group_id=<db.schema.table.index>group_id\n" +
                        "               Array (fieldname=pk_id, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "                  Number (fieldname=pk_id, operator=CONTAINS, value=3, index=db.schema.table.index)\n" +
                        "                  Number (fieldname=pk_id, operator=CONTAINS, value=5, index=db.schema.table.index)\n" +
                        "            Expansion\n" +
                        "               id=<db.schema.table.index>id\n" +
                        "               Array (fieldname=pk_id, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "                  Number (fieldname=pk_id, operator=CONTAINS, value=3, index=db.schema.table.index)\n" +
                        "                  Number (fieldname=pk_id, operator=CONTAINS, value=5, index=db.schema.table.index)\n" +
                        "      Expansion\n" +
                        "         id=<db.schema.table.index>id\n" +
                        "         Array (fieldname=pk_id, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "            Number (fieldname=pk_id, operator=CONTAINS, value=3, index=db.schema.table.index)\n" +
                        "            Number (fieldname=pk_id, operator=CONTAINS, value=5, index=db.schema.table.index)"
        );
    }

    @Test
    public void testIssue143_ASTParsing() throws Exception {
        assertAST(
                "subject:(beer or wine and cheese) and ({" +
                        "\"match_all\":{}" +
                        "}) not subject:pickles",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      And\n" +
                        "         Or\n" +
                        "            Word (fieldname=subject, operator=CONTAINS, value=beer, index=db.schema.table.index)\n" +
                        "            Array (fieldname=subject, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "               Word (fieldname=subject, operator=CONTAINS, value=wine, index=db.schema.table.index)\n" +
                        "               Word (fieldname=subject, operator=CONTAINS, value=cheese, index=db.schema.table.index)\n" +
                        "         JsonQuery (value={\"match_all\":{}})\n" +
                        "         Not\n" +
                        "            Word (fieldname=subject, operator=CONTAINS, value=pickles, index=db.schema.table.index)"
        );
    }

    @Test
    public void testMultipleEmbeddedJsonQueryDSL() throws Exception {
        assertAST(
                "({\"term\":{\"_all\":\"java\"}}) or ({ \"term\":{\"_all\":\"joe\"} })",
                "QueryTree\n" +
                        "   Or\n" +
                        "      JsonQuery (value={\"term\":{\"_all\":\"java\"}})\n" +
                        "      JsonQuery (value={ \"term\":{\"_all\":\"joe\"} })"
        );
    }

    @Test
    public void testIssue306_Json() throws Exception {
        assertJson(
                "({\"simple_query_string\":{\"query\":\"(java)\", \"fields\": [\"title\"]}})",
          "{\n" +
                        "  \"simple_query_string\" : {\n" +
                        "    \"query\" : \"(java)\",\n" +
                        "    \"fields\" : [\n" +
                        "      \"title^1.0\"\n" +
                        "    ],\n" +
                        "    \"flags\" : -1,\n" +
                        "    \"default_operator\" : \"or\",\n" +
                        "    \"lenient\" : false,\n" +
                        "    \"analyze_wildcard\" : false,\n" +
                        "    \"boost\" : 1.0\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue143_Json() throws Exception {
        assertJson(
                "subject:(beer or wine and cheese) and ({" +
                        "\"term\":{\"some_field\": \"some_value\"}" +
                        "}) not subject:pickles",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"must\" : [\n" +
                        "      {\n" +
                        "        \"bool\" : {\n" +
                        "          \"should\" : [\n" +
                        "            {\n" +
                        "              \"term\" : {\n" +
                        "                \"subject\" : {\n" +
                        "                  \"value\" : \"beer\",\n" +
                        "                  \"boost\" : 1.0\n" +
                        "                }\n" +
                        "              }\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"bool\" : {\n" +
                        "                \"must\" : [\n" +
                        "                  {\n" +
                        "                    \"term\" : {\n" +
                        "                      \"subject\" : {\n" +
                        "                        \"value\" : \"wine\",\n" +
                        "                        \"boost\" : 1.0\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  },\n" +
                        "                  {\n" +
                        "                    \"term\" : {\n" +
                        "                      \"subject\" : {\n" +
                        "                        \"value\" : \"cheese\",\n" +
                        "                        \"boost\" : 1.0\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                ],\n" +
                        "                \"disable_coord\" : false,\n" +
                        "                \"adjust_pure_negative\" : true,\n" +
                        "                \"boost\" : 1.0\n" +
                        "              }\n" +
                        "            }\n" +
                        "          ],\n" +
                        "          \"disable_coord\" : false,\n" +
                        "          \"adjust_pure_negative\" : true,\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"term\" : {\n" +
                        "          \"some_field\" : {\n" +
                        "            \"value\" : \"some_value\",\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"bool\" : {\n" +
                        "          \"must_not\" : [\n" +
                        "            {\n" +
                        "              \"term\" : {\n" +
                        "                \"subject\" : {\n" +
                        "                  \"value\" : \"pickles\",\n" +
                        "                  \"boost\" : 1.0\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          ],\n" +
                        "          \"disable_coord\" : false,\n" +
                        "          \"adjust_pure_negative\" : true,\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"disable_coord\" : false,\n" +
                        "    \"adjust_pure_negative\" : true,\n" +
                        "    \"boost\" : 1.0\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue150() throws Exception {
        QueryRewriter qr = qr("#json_agg({\n" +
                "  \"top-tags\" : {\n" +
                "    \"terms\" : {\n" +
                "      \"field\" : \"tags\",\n" +
                "      \"size\" : 3\n" +
                "    },\n" +
                "    \"aggs\" : {\n" +
                "      \"top_tag_hits\" : {\n" +
                "        \"top_hits\" : {\n" +
                "          \"sort\" : [ {\n" +
                "            \"last_activity_date\" : {\n" +
                "              \"order\" : \"desc\"\n" +
                "            }\n" +
                "          } ],\n" +
                "          \"_source\" : {\n" +
                "            \"include\" : [ \"title\" ]\n" +
                "          },\n" +
                "          \"size\" : 1\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}) beer,wine,cheese");
        SearchRequestBuilder builder = SearchAction.INSTANCE.newRequestBuilder(client());
        builder.setQuery(qr.rewriteQuery());
        builder.addAggregation(qr.rewriteAggregations());

        assertEquals(
                "{\n" +
                        "  \"query\" : {\n" +
                        "    \"bool\" : {\n" +
                        "      \"should\" : [\n" +
                        "        {\n" +
                        "          \"terms\" : {\n" +
                        "            \"fulltext_field\" : [\n" +
                        "              \"beer\",\n" +
                        "              \"wine\",\n" +
                        "              \"cheese\"\n" +
                        "            ],\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        },\n" +
                        "        {\n" +
                        "          \"terms\" : {\n" +
                        "            \"_all\" : [\n" +
                        "              \"beer\",\n" +
                        "              \"wine\",\n" +
                        "              \"cheese\"\n" +
                        "            ],\n" +
                        "            \"boost\" : 1.0\n" +
                        "          }\n" +
                        "        }\n" +
                        "      ],\n" +
                        "      \"disable_coord\" : false,\n" +
                        "      \"adjust_pure_negative\" : true,\n" +
                        "      \"boost\" : 1.0\n" +
                        "    }\n" +
                        "  },\n" +
                        "  \"aggregations\" : {\n" +
                        "    \"top-tags\" : {\n" +
                        "      \"terms\" : {\n" +
                        "        \"field\" : \"tags\",\n" +
                        "        \"size\" : 3,\n" +
                        "        \"min_doc_count\" : 1,\n" +
                        "        \"shard_min_doc_count\" : 0,\n" +
                        "        \"show_term_doc_count_error\" : false,\n" +
                        "        \"order\" : [\n" +
                        "          {\n" +
                        "            \"_count\" : \"desc\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"_term\" : \"asc\"\n" +
                        "          }\n" +
                        "        ]\n" +
                        "      },\n" +
                        "      \"aggregations\" : {\n" +
                        "        \"top_tag_hits\" : {\n" +
                        "          \"top_hits\" : {\n" +
                        "            \"from\" : 0,\n" +
                        "            \"size\" : 1,\n" +
                        "            \"version\" : false,\n" +
                        "            \"explain\" : false,\n" +
                        "            \"_source\" : {\n" +
                        "              \"includes\" : [\n" +
                        "                \"title\"\n" +
                        "              ],\n" +
                        "              \"excludes\" : [ ]\n" +
                        "            },\n" +
                        "            \"sort\" : [\n" +
                        "              {\n" +
                        "                \"last_activity_date\" : {\n" +
                        "                  \"order\" : \"desc\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            ]\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                builder.toString().replaceAll("\r", "")
        );
    }

    @Test
    public void testIssue175() throws Exception {
        assertAST("#options(other:(left=<so_users.idxso_users>right)) food beer",
                "QueryTree\n" +
                        "   Options\n" +
                        "      other:(left=<db.schema.so_users.idxso_users>right)\n" +
                        "         LeftField (value=left)\n" +
                        "         IndexName (value=db.schema.so_users.idxso_users)\n" +
                        "         RightField (value=right)\n" +
                        "   And\n" +
                        "      Or\n" +
                        "         Expansion\n" +
                        "            id=<db.schema.table.index>id\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=food, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=food, index=db.schema.table.index)\n" +
                        "         Expansion\n" +
                        "            other:(left=<db.schema.so_users.idxso_users>right)\n" +
                        "               LeftField (value=left)\n" +
                        "               IndexName (value=db.schema.so_users.idxso_users)\n" +
                        "               RightField (value=right)\n" +
                        "            Or\n" +
                        "               Word (fieldname=body, operator=CONTAINS, value=food, index=db.schema.so_users.idxso_users)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=food, index=db.schema.so_users.idxso_users)\n" +
                        "      Or\n" +
                        "         Expansion\n" +
                        "            id=<db.schema.table.index>id\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=beer, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=beer, index=db.schema.table.index)\n" +
                        "         Expansion\n" +
                        "            other:(left=<db.schema.so_users.idxso_users>right)\n" +
                        "               LeftField (value=left)\n" +
                        "               IndexName (value=db.schema.so_users.idxso_users)\n" +
                        "               RightField (value=right)\n" +
                        "            Or\n" +
                        "               Word (fieldname=body, operator=CONTAINS, value=beer, index=db.schema.so_users.idxso_users)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=beer, index=db.schema.so_users.idxso_users)"
        );
    }

    @Test
    public void testIssue175InExpand() throws Exception {
        assertAST("#expand<field=<this.index>field>(john doe)",
                "QueryTree\n" +
                        "   Or\n" +
                        "      Expansion\n" +
                        "         field=<db.schema.table.index>field\n" +
                        "         And\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=john, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=john, index=db.schema.table.index)\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=doe, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=doe, index=db.schema.table.index)\n" +
                        "      Expansion\n" +
                        "         id=<db.schema.table.index>id\n" +
                        "         And\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=john, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=john, index=db.schema.table.index)\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=doe, index=db.schema.table.index)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=doe, index=db.schema.table.index)"
        );
    }

    @Test
    public void testIssue175WithProximity() throws Exception {
        assertAST("john w/12 doe",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Or\n" +
                        "         Proximity (fieldname=fulltext_field, operator=CONTAINS, distance=12, ordered=false, index=db.schema.table.index)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=john, index=db.schema.table.index)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=doe, index=db.schema.table.index)\n" +
                        "         Proximity (fieldname=_all, operator=CONTAINS, distance=12, ordered=false, index=db.schema.table.index)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=john, index=db.schema.table.index)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=doe, index=db.schema.table.index)"
        );
    }

    @Test
    public void testIssue183() throws Exception {
        assertSameJson("id<>[1,2,3,4,5]", "id<>[[1,2,3,4,5]]");
        assertSameJson("id=[1,2,3,4,5]", "id=[[1,2,3,4,5]]");
        assertSameJson("id=['1','2','3','4','5']", "id=[['1','2','3','4','5']]");
        assertSameJson("id=['1','2','3','4','5']", "id=[[1,2,3,4,5]]");
        assertSameJson("id=[1,2,3,4,5]", "id=[['1','2','3','4','5']]");

        assertDifferentJson("exact_field:[a,b,c]", "exact_field:[['a','b','c']]");
        assertDifferentJson("exact_field:[1,2,3,4,5]", "exact_field:[['1','2','3','4','5']]");
    }

    @Test
    public void testIssue195() throws Exception {
        Map<String, Object> data = new HashMap<>();

        DocumentHighlighter highlighter;
        List<AnalyzedField.Token> highlights;

        data.put("exact_field", "the birds and the bees");
        highlighter = new DocumentHighlighter(client(),
                DEFAULT_INDEX_NAME,
                "id",
                data,
                "exact_field:'the bees the birds'");
        highlights = highlighter.highlight();
        sortHighlightTokens(highlights);

        assertEquals("[]",
                Utils.objectToJson(highlights));
    }

    @Test
    public void testIssue201() throws Exception {
        assertAST("exact_field:(this and and that)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Array (fieldname=exact_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=this, index=db.schema.table.index)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=that, index=db.schema.table.index)"
        );
        assertAST("exact_field:(this or or that)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Array (fieldname=exact_field, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=this, index=db.schema.table.index)\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=that, index=db.schema.table.index)"
        );
        assertAST("exact_field:(this not not that)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      And\n" +
                        "         Word (fieldname=exact_field, operator=CONTAINS, value=this, index=db.schema.table.index)\n" +
                        "         Not\n" +
                        "            Not\n" +
                        "               Word (fieldname=exact_field, operator=CONTAINS, value=that, index=db.schema.table.index)"
        );
    }

    @Test
    public void testIssue175_OR() throws Exception {
        assertAST("#options(user_data:(owner_user_id=<so_users.idxso_users>id), comment_data:(id=<so_comments.idxso_comments>post_id)) " +
                        "a OR b",
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
                        "   Or\n" +
                        "      Expansion\n" +
                        "         id=<db.schema.table.index>id\n" +
                        "         Array (fieldname=fulltext_field, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=a, index=db.schema.table.index)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=b, index=db.schema.table.index)\n" +
                        "      Or\n" +
                        "         Expansion\n" +
                        "            user_data:(owner_user_id=<db.schema.so_users.idxso_users>id)\n" +
                        "               LeftField (value=owner_user_id)\n" +
                        "               IndexName (value=db.schema.so_users.idxso_users)\n" +
                        "               RightField (value=id)\n" +
                        "            Array (fieldname=_all, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=a, index=db.schema.so_users.idxso_users)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=b, index=db.schema.so_users.idxso_users)\n" +
                        "         Expansion\n" +
                        "            comment_data:(id=<db.schema.so_comments.idxso_comments>post_id)\n" +
                        "               LeftField (value=id)\n" +
                        "               IndexName (value=db.schema.so_comments.idxso_comments)\n" +
                        "               RightField (value=post_id)\n" +
                        "            Array (fieldname=_all, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=a, index=db.schema.so_comments.idxso_comments)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=b, index=db.schema.so_comments.idxso_comments)\n" +
                        "         Array (fieldname=_all, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=a, index=db.schema.table.index)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=b, index=db.schema.table.index)\n" +
                        "      Expansion\n" +
                        "         user_data:(owner_user_id=<db.schema.so_users.idxso_users>id)\n" +
                        "            LeftField (value=owner_user_id)\n" +
                        "            IndexName (value=db.schema.so_users.idxso_users)\n" +
                        "            RightField (value=id)\n" +
                        "         Array (fieldname=body, operator=CONTAINS, index=db.schema.so_users.idxso_users) (OR)\n" +
                        "            Word (fieldname=body, operator=CONTAINS, value=a, index=db.schema.so_users.idxso_users)\n" +
                        "            Word (fieldname=body, operator=CONTAINS, value=b, index=db.schema.so_users.idxso_users)"
        );
    }

    @Test
    public void testLimit() throws Exception {
        assertAST("#limit(exact_field desc, 12, 42)",
                "QueryTree\n" +
                        "   Limit\n" +
                        "      LimitFieldname (value=exact_field)\n" +
                        "      SortDirection (value=desc)\n" +
                        "      Number (value=12)\n" +
                        "      Number (value=42)"
        );

        assertAST("(#limit(exact_field desc, 12, 42) a or b)",
                "QueryTree\n" +
                        "   Limit\n" +
                        "      LimitFieldname (value=exact_field)\n" +
                        "      SortDirection (value=desc)\n" +
                        "      Number (value=12)\n" +
                        "      Number (value=42)\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Or\n" +
                        "         Array (fieldname=fulltext_field, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=a, index=db.schema.table.index)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=b, index=db.schema.table.index)\n" +
                        "         Array (fieldname=_all, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=a, index=db.schema.table.index)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=b, index=db.schema.table.index)"
        );

        assertJson("(#limit(exact_fied desc, 12, 42) a or b)",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"should\" : [ {\n" +
                        "      \"terms\" : {\n" +
                        "        \"fulltext_field\" : [ \"a\", \"b\" ]\n" +
                        "      }\n" +
                        "    }, {\n" +
                        "      \"terms\" : {\n" +
                        "        \"_all\" : [ \"a\", \"b\" ]\n" +
                        "      }\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue316() throws Exception {
        assertAST("#limit(a asc, b desc, c asc, d asc, 0, 10)",
                "QueryTree\n" +
                        "   Limit\n" +
                        "      LimitFieldname (value=a)\n" +
                        "      SortDirection (value=asc)\n" +
                        "      LimitFieldname (value=b)\n" +
                        "      SortDirection (value=desc)\n" +
                        "      LimitFieldname (value=c)\n" +
                        "      SortDirection (value=asc)\n" +
                        "      LimitFieldname (value=d)\n" +
                        "      SortDirection (value=asc)\n" +
                        "      Number (value=0)\n" +
                        "      Number (value=10)");
    }

    @Test
    public void testIssue215() throws Exception {
        assertAST("(#options() foo)",
                "QueryTree\n" +
                        "   Options\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Or\n" +
                        "         Word (fieldname=fulltext_field, operator=CONTAINS, value=foo, index=db.schema.table.index)\n" +
                        "         Word (fieldname=_all, operator=CONTAINS, value=foo, index=db.schema.table.index)"
        );
    }

    @Test
    public void testES563DoesntAllowNullInTermsQuery() throws Exception {
        try {
            reparseViaQueryBuilder("{\"terms\": {\"field\": [\"a\", null, \"c\" } }");
            fail("query parsed correctly when it shouldn't have");
        } catch (ParsingException pe) {
            assertTrue(pe.getMessage().contains("No value specified for terms query"));
        }
    }

    @Test
    public void testPullOutNullsFromArrays() throws Exception {
        assertJson("field:[a,b, null, c]",
                "{\n" +
                        "  \"bool\" : {\n" +
                        "    \"should\" : [\n" +
                        "      {\n" +
                        "        \"bool\" : {\n" +
                        "          \"must_not\" : [\n" +
                        "            {\n" +
                        "              \"exists\" : {\n" +
                        "                \"field\" : \"field\",\n" +
                        "                \"boost\" : 1.0\n" +
                        "              }\n" +
                        "            }\n" +
                        "          ],\n" +
                        "          \"disable_coord\" : false,\n" +
                        "          \"adjust_pure_negative\" : true,\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"terms\" : {\n" +
                        "          \"field\" : [\n" +
                        "            \"a\",\n" +
                        "            \"b\",\n" +
                        "            \"c\"\n" +
                        "          ],\n" +
                        "          \"boost\" : 1.0\n" +
                        "        }\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"disable_coord\" : false,\n" +
                        "    \"adjust_pure_negative\" : true,\n" +
                        "    \"boost\" : 1.0\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue244() throws Exception {
        assertAST(resource(this.getClass(), "testIssue244.query"),
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Or\n" +
                        "         Array (fieldname=exact_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "            Word (fieldname=exact_field, operator=CONTAINS, value=\\\\, index=db.schema.table.index)\n" +
                        "            Word (fieldname=exact_field, operator=CONTAINS, value=foo, index=db.schema.table.index)\n" +
                        "         Array (fieldname=exact_field, operator=CONTAINS, index=db.schema.table.index) (AND)\n" +
                        "            Word (fieldname=exact_field, operator=CONTAINS, value=\\\\, index=db.schema.table.index)\n" +
                        "            Word (fieldname=exact_field, operator=CONTAINS, value=foo, index=db.schema.table.index)"
        );
    }

    @Test
    public void testSubselect() throws Exception {
        assertAST("#subselect<id=<this.index>id>(beer, wine, cheese)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Or\n" +
                        "         Array (fieldname=fulltext_field, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=beer, index=db.schema.table.index)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=wine, index=db.schema.table.index)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=cheese, index=db.schema.table.index)\n" +
                        "         Array (fieldname=_all, operator=CONTAINS, index=db.schema.table.index) (OR)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=beer, index=db.schema.table.index)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=wine, index=db.schema.table.index)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=cheese, index=db.schema.table.index)");
    }

    @Test
    public void testIssue272_AST_WITH() throws Exception {
        assertAST("(data.obj1.key1=val1 with data.obj1.key2=val1) with data.top_key:1",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      With\n" +
                        "         Number (fieldname=data.top_key, operator=CONTAINS, value=1, index=db.schema.table.index)\n" +
                        "         With\n" +
                        "            Word (fieldname=data.obj1.key1, operator=EQ, value=val1, index=db.schema.table.index)\n" +
                        "            Word (fieldname=data.obj1.key2, operator=EQ, value=val1, index=db.schema.table.index)");
    }

    @Test
    public void testIssue272_AST_WITH2() throws Exception {
        assertAST("(data.obj1.key1=val1 with data.obj2.key2=val1) with data.top_key:1",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      With\n" +
                        "         Number (fieldname=data.top_key, operator=CONTAINS, value=1, index=db.schema.table.index)\n" +
                        "         With\n" +
                        "            Word (fieldname=data.obj1.key1, operator=EQ, value=val1, index=db.schema.table.index)\n" +
                        "         With\n" +
                        "            Word (fieldname=data.obj2.key2, operator=EQ, value=val1, index=db.schema.table.index)");
    }

    @Test
    public void testIssue272_AST_OR() throws Exception {
        assertAST("data.top_key:1 with (data.obj1.key1=val1 or data.top_key=val1)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      With\n" +
                        "         Number (fieldname=data.top_key, operator=CONTAINS, value=1, index=db.schema.table.index)\n" +
                        "         Or\n" +
                        "            Word (fieldname=data.top_key, operator=EQ, value=val1, index=db.schema.table.index)\n" +
                        "            Word (fieldname=data.obj1.key1, operator=EQ, value=val1, index=db.schema.table.index)");
    }

    @Test
    public void testIssue272_AST_AND() throws Exception {
        assertAST("data.top_key:1 with (data.obj1.key1=val1 and data.obj1.key2=val1)",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      With\n" +
                        "         Number (fieldname=data.top_key, operator=CONTAINS, value=1, index=db.schema.table.index)\n" +
                        "         And\n" +
                        "            Word (fieldname=data.obj1.key1, operator=EQ, value=val1, index=db.schema.table.index)\n" +
                        "            Word (fieldname=data.obj1.key2, operator=EQ, value=val1, index=db.schema.table.index)");
    }

    @Test
    public void testIssue272_Json() throws Exception {
        assertJson("data.top_key:1 with (data.obj1.key1=val1 WITH data.obj1.key2=val1)",
                "{\n" +
                        "  \"nested\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [\n" +
                        "          {\n" +
                        "            \"term\" : {\n" +
                        "              \"data.top_key\" : {\n" +
                        "                \"value\" : 1,\n" +
                        "                \"boost\" : 1.0\n" +
                        "              }\n" +
                        "            }\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"nested\" : {\n" +
                        "              \"query\" : {\n" +
                        "                \"bool\" : {\n" +
                        "                  \"must\" : [\n" +
                        "                    {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"data.obj1.key1\" : {\n" +
                        "                          \"value\" : \"val1\",\n" +
                        "                          \"boost\" : 1.0\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    },\n" +
                        "                    {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"data.obj1.key2\" : {\n" +
                        "                          \"value\" : \"val1\",\n" +
                        "                          \"boost\" : 1.0\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  ],\n" +
                        "                  \"disable_coord\" : false,\n" +
                        "                  \"adjust_pure_negative\" : true,\n" +
                        "                  \"boost\" : 1.0\n" +
                        "                }\n" +
                        "              },\n" +
                        "              \"path\" : \"data.obj1\",\n" +
                        "              \"ignore_unmapped\" : false,\n" +
                        "              \"score_mode\" : \"avg\",\n" +
                        "              \"boost\" : 1.0\n" +
                        "            }\n" +
                        "          }\n" +
                        "        ],\n" +
                        "        \"disable_coord\" : false,\n" +
                        "        \"adjust_pure_negative\" : true,\n" +
                        "        \"boost\" : 1.0\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"path\" : \"data\",\n" +
                        "    \"ignore_unmapped\" : false,\n" +
                        "    \"score_mode\" : \"avg\",\n" +
                        "    \"boost\" : 1.0\n" +
                        "  }\n" +
                        "}");
    }

    @Test
    public void testIssue311() throws Exception {
        assertJson("issue311.text:(\"acute\") with issue311.text:(\"heart attack\")",
                "{\n" +
                        "  \"nested\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [\n" +
                        "          {\n" +
                        "            \"term\" : {\n" +
                        "              \"issue311.text\" : {\n" +
                        "                \"value\" : \"acute\",\n" +
                        "                \"boost\" : 1.0\n" +
                        "              }\n" +
                        "            }\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"match_phrase\" : {\n" +
                        "              \"issue311.text\" : {\n" +
                        "                \"query\" : \"heart attack\",\n" +
                        "                \"slop\" : 0,\n" +
                        "                \"boost\" : 1.0\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        ],\n" +
                        "        \"disable_coord\" : false,\n" +
                        "        \"adjust_pure_negative\" : true,\n" +
                        "        \"boost\" : 1.0\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"path\" : \"issue311\",\n" +
                        "    \"ignore_unmapped\" : false,\n" +
                        "    \"score_mode\" : \"avg\",\n" +
                        "    \"boost\" : 1.0\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue349_Prefix() throws Exception {
        assertAST("issue349_field:j*",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Prefix (fieldname=issue349_field, operator=CONTAINS, value=j, index=db.schema.table.index)"
        );
    }

    @Test
    public void testIssue349_Wildcard() throws Exception {
        assertAST("issue349_field:*j*",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Wildcard (fieldname=issue349_field, operator=CONTAINS, value=*j*, index=db.schema.table.index)"
        );
    }

    @Test
    public void testIssue349_Single() throws Exception {
        assertAST("issue349_field:j",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id"
        );

        assertJson("issue349_field:j",
                "{\n" +
                        "  \"match_none\" : {\n" +
                        "    \"boost\" : 1.0\n" +
                        "  }\n" +
                        "}"
        );
    }

    @Test
    public void testIssue349_Fuzzy() throws Exception {
        assertAST("issue349_field:jeffery~2",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      id=<db.schema.table.index>id\n" +
                        "      Fuzzy (fieldname=issue349_field, operator=CONTAINS, value=jeffery, fuzz=2, index=db.schema.table.index)"
        );
    }
}

