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

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by e_ridge on 10/15/14.
 */
public class TestQueryRewriter {
    String query = "#options(id=<table.idx>id, id=<table.idx>id, id=<table.idx>id, other:(left=<table.idx>right)) #extended_stats(custodian) #tally(subject, '^.*', 1000, '_term', #significant_terms(author, '^.*', 1000))  " +
            "#child<data>(" +
            "#nest<review_data>(state_id=2239 and set_tag_id=82 review_data.foo:true) fulltext=[beer] meeting not staff not cancelled not risk " +
            "#expand<left_field = <index.name>right_field>(the subquery) " +
            "#child<data>(some query) #parent<xact>(other query) #child<data>(())" +
            "long.dotted.field:foo " +
            "fuzzy~32 1-2 " +
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


    private class MockClientAndRequest {
        private final Client client;
        private final RestRequest request;

        public MockClientAndRequest() throws InterruptedException, java.util.concurrent.ExecutionException {
            request = mock(RestRequest.class);
            when(request.param("index")).thenReturn("schema.table.idxname");

            client = mock(Client.class);
            ActionFuture<GetMappingsResponse> future = mock(ActionFuture.class);
            GetMappingsResponse response = mock(GetMappingsResponse.class);
            AdminClient mockedAdminClient = mock(AdminClient.class);
            IndicesAdminClient mockedIndiciesAdminClient = mock(IndicesAdminClient.class);

            when(mockedAdminClient.indices()).thenReturn(mockedIndiciesAdminClient);
            when(client.admin()).thenReturn(mockedAdminClient);

            when(mockedIndiciesAdminClient.getMappings(any(GetMappingsRequest.class))).thenReturn(future);
            when(future.get()).thenReturn(response);
            when(response.getMappings()).then(new Answer<Object>() {
                @Override
                public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                    ImmutableOpenMap.Builder builder = new ImmutableOpenMap.Builder<>();
                    ImmutableOpenMap.Builder builder2 = new ImmutableOpenMap.Builder<>();
                    MappingMetaData meta = mock(MappingMetaData.class);

                    when(meta.getSourceAsMap()).then(new Answer<Map>() {
                        @Override
                        public Map answer(InvocationOnMock invocationOnMock) throws Throwable {
                            Map map = new HashMap();
                            Map props = new HashMap();
                            Map fieldProps;

                            map.put("properties", props);

                            fieldProps = new HashMap();
                            props.put("exact_field", fieldProps);
                            fieldProps.put("analyzer", "exact");

                            props.put("id", fieldProps);
                            fieldProps.put("analyzer", "exact");
                            fieldProps.put("type", "long");

                            fieldProps = new HashMap();
                            props.put("phrase_field", fieldProps);
                            fieldProps.put("analyzer", "phrase");

                            fieldProps = new HashMap();
                            props.put("fulltext_field", fieldProps);
                            fieldProps.put("analyzer", "fulltext");

                            fieldProps = new HashMap();
                            props.put("sent", fieldProps);
                            fieldProps.put("analyzer", "date");

                            fieldProps = new HashMap();
                            props.put("sent", fieldProps);
                            fieldProps.put("analyzer", "date");
                            final Map fieldsMap = new HashMap();
                            fieldsMap.put("date", "something about date");
                            fieldProps.put("fields", fieldsMap);

                            fieldProps = new HashMap();
                            props.put("_all", fieldProps);
                            fieldProps.put("analyzer", "phrase");

                            return map;
                        }
                    });
                    builder2.put("data", meta);
                    builder.put("schema.table.idxname", builder2.build());
                    return builder.build();
                }
            });
        }
    }

    @Test
    public void testIt() throws Exception {
        MockClientAndRequest mock = new MockClientAndRequest();

        QueryRewriter qr = new QueryRewriter(mock.client, mock.request, query, false, true);
        QueryBuilder qb = qr.rewriteQuery();
        assertEquals("testIt",
                resource("testIt.expected"),
                qb.toString());

        AbstractAggregationBuilder ab = qr.rewriteAggregations();
    }

    @Test
    public void testComplexQueryAST() throws Exception {
        MockClientAndRequest mock = new MockClientAndRequest();

        QueryRewriter qr = new QueryRewriter(mock.client, mock.request, query, false, true);
        assertEquals("testComplexQueryAST",
                resource("testComplexQueryAST.expected").trim(),
                qr.dumpAsString().trim());
    }

    private String resource(String name) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(TestQueryRewriter.class.getResourceAsStream(name), "UTF-8"))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (sb.length()>0) sb.append("\n");
                sb.append(line);
            }
            return sb.toString();
        }
    }

    @Test
    public void testSingleOption() throws Exception {
        assertEquals("testSingleOption",
                "QueryTree\n" +
                        "   Options\n" +
                        "      left=<table.index>right\n" +
                        "         LeftField (value=left)\n" +
                        "         IndexName (value=table.index)\n" +
                        "         RightField (value=right)\n",
                QueryRewriter.dumpAsString("#options(left=<table.index>right)"));
    }

    @Test
    public void testMultipleOptions() throws Exception {
        assertEquals("testMultipleOptions",
                "QueryTree\n" +
                        "   Options\n" +
                        "      left=<table.index>right\n" +
                        "         LeftField (value=left)\n" +
                        "         IndexName (value=table.index)\n" +
                        "         RightField (value=right)\n" +
                        "      left2=<table2.index2>right2\n" +
                        "         LeftField (value=left2)\n" +
                        "         IndexName (value=table2.index2)\n" +
                        "         RightField (value=right2)\n",
                QueryRewriter.dumpAsString("#options(left=<table.index>right, left2=<table2.index2>right2)"));
    }

    @Test
    public void testSingleNamedOption() throws Exception {
        assertEquals("testSingleNamedOption",
                "QueryTree\n" +
                        "   Options\n" +
                        "      f_name:(left=<table.index>right)\n" +
                        "         LeftField (value=left)\n" +
                        "         IndexName (value=table.index)\n" +
                        "         RightField (value=right)\n",
                QueryRewriter.dumpAsString("#options(f_name:(left=<table.index>right))"));
    }

    @Test
    public void testMultipleNamedOptions() throws Exception {
        assertEquals("testMultipleNamedOptions",
                "QueryTree\n" +
                        "   Options\n" +
                        "      f_name:(left=<table.index>right)\n" +
                        "         LeftField (value=left)\n" +
                        "         IndexName (value=table.index)\n" +
                        "         RightField (value=right)\n" +
                        "      f_name2:(left2=<table2.index2>right2)\n" +
                        "         LeftField (value=left2)\n" +
                        "         IndexName (value=table2.index2)\n" +
                        "         RightField (value=right2)\n",
                QueryRewriter.dumpAsString("#options(f_name:(left=<table.index>right), f_name2:(left2=<table2.index2>right2))"));
    }

    @Test
    public void testMultipleMixedOptions() throws Exception {
        assertEquals("testMultipleMixedOptions",
                "QueryTree\n" +
                        "   Options\n" +
                        "      left=<table.index>right\n" +
                        "         LeftField (value=left)\n" +
                        "         IndexName (value=table.index)\n" +
                        "         RightField (value=right)\n" +
                        "      f_name2:(left2=<table2.index2>right2)\n" +
                        "         LeftField (value=left2)\n" +
                        "         IndexName (value=table2.index2)\n" +
                        "         RightField (value=right2)\n",
                QueryRewriter.dumpAsString("#options(left=<table.index>right, f_name2:(left2=<table2.index2>right2))"));
    }

    @Test
    public void test_allFieldExpansion() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();

        qr = new QueryRewriter(mock.client, mock.request, "beer or wine or cheese and fulltext:bob", false, true);
        assertEquals("test_allFieldExpansion",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      null=<schema.table.idxname>null\n" +
                        "      Or\n" +
                        "         And\n" +
                        "            Or\n" +
                        "               Word (fieldname=fulltext_field, operator=CONTAINS, value=cheese, index=schema.table.idxname)\n" +
                        "               Word (fieldname=_all, operator=CONTAINS, value=cheese, index=schema.table.idxname)\n" +
                        "            Word (fieldname=fulltext, operator=CONTAINS, value=bob, index=schema.table.idxname)\n" +
                        "         Array (fieldname=fulltext_field, operator=CONTAINS, index=schema.table.idxname) (OR)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=beer, index=schema.table.idxname)\n" +
                        "            Word (fieldname=fulltext_field, operator=CONTAINS, value=wine, index=schema.table.idxname)\n" +
                        "         Array (fieldname=_all, operator=CONTAINS, index=schema.table.idxname) (OR)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=beer, index=schema.table.idxname)\n" +
                        "            Word (fieldname=_all, operator=CONTAINS, value=wine, index=schema.table.idxname)\n",
                qr.dumpAsString());
    }

    @Test
    public void testASTExpansionInjection() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();

        qr = new QueryRewriter(mock.client, mock.request, "#options(id=<main_ft.idxmain_ft>ft_id, id=<main_vol.idxmain_vol>vol_id, id=<main_other.idxmain_other>other_id) (((_xmin = 6250261 AND _cmin < 0 AND (_xmax = 0 OR (_xmax = 6250261 AND _cmax >= 0))) OR (_xmin_is_committed = true AND (_xmax = 0 OR (_xmax = 6250261 AND _cmax >= 0) OR (_xmax <> 6250261 AND _xmax_is_committed = false))))) AND (#child<data>((phrase_field:(beer w/500 a))))", false, true);
        assertEquals("testASTExpansionInjection",
                "QueryTree\n" +
                        "   Options\n" +
                        "      id=<schema.main_ft.idxmain_ft>ft_id\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=schema.main_ft.idxmain_ft)\n" +
                        "         RightField (value=ft_id)\n" +
                        "      id=<schema.main_vol.idxmain_vol>vol_id\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=schema.main_vol.idxmain_vol)\n" +
                        "         RightField (value=vol_id)\n" +
                        "      id=<schema.main_other.idxmain_other>other_id\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=schema.main_other.idxmain_other)\n" +
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
                        "            null=<schema.table.idxname>null\n" +
                        "            Proximity (fieldname=phrase_field, operator=CONTAINS, distance=500, index=schema.table.idxname)\n" +
                        "               Word (fieldname=phrase_field, operator=CONTAINS, value=beer, index=schema.table.idxname)\n" +
                        "               Word (fieldname=phrase_field, operator=CONTAINS, value=a, index=schema.table.idxname)\n",
                qr.dumpAsString());
    }

    @Test
    public void testASTExpansionInjection2() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();

        qr = new QueryRewriter(mock.client, mock.request,
                "#options(id=<main_ft.idxmain_ft>ft_id, id=<main_vol.idxmain_vol>vol_id, id=<main_other.idxmain_other>other_id) (((_xmin = 6250507 AND _cmin < 0 AND (_xmax = 0 OR (_xmax = 6250507 AND _cmax >= 0))) OR (_xmin_is_committed = true AND (_xmax = 0 OR (_xmax = 6250507 AND _cmax >= 0) OR (_xmax <> 6250507 AND _xmax_is_committed = false))))) AND (#child<data>((( #expand<data_cv_group_id=<this.index>data_cv_group_id> ( ( (( ( data_client_name = ANTHEM AND data_duplicate_resource = NO ) )) AND " +
                        "( (data_custodian = \"Querty, AMY\" OR data_custodian = \"QWERTY, COLIN\" OR data_custodian = \"QWERTY, KEITH\" OR data_custodian = \"QWERTY, PERRY\" OR data_custodian = \"QWERTY, NORM\" OR data_custodian = \"QWERTY, MIKE\" OR " +
                        "data_custodian = \"QWERTY,MIKE\" OR data_custodian = \"QWERTY, DAN\" OR data_custodian = \"QWERTY,DAN\") AND data_filter_06b = \"QWERTY*\" AND NOT data_moved_to = \"*\" ) ) ) ))))",
                false, true);
        assertEquals("testASTExpansionInjection2",
                "QueryTree\n" +
                        "   Options\n" +
                        "      id=<schema.main_ft.idxmain_ft>ft_id\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=schema.main_ft.idxmain_ft)\n" +
                        "         RightField (value=ft_id)\n" +
                        "      id=<schema.main_vol.idxmain_vol>vol_id\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=schema.main_vol.idxmain_vol)\n" +
                        "         RightField (value=vol_id)\n" +
                        "      id=<schema.main_other.idxmain_other>other_id\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=schema.main_other.idxmain_other)\n" +
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
                        "         Expansion\n" +
                        "            data_cv_group_id=<schema.table.idxname>data_cv_group_id\n" +
                        "            Expansion\n" +
                        "               null=<schema.table.idxname>null\n" +
                        "               And\n" +
                        "                  Word (fieldname=data_client_name, operator=EQ, value=anthem, index=schema.table.idxname)\n" +
                        "                  Word (fieldname=data_duplicate_resource, operator=EQ, value=no, index=schema.table.idxname)\n" +
                        "                  Or\n" +
                        "                     Phrase (fieldname=data_custodian, operator=EQ, value=querty, amy, ordered=true, index=schema.table.idxname)\n" +
                        "                     Phrase (fieldname=data_custodian, operator=EQ, value=qwerty, colin, ordered=true, index=schema.table.idxname)\n" +
                        "                     Phrase (fieldname=data_custodian, operator=EQ, value=qwerty, keith, ordered=true, index=schema.table.idxname)\n" +
                        "                     Phrase (fieldname=data_custodian, operator=EQ, value=qwerty, perry, ordered=true, index=schema.table.idxname)\n" +
                        "                     Phrase (fieldname=data_custodian, operator=EQ, value=qwerty, norm, ordered=true, index=schema.table.idxname)\n" +
                        "                     Phrase (fieldname=data_custodian, operator=EQ, value=qwerty, mike, ordered=true, index=schema.table.idxname)\n" +
                        "                     Word (fieldname=data_custodian, operator=EQ, value=qwerty,mike, index=schema.table.idxname)\n" +
                        "                     Phrase (fieldname=data_custodian, operator=EQ, value=qwerty, dan, ordered=true, index=schema.table.idxname)\n" +
                        "                     Word (fieldname=data_custodian, operator=EQ, value=qwerty,dan, index=schema.table.idxname)\n" +
                        "                  Phrase (fieldname=data_filter_06b, operator=EQ, value=qwerty*, ordered=true, index=schema.table.idxname)\n" +
                        "                  Not\n" +
                        "                     NotNull (fieldname=data_moved_to, operator=EQ, index=schema.table.idxname)\n",
                qr.dumpAsString());
    }

    @Test
    public void testSimplePhrase() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:(\"this is a phrase\")", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("testSimplePhrase",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"query\" : \"this is a phrase\",\n" +
                        "            \"type\" : \"phrase\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testPhraseWithEscapedWildcards() throws Exception {
        assertEquals("parsed as phrase query", "{\n" +
                "  \"filtered\" : {\n" +
                "    \"query\" : {\n" +
                "      \"match_all\" : { }\n" +
                "    },\n" +
                "    \"filter\" : {\n" +
                "      \"query\" : {\n" +
                "        \"match\" : {\n" +
                "          \"_all\" : {\n" +
                "            \"query\" : \"* this phrase has ?escaped~ wildcards*\",\n" +
                "            \"type\" : \"phrase\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", QueryRewriter.toJson("'\\* this phrase has \\?escaped\\~ wildcards\\*'"));
    }

    @Test
    public void testPhraseWithFuzzyTerms() throws Exception {
        assertEquals("parsed with fuzzy terms", "{\n" +
                "  \"filtered\" : {\n" +
                "    \"query\" : {\n" +
                "      \"match_all\" : { }\n" +
                "    },\n" +
                "    \"filter\" : {\n" +
                "      \"query\" : {\n" +
                "        \"span_near\" : {\n" +
                "          \"clauses\" : [ {\n" +
                "            \"span_multi\" : {\n" +
                "              \"match\" : {\n" +
                "                \"fuzzy\" : {\n" +
                "                  \"_all\" : {\n" +
                "                    \"value\" : \"here\",\n" +
                "                    \"prefix_length\" : 3\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_multi\" : {\n" +
                "              \"match\" : {\n" +
                "                \"fuzzy\" : {\n" +
                "                  \"_all\" : {\n" +
                "                    \"value\" : \"is\",\n" +
                "                    \"prefix_length\" : 3\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_multi\" : {\n" +
                "              \"match\" : {\n" +
                "                \"fuzzy\" : {\n" +
                "                  \"_all\" : {\n" +
                "                    \"value\" : \"fuzzy\",\n" +
                "                    \"prefix_length\" : 3\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"words\"\n" +
                "              }\n" +
                "            }\n" +
                "          } ],\n" +
                "          \"slop\" : 0,\n" +
                "          \"in_order\" : true\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", QueryRewriter.toJson("'Here~ is~ fuzzy~ words'"));
    }

    @Test
    public void testPhraseWithEscapedFuzzyCharacters() throws Exception {
        assertEquals("parsed with escaped fuzzy characters", "{\n" +
                "  \"filtered\" : {\n" +
                "    \"query\" : {\n" +
                "      \"match_all\" : { }\n" +
                "    },\n" +
                "    \"filter\" : {\n" +
                "      \"query\" : {\n" +
                "        \"match\" : {\n" +
                "          \"_all\" : {\n" +
                "            \"query\" : \"here~ is~ fuzzy~ words\",\n" +
                "            \"type\" : \"phrase\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", QueryRewriter.toJson("'Here\\~ is\\~ fuzzy\\~ words'"));
    }

    @Test
    public void testPhraseWithMixedEscaping() throws Exception {
        assertEquals("parsed with mixed escaping", "{\n" +
                "  \"filtered\" : {\n" +
                "    \"query\" : {\n" +
                "      \"match_all\" : { }\n" +
                "    },\n" +
                "    \"filter\" : {\n" +
                "      \"query\" : {\n" +
                "        \"span_near\" : {\n" +
                "          \"clauses\" : [ {\n" +
                "            \"span_multi\" : {\n" +
                "              \"match\" : {\n" +
                "                \"prefix\" : {\n" +
                "                  \"_all\" : \"this\"\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"should*\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"subparse\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"into\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"a\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_multi\" : {\n" +
                "              \"match\" : {\n" +
                "                \"fuzzy\" : {\n" +
                "                  \"_all\" : {\n" +
                "                    \"value\" : \"sp?n\",\n" +
                "                    \"prefix_length\" : 3\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          } ],\n" +
                "          \"slop\" : 0,\n" +
                "          \"in_order\" : true\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", QueryRewriter.toJson("'this* should\\* subparse into a sp\\?n~'"));
    }

    @Test
    public void testPhraseWithMetaCharacters() throws Exception {
        assertEquals("parsed with mixed escaping and meta characters", "{\n" +
                "  \"filtered\" : {\n" +
                "    \"query\" : {\n" +
                "      \"match_all\" : { }\n" +
                "    },\n" +
                "    \"filter\" : {\n" +
                "      \"query\" : {\n" +
                "        \"span_near\" : {\n" +
                "          \"clauses\" : [ {\n" +
                "            \"span_multi\" : {\n" +
                "              \"match\" : {\n" +
                "                \"prefix\" : {\n" +
                "                  \"_all\" : \"this\"\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"should*\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"sub:parse\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \":-\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"into\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"a\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_multi\" : {\n" +
                "              \"match\" : {\n" +
                "                \"fuzzy\" : {\n" +
                "                  \"_all\" : {\n" +
                "                    \"value\" : \"sp?n\",\n" +
                "                    \"prefix_length\" : 3\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          } ],\n" +
                "          \"slop\" : 0,\n" +
                "          \"in_order\" : true\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", QueryRewriter.toJson("'this* should\\* sub:parse :- into a sp\\?n~'"));
    }

    @Test
    public void testPhraseWithSlop() throws Exception {
        assertEquals("phrase with slop", "{\n" +
                "  \"filtered\" : {\n" +
                "    \"query\" : {\n" +
                "      \"match_all\" : { }\n" +
                "    },\n" +
                "    \"filter\" : {\n" +
                "      \"query\" : {\n" +
                "        \"span_near\" : {\n" +
                "          \"clauses\" : [ {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"some\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"phrase\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"with\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"slop\"\n" +
                "              }\n" +
                "            }\n" +
                "          } ],\n" +
                "          \"slop\" : 2,\n" +
                "          \"in_order\" : true\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", QueryRewriter.toJson("'some phrase with slop'~2"));
    }

    @Test
    public void testPhraseWithWildcards() throws Exception {
        assertEquals("phrase with wildcards", "{\n" +
                "  \"filtered\" : {\n" +
                "    \"query\" : {\n" +
                "      \"match_all\" : { }\n" +
                "    },\n" +
                "    \"filter\" : {\n" +
                "      \"query\" : {\n" +
                "        \"span_near\" : {\n" +
                "          \"clauses\" : [ {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"phrase\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"with\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_multi\" : {\n" +
                "              \"match\" : {\n" +
                "                \"wildcard\" : {\n" +
                "                  \"_all\" : \"wild*card\"\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          } ],\n" +
                "          \"slop\" : 0,\n" +
                "          \"in_order\" : true\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", QueryRewriter.toJson("'phrase with wild*card'"));
    }

    @Test
    public void testAndedWildcardPhrases() throws Exception {
        assertEquals("anded wildcard phrases", "{\n" +
                "  \"filtered\" : {\n" +
                "    \"query\" : {\n" +
                "      \"match_all\" : { }\n" +
                "    },\n" +
                "    \"filter\" : {\n" +
                "      \"query\" : {\n" +
                "        \"span_near\" : {\n" +
                "          \"clauses\" : [ {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"phrase\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"with\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_multi\" : {\n" +
                "              \"match\" : {\n" +
                "                \"wildcard\" : {\n" +
                "                  \"_all\" : \"wild*card\"\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"and\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_multi\" : {\n" +
                "              \"match\" : {\n" +
                "                \"wildcard\" : {\n" +
                "                  \"_all\" : \"w*ns\"\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          } ],\n" +
                "          \"slop\" : 0,\n" +
                "          \"in_order\" : true\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", QueryRewriter.toJson("'phrase with wild*card AND w*ns'"));
    }

    @Test
    public void testOredWildcardPhrases() throws Exception {
        assertEquals("", "{\n" +
                "  \"filtered\" : {\n" +
                "    \"query\" : {\n" +
                "      \"match_all\" : { }\n" +
                "    },\n" +
                "    \"filter\" : {\n" +
                "      \"query\" : {\n" +
                "        \"span_near\" : {\n" +
                "          \"clauses\" : [ {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"phrase\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"with\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_multi\" : {\n" +
                "              \"match\" : {\n" +
                "                \"wildcard\" : {\n" +
                "                  \"_all\" : \"wild*card\"\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_term\" : {\n" +
                "              \"_all\" : {\n" +
                "                \"value\" : \"or\"\n" +
                "              }\n" +
                "            }\n" +
                "          }, {\n" +
                "            \"span_multi\" : {\n" +
                "              \"match\" : {\n" +
                "                \"wildcard\" : {\n" +
                "                  \"_all\" : \"w*ns\"\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          } ],\n" +
                "          \"slop\" : 0,\n" +
                "          \"in_order\" : true\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", QueryRewriter.toJson("'phrase with wild*card OR w*ns'"));
    }

    @Test
    public void testCVSIX939_boolean_simpilification() throws Exception {
        assertEquals("test id:1 and id<>1", "{\n" +
                "  \"filtered\" : {\n" +
                "    \"query\" : {\n" +
                "      \"match_all\" : { }\n" +
                "    },\n" +
                "    \"filter\" : {\n" +
                "      \"bool\" : {\n" +
                "        \"must\" : [ {\n" +
                "          \"term\" : {\n" +
                "            \"id\" : 1\n" +
                "          }\n" +
                "        }, {\n" +
                "          \"not\" : {\n" +
                "            \"filter\" : {\n" +
                "              \"term\" : {\n" +
                "                \"id\" : 1\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        } ]\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", QueryRewriter.toJson("id:1 and id<>1"));
    }

    @Test
    public void testTermAndPhraseEqual() throws Exception {
        assertEquals("term and phrase equal", QueryRewriter.toJson("\\*test\\~"), QueryRewriter.toJson("'\\*test\\~'"));
    }

    @Test
    public void testSimpleNestedGroup() throws Exception {
        assertEquals("testSimpleNestedGroup",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"nested\" : {\n" +
                        "        \"filter\" : {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"term\" : {\n" +
                        "                \"nested_group.field_a\" : \"value\"\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"nested_group.field_b\" : \"value\"\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"join\" : true,\n" +
                        "        \"path\" : \"nested_group\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                QueryRewriter.toJson("nested_group.field_a:value and nested_group.field_b:value"));
    }

    @Test
    public void testSimpleNestedGroup2() throws Exception {
        assertEquals("testSimpleNestedGroup2",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"nested\" : {\n" +
                        "        \"filter\" : {\n" +
                        "          \"term\" : {\n" +
                        "            \"nested_group.field_a\" : \"value\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"join\" : true,\n" +
                        "        \"path\" : \"nested_group\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                QueryRewriter.toJson("nested_group.field_a:value "));
    }

    @Test
    public void testExternalNestedGroup() throws Exception {
        String query = "#options(witness_data:(id=<witness.idxwitness>id)) witness_data.wit_first_name = 'mark' and witness_data.wit_last_name = 'matte'";
        String json;

        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        qr = new QueryRewriter(mock.client, mock.request, query, false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("testExternalNestedGroup",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"term\" : {\n" +
                        "            \"witness_data.wit_first_name\" : \"mark\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"term\" : {\n" +
                        "            \"witness_data.wit_last_name\" : \"matte\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testCVSIX_2551() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:c-note", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("CVSIX_2551",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"query\" : \"c-note\",\n" +
                        "            \"type\" : \"phrase\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testCVSIX_2551_PrefixWildcard() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:c-note*", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("CVSIX_2551",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"c\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_multi\" : {\n" +
                        "              \"match\" : {\n" +
                        "                \"prefix\" : {\n" +
                        "                  \"phrase_field\" : \"note\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 0,\n" +
                        "          \"in_order\" : true\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testCVSIX_2551_EmbeddedWildcard() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:c-note*s", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("CVSIX_2551",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"c\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_multi\" : {\n" +
                        "              \"match\" : {\n" +
                        "                \"wildcard\" : {\n" +
                        "                  \"phrase_field\" : \"note*s\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 0,\n" +
                        "          \"in_order\" : true\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testCVSIX_2551_Fuzzy() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:c-note~2", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("CVSIX_2551",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"c\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_multi\" : {\n" +
                        "              \"match\" : {\n" +
                        "                \"fuzzy\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"note\",\n" +
                        "                    \"prefix_length\" : 2\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 0,\n" +
                        "          \"in_order\" : true\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testReverseOfCVSIX_2551() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "exact_field:c-note", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("ReverseOfCVSIX_2551",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"term\" : {\n" +
                        "        \"exact_field\" : \"c-note\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testCVSIX_2551_WithProximity() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:(c-note w/3 beer)", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("CVSIX_2551_WithProximity",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_near\" : {\n" +
                        "              \"clauses\" : [ {\n" +
                        "                \"span_term\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"c\"\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              }, {\n" +
                        "                \"span_term\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"note\"\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              } ],\n" +
                        "              \"slop\" : 0,\n" +
                        "              \"in_order\" : true\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"beer\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 3,\n" +
                        "          \"in_order\" : false\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testCVSIX_2551_subjectsStarsSymbol002() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "( phrase_field : \"Qwerty \\*FREE Samples\\*\" )", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("testCVSIX_2551_subjectsStarsSymbol002",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"query\" : \"qwerty *free samples*\",\n" +
                        "            \"type\" : \"phrase\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testCVSIX_2577() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "( phrase_field: \"cut-over\" OR phrase_field: \"get-prices\" )", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("testCVSIX_2577",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"should\" : [ {\n" +
                        "          \"query\" : {\n" +
                        "            \"match\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"query\" : \"cut-over\",\n" +
                        "                \"type\" : \"phrase\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"query\" : {\n" +
                        "            \"match\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"query\" : \"get-prices\",\n" +
                        "                \"type\" : \"phrase\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testTermRollups() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "id: 100 OR id: 200", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("testCVSIX_2521",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"terms\" : {\n" +
                        "        \"id\" : [ 100, 200 ],\n" +
                        "        \"execution\" : \"plain\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testCVSIX_2521() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "id < 100 OR id < 100", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("testCVSIX_2521",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"should\" : [ {\n" +
                        "          \"range\" : {\n" +
                        "            \"id\" : {\n" +
                        "              \"from\" : null,\n" +
                        "              \"to\" : 100,\n" +
                        "              \"include_lower\" : true,\n" +
                        "              \"include_upper\" : false\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"range\" : {\n" +
                        "            \"id\" : {\n" +
                        "              \"from\" : null,\n" +
                        "              \"to\" : 100,\n" +
                        "              \"include_lower\" : true,\n" +
                        "              \"include_upper\" : false\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testCVSIX_2578_AnyBareWildcard() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:'V - A - C - A - T - I - O * N'", false, true);

        try {
            qr.rewriteQuery().toString();
            fail("CVSIX_2578 is supposed to throw an exception");
        } catch (QueryRewriter.QueryRewriteException qre) {
            assertEquals("testCVSIX_2578_AnyBareWildcard", "Bare wildcards not supported within phrases", qre.getMessage());
        }
    }

    @Test
    public void testCVSIX_2578_SingleBareWildcard() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:'V - A - C - A -?'", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("testCVSIX_2578_SingleBareWildcard",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"v\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"a\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"c\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"a\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_multi\" : {\n" +
                        "              \"match\" : {\n" +
                        "                \"wildcard\" : {\n" +
                        "                  \"phrase_field\" : \"-?\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 0,\n" +
                        "          \"in_order\" : true\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testCVSIX_2578_AnyWildcard() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:'V - A - C - A - T - I - O* N'", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("testCVSIX_2578_AnyWildcard",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"v\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"a\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"c\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"a\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"t\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"i\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_multi\" : {\n" +
                        "              \"match\" : {\n" +
                        "                \"prefix\" : {\n" +
                        "                  \"phrase_field\" : \"o\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"n\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 0,\n" +
                        "          \"in_order\" : true\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testCVSIX_2578_SingleWildcard() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:'V - A - C - A - T - I?'", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("testCVSIX_2578_SingleWildcard",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"v\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"a\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"c\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"a\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"t\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_multi\" : {\n" +
                        "              \"match\" : {\n" +
                        "                \"wildcard\" : {\n" +
                        "                  \"phrase_field\" : \"i?\"\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 0,\n" +
                        "          \"in_order\" : true\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_subjectsFuzzyPhrase_001() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "( phrase_field : \"choose prize your!~2\"~!0 )", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_subjectsFuzzyPhrase_001",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"choose\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"prize\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_multi\" : {\n" +
                        "              \"match\" : {\n" +
                        "                \"fuzzy\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"your!\",\n" +
                        "                    \"prefix_length\" : 2\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 0,\n" +
                        "          \"in_order\" : false\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2579() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:(4-13-01* w/15 Weekend w/15 outage)", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_CVSIX_2579",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_near\" : {\n" +
                        "              \"clauses\" : [ {\n" +
                        "                \"span_term\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"4\"\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              }, {\n" +
                        "                \"span_term\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"13\"\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              }, {\n" +
                        "                \"span_multi\" : {\n" +
                        "                  \"match\" : {\n" +
                        "                    \"prefix\" : {\n" +
                        "                      \"phrase_field\" : \"01\"\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              } ],\n" +
                        "              \"slop\" : 0,\n" +
                        "              \"in_order\" : true\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_near\" : {\n" +
                        "              \"clauses\" : [ {\n" +
                        "                \"span_term\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"weekend\"\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              }, {\n" +
                        "                \"span_term\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"outage\"\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              } ],\n" +
                        "              \"slop\" : 15,\n" +
                        "              \"in_order\" : false\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 15,\n" +
                        "          \"in_order\" : false\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2591() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "exact_field:\"2001-*\"", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("CVSIX-2591",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"prefix\" : {\n" +
                        "        \"exact_field\" : \"2001-\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2835_prefix() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "(exact_field = \"bob dol*\")", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("CVSIX-2835_prefix",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"prefix\" : {\n" +
                        "        \"exact_field\" : \"bob dol\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2835_wildcard() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "(exact_field = \"bob* dol*\")", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("CVSIX-2835_wildcard",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"wildcard\" : {\n" +
                        "          \"exact_field\" : \"bob* dol*\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2835_fuzzy() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "(exact_field = \"bob dol~\")", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("CVSIX-2835_fuzzy",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"fuzzy\" : {\n" +
                        "          \"exact_field\" : {\n" +
                        "            \"value\" : \"bob dol\",\n" +
                        "            \"prefix_length\" : 3\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2807() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "(exact_field <> \"bob*\")", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("CVSIX-2807",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"not\" : {\n" +
                        "        \"filter\" : {\n" +
                        "          \"prefix\" : {\n" +
                        "            \"exact_field\" : \"bob\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2807_phrase() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "(phrase_field <> \"bob*\")", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("CVSIX-2807_phrase",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"not\" : {\n" +
                        "        \"filter\" : {\n" +
                        "          \"prefix\" : {\n" +
                        "            \"phrase_field\" : \"bob\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2682() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "( phrase_field:(more w/10 \"food\\*\") )", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("CVSIX-2682",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"more\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_near\" : {\n" +
                        "              \"clauses\" : [ {\n" +
                        "                \"span_near\" : {\n" +
                        "                  \"clauses\" : [ {\n" +
                        "                    \"span_term\" : {\n" +
                        "                      \"phrase_field\" : {\n" +
                        "                        \"value\" : \"food*\"\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  } ],\n" +
                        "                  \"slop\" : 0,\n" +
                        "                  \"in_order\" : true\n" +
                        "                }\n" +
                        "              } ],\n" +
                        "              \"slop\" : 0,\n" +
                        "              \"in_order\" : true\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 10,\n" +
                        "          \"in_order\" : false\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2579_WithQuotes() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:(\"4-13-01*\" w/15 Weekend w/15 outage)", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_CVSIX_2579_WithQuotes",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_near\" : {\n" +
                        "              \"clauses\" : [ {\n" +
                        "                \"span_near\" : {\n" +
                        "                  \"clauses\" : [ {\n" +
                        "                    \"span_term\" : {\n" +
                        "                      \"phrase_field\" : {\n" +
                        "                        \"value\" : \"4\"\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  }, {\n" +
                        "                    \"span_term\" : {\n" +
                        "                      \"phrase_field\" : {\n" +
                        "                        \"value\" : \"13\"\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  }, {\n" +
                        "                    \"span_multi\" : {\n" +
                        "                      \"match\" : {\n" +
                        "                        \"prefix\" : {\n" +
                        "                          \"phrase_field\" : \"01\"\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  } ],\n" +
                        "                  \"slop\" : 0,\n" +
                        "                  \"in_order\" : true\n" +
                        "                }\n" +
                        "              } ],\n" +
                        "              \"slop\" : 0,\n" +
                        "              \"in_order\" : true\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_near\" : {\n" +
                        "              \"clauses\" : [ {\n" +
                        "                \"span_term\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"weekend\"\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              }, {\n" +
                        "                \"span_term\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"outage\"\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              } ],\n" +
                        "              \"slop\" : 15,\n" +
                        "              \"in_order\" : false\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 15,\n" +
                        "          \"in_order\" : false\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_MergeLiterals_AND() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "exact_field:(one & two & three)", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_MergeLiterals_AND",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"terms\" : {\n" +
                        "        \"exact_field\" : [ \"one\", \"two\", \"three\" ],\n" +
                        "        \"execution\" : \"and\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_MergeLiterals_AND_NE() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "exact_field<>(one & two & three)", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_MergeLiterals_AND_NE",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"not\" : {\n" +
                        "        \"filter\" : {\n" +
                        "          \"terms\" : {\n" +
                        "            \"exact_field\" : [ \"one\", \"two\", \"three\" ],\n" +
                        "            \"execution\" : \"plain\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_MergeLiterals_OR_NE() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "exact_field<>(one , two , three)", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_MergeLiterals_OR_NE",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"not\" : {\n" +
                        "        \"filter\" : {\n" +
                        "          \"terms\" : {\n" +
                        "            \"exact_field\" : [ \"one\", \"two\", \"three\" ],\n" +
                        "            \"execution\" : \"and\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_MergeLiterals_OR() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "exact_field:(one , two , three)", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_MergeLiterals_OR",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"terms\" : {\n" +
                        "        \"exact_field\" : [ \"one\", \"two\", \"three\" ],\n" +
                        "        \"execution\" : \"plain\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_MergeLiterals_AND_WithArrays() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "exact_field:(one & two & three & [four,five,six])", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_MergeLiterals_AND_WithArrays",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"terms\" : {\n" +
                        "            \"exact_field\" : [ \"one\", \"two\", \"three\" ],\n" +
                        "            \"execution\" : \"and\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"terms\" : {\n" +
                        "            \"exact_field\" : [ \"four\", \"five\", \"six\" ],\n" +
                        "            \"execution\" : \"plain\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_MergeLiterals_OR_WithArrays() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "exact_field:(one , two , three , [four,five,six])", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_MergeLiterals_OR_WithArrays",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"terms\" : {\n" +
                        "        \"exact_field\" : [ \"one\", \"two\", \"three\", \"four\", \"five\", \"six\" ],\n" +
                        "        \"execution\" : \"plain\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_MergeLiterals_AND_OR_WithArrays() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "exact_field:(one , two , three , [four,five,six] & one & two & three & [four, five, six])", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_MergeLiterals_AND_OR_WithArrays",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"should\" : [ {\n" +
                        "          \"terms\" : {\n" +
                        "            \"exact_field\" : [ \"one\", \"two\", \"three\" ],\n" +
                        "            \"execution\" : \"plain\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"terms\" : {\n" +
                        "                \"exact_field\" : [ \"four\", \"five\", \"six\", \"four\", \"five\", \"six\" ],\n" +
                        "                \"execution\" : \"plain\"\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"terms\" : {\n" +
                        "                \"exact_field\" : [ \"one\", \"two\", \"three\" ],\n" +
                        "                \"execution\" : \"and\"\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2941_DoesntWork() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "( ( ( review_data_ben.coding.responsiveness = Responsive OR review_data_ben.coding.responsiveness = \"Potentially Responsive\" OR review_data_ben.coding.responsiveness = \"Not Responsive\" OR review_data_ben.coding.responsiveness = Unreviewable ) AND review_data_ben.review_data_id = 67115 ) )", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_CVSIX_2941_DoesntWork",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"nested\" : {\n" +
                        "        \"filter\" : {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"bool\" : {\n" +
                        "                \"should\" : [ {\n" +
                        "                  \"terms\" : {\n" +
                        "                    \"review_data_ben.coding.responsiveness\" : [ \"responsive\", \"unreviewable\" ],\n" +
                        "                    \"execution\" : \"plain\"\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"review_data_ben.coding.responsiveness\" : \"potentially responsive\"\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"review_data_ben.coding.responsiveness\" : \"not responsive\"\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"review_data_ben.review_data_id\" : 67115\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"join\" : true,\n" +
                        "        \"path\" : \"review_data_ben\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2941_DoesWork() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "( (review_data_ben.review_data_id = 67115 AND ( review_data_ben.coding.responsiveness = Responsive OR review_data_ben.coding.responsiveness = \"Potentially Responsive\" OR review_data_ben.coding.responsiveness = \"Not Responsive\" OR review_data_ben.coding.responsiveness = Unreviewable  ) ) )", false, true);
        json = qr.rewriteQuery().toString();

        assertEquals("test_CVSIX_2941_DoesWork",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"nested\" : {\n" +
                        "        \"filter\" : {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"term\" : {\n" +
                        "                \"review_data_ben.review_data_id\" : 67115\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"bool\" : {\n" +
                        "                \"should\" : [ {\n" +
                        "                  \"terms\" : {\n" +
                        "                    \"review_data_ben.coding.responsiveness\" : [ \"responsive\", \"unreviewable\" ],\n" +
                        "                    \"execution\" : \"plain\"\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"review_data_ben.coding.responsiveness\" : \"potentially responsive\"\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"review_data_ben.coding.responsiveness\" : \"not responsive\"\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"join\" : true,\n" +
                        "        \"path\" : \"review_data_ben\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

   @Test
    public void test_CVSIX_2941_NestedGroupRollupWithNonNestedFields_AND() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request,
                "(review_data.subject:(first wine cheese food foo bar) and field:drink and review_data.subject:wine and review_data.subject:last and field:food) ",
                false, true);
        json = qr.rewriteQuery().toString();

        assertEquals("testNestedGroupRollupWithNonNestedFields_AND",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"terms\" : {\n" +
                        "            \"field\" : [ \"drink\", \"food\" ],\n" +
                        "            \"execution\" : \"and\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"nested\" : {\n" +
                        "            \"filter\" : {\n" +
                        "              \"terms\" : {\n" +
                        "                \"review_data.subject\" : [ \"wine\", \"last\", \"first\", \"wine\", \"cheese\", \"food\", \"foo\", \"bar\" ],\n" +
                        "                \"execution\" : \"and\"\n" +
                        "              }\n" +
                        "            },\n" +
                        "            \"join\" : true,\n" +
                        "            \"path\" : \"review_data\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

   @Test
    public void test_CVSIX_2941_NestedGroupRollupWithNonNestedFields_OR() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request,
                "(review_data.subject:(first, wine, cheese, food, foo, bar) or field:food and review_data.subject:wine or review_data.subject:last) or field:drink ",
                false, true);
        json = qr.rewriteQuery().toString();

        assertEquals("testNestedGroupRollupWithNonNestedFields_OR",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"should\" : [ {\n" +
                        "          \"term\" : {\n" +
                        "            \"field\" : \"drink\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must\" : [ {\n" +
                        "              \"term\" : {\n" +
                        "                \"field\" : \"food\"\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"nested\" : {\n" +
                        "                \"filter\" : {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"review_data.subject\" : \"wine\"\n" +
                        "                  }\n" +
                        "                },\n" +
                        "                \"join\" : true,\n" +
                        "                \"path\" : \"review_data\"\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"nested\" : {\n" +
                        "            \"filter\" : {\n" +
                        "              \"terms\" : {\n" +
                        "                \"review_data.subject\" : [ \"last\", \"first\", \"wine\", \"cheese\", \"food\", \"foo\", \"bar\" ],\n" +
                        "                \"execution\" : \"plain\"\n" +
                        "              }\n" +
                        "            },\n" +
                        "            \"join\" : true,\n" +
                        "            \"path\" : \"review_data\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2941_NestedGroupRollupWithNonNestedFields_BOTH() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request,
                "(review_data.subject:(first wine cheese food foo bar) and review_data.subject:wine and review_data.subject:last and field:food) (review_data.subject:(first, wine, cheese, food, foo, bar) or review_data.subject:wine or review_data.subject:last)",
                false, true);
        json = qr.rewriteQuery().toString();

        assertEquals("testNestedGroupRollupWithNonNestedFields_BOTH",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"term\" : {\n" +
                        "            \"field\" : \"food\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"nested\" : {\n" +
                        "            \"filter\" : {\n" +
                        "              \"bool\" : {\n" +
                        "                \"must\" : [ {\n" +
                        "                  \"terms\" : {\n" +
                        "                    \"review_data.subject\" : [ \"wine\", \"last\", \"first\", \"wine\", \"cheese\", \"food\", \"foo\", \"bar\" ],\n" +
                        "                    \"execution\" : \"plain\"\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"terms\" : {\n" +
                        "                    \"review_data.subject\" : [ \"wine\", \"last\", \"first\", \"wine\", \"cheese\", \"food\", \"foo\", \"bar\" ],\n" +
                        "                    \"execution\" : \"and\"\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            },\n" +
                        "            \"join\" : true,\n" +
                        "            \"path\" : \"review_data\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2941_NestedGroupRollup() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "beer and ( ( ((review_data.owner_username=E_RIDGE AND review_data.status_name:[\"REVIEW_UPDATED\",\"REVIEW_CHECKED_OUT\"]) OR (review_data.status_name:REVIEW_READY)) AND review_data.project_id = 1040 ) ) ", false, true);
        json = qr.rewriteQuery().toString();

        assertEquals("test_CVSIX_2941_NestedGroupRollup",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"bool\" : {\n" +
                        "            \"should\" : [ {\n" +
                        "              \"term\" : {\n" +
                        "                \"fulltext_field\" : \"beer\"\n" +
                        "              }\n" +
                        "            }, {\n" +
                        "              \"term\" : {\n" +
                        "                \"_all\" : \"beer\"\n" +
                        "              }\n" +
                        "            } ]\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"nested\" : {\n" +
                        "            \"filter\" : {\n" +
                        "              \"bool\" : {\n" +
                        "                \"must\" : [ {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"should\" : [ {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"must\" : [ {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"review_data.owner_username\" : \"e_ridge\"\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"terms\" : {\n" +
                        "                            \"review_data.status_name\" : [ \"review_updated\", \"review_checked_out\" ],\n" +
                        "                            \"execution\" : \"plain\"\n" +
                        "                          }\n" +
                        "                        } ]\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"review_data.status_name\" : \"review_ready\"\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"review_data.project_id\" : 1040\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            },\n" +
                        "            \"join\" : true,\n" +
                        "            \"path\" : \"review_data\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2941_NestedGroupRollupInChild() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "#child<data>(( ( ((review_data.owner_username=E_RIDGE AND review_data.status_name:[\"REVIEW_UPDATED\",\"REVIEW_CHECKED_OUT\"]) OR (review_data.status_name:REVIEW_READY)) AND review_data.project_id = 1040 ) ) )", false, true);
        json = qr.rewriteQuery().toString();

        assertEquals("test_CVSIX_2941_NestedGroupRollupInChild",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"has_child\" : {\n" +
                        "        \"filter\" : {\n" +
                        "          \"nested\" : {\n" +
                        "            \"filter\" : {\n" +
                        "              \"bool\" : {\n" +
                        "                \"must\" : [ {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"should\" : [ {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"must\" : [ {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"review_data.owner_username\" : \"e_ridge\"\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"terms\" : {\n" +
                        "                            \"review_data.status_name\" : [ \"review_updated\", \"review_checked_out\" ],\n" +
                        "                            \"execution\" : \"plain\"\n" +
                        "                          }\n" +
                        "                        } ]\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"review_data.status_name\" : \"review_ready\"\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"review_data.project_id\" : 1040\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            },\n" +
                        "            \"join\" : true,\n" +
                        "            \"path\" : \"review_data\"\n" +
                        "          }\n" +
                        "        },\n" +
                        "        \"child_type\" : \"data\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2941_Aggregate() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "#tally(review_data_ridge.coding.responsiveness, \"^.*\", 5000, \"term\") #options(id=<main_ft.idxmain_ft>ft_id, id=<main_vol.idxmain_vol>vol_id, id=<main_other.idxmain_other>other_id) #parent<xact>((((_xmin = 6249019 AND _cmin < 0 AND (_xmax = 0 OR (_xmax = 6249019 AND _cmax >= 0))) OR (_xmin_is_committed = true AND (_xmax = 0 OR (_xmax = 6249019 AND _cmax >= 0) OR (_xmax <> 6249019 AND _xmax_is_committed = false)))))) AND ((review_data_ridge.review_set_name:\"test\"))", false, true);
        json = qr.rewriteQuery().toString();

        assertEquals("test_CVSIX_2941_Aggregate",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"has_parent\" : {\n" +
                        "            \"filter\" : {\n" +
                        "              \"bool\" : {\n" +
                        "                \"should\" : [ {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : [ {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmin\" : 6249019\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"range\" : {\n" +
                        "                        \"_cmin\" : {\n" +
                        "                          \"from\" : null,\n" +
                        "                          \"to\" : 0,\n" +
                        "                          \"include_lower\" : true,\n" +
                        "                          \"include_upper\" : false\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"should\" : [ {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"_xmax\" : 0\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"bool\" : {\n" +
                        "                            \"must\" : [ {\n" +
                        "                              \"term\" : {\n" +
                        "                                \"_xmax\" : 6249019\n" +
                        "                              }\n" +
                        "                            }, {\n" +
                        "                              \"range\" : {\n" +
                        "                                \"_cmax\" : {\n" +
                        "                                  \"from\" : 0,\n" +
                        "                                  \"to\" : null,\n" +
                        "                                  \"include_lower\" : true,\n" +
                        "                                  \"include_upper\" : true\n" +
                        "                                }\n" +
                        "                              }\n" +
                        "                            } ]\n" +
                        "                          }\n" +
                        "                        } ]\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : [ {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmin_is_committed\" : true\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"should\" : [ {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"_xmax\" : 0\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"bool\" : {\n" +
                        "                            \"must\" : [ {\n" +
                        "                              \"term\" : {\n" +
                        "                                \"_xmax\" : 6249019\n" +
                        "                              }\n" +
                        "                            }, {\n" +
                        "                              \"range\" : {\n" +
                        "                                \"_cmax\" : {\n" +
                        "                                  \"from\" : 0,\n" +
                        "                                  \"to\" : null,\n" +
                        "                                  \"include_lower\" : true,\n" +
                        "                                  \"include_upper\" : true\n" +
                        "                                }\n" +
                        "                              }\n" +
                        "                            } ]\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"bool\" : {\n" +
                        "                            \"must\" : [ {\n" +
                        "                              \"not\" : {\n" +
                        "                                \"filter\" : {\n" +
                        "                                  \"term\" : {\n" +
                        "                                    \"_xmax\" : 6249019\n" +
                        "                                  }\n" +
                        "                                }\n" +
                        "                              }\n" +
                        "                            }, {\n" +
                        "                              \"term\" : {\n" +
                        "                                \"_xmax_is_committed\" : false\n" +
                        "                              }\n" +
                        "                            } ]\n" +
                        "                          }\n" +
                        "                        } ]\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            },\n" +
                        "            \"parent_type\" : \"xact\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"nested\" : {\n" +
                        "            \"filter\" : {\n" +
                        "              \"term\" : {\n" +
                        "                \"review_data_ridge.review_set_name\" : \"test\"\n" +
                        "              }\n" +
                        "            },\n" +
                        "            \"join\" : true,\n" +
                        "            \"path\" : \"review_data_ridge\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_RegexWord() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:~'\\d{2}'", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_RegeWord",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"regexp\" : {\n" +
                        "        \"phrase_field\" : \"\\\\d{2}\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_RegexPhrase() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:~'\\d{2} \\d{3}'", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_RegexPhrase",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"regexp\" : {\n" +
                        "        \"phrase_field\" : \"\\\\d{2} \\\\d{3}\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_RegexProximity() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:~ ('[0-9]{2}' w/3 '[0-9]{3}')", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_RegexPhrase",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_multi\" : {\n" +
                        "              \"match\" : {\n" +
                        "                \"regexp\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"[0-9]{2}\"\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_multi\" : {\n" +
                        "              \"match\" : {\n" +
                        "                \"regexp\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"[0-9]{3}\"\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 3,\n" +
                        "          \"in_order\" : false\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2755() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "exact_field = \"CRAZY W/5 PEOPLE W/15 FOREVER W/25 (\\\"EYE UP\\\")\"", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_CVSIX_2755",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"term\" : {\n" +
                        "        \"exact_field\" : \"crazy w/5 people w/15 forever w/25 (\\\"eye up\\\")\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2755_WithSingleQuotes() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "exact_field = 'CRAZY W/5 PEOPLE W/15 FOREVER W/25 (\"EYE UP\")'", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_CVSIX_2755",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"term\" : {\n" +
                        "        \"exact_field\" : \"crazy w/5 people w/15 forever w/25 (\\\"eye up\\\")\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2770_exact() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "exact_field = \"\\\"NOTES:KARO\\?\\?\\?\\?\\?\\?\\?\"  ", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_CVSIX_2770_exact",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"term\" : {\n" +
                        "        \"exact_field\" : \"\\\"notes:karo???????\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2770_phrase() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field = \"\\\"NOTES:KARO\\?\\?\\?\\?\\?\\?\\?\"  ", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_CVSIX_2770_phrase",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"match\" : {\n" +
                        "          \"phrase_field\" : {\n" +
                        "            \"query\" : \"\\\"notes:karo???????\",\n" +
                        "            \"type\" : \"phrase\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_2766() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "exact_field:\"7 KING\\'S BENCH WALK\"", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_CVSIX_2766",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"term\" : {\n" +
                        "        \"exact_field\" : \"7 king's bench walk\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_CVSIX_914() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "\"xxxx17.0000000001.0000000001.0000000001.M.00.0000000-0000000\"", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_CVSIX_914",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"should\" : [ {\n" +
                        "          \"query\" : {\n" +
                        "            \"match\" : {\n" +
                        "              \"fulltext_field\" : {\n" +
                        "                \"query\" : \"xxxx17.0000000001.0000000001.0000000001.m.00.0000000-0000000\",\n" +
                        "                \"type\" : \"phrase\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"query\" : {\n" +
                        "            \"match\" : {\n" +
                        "              \"_all\" : {\n" +
                        "                \"query\" : \"xxxx17.0000000001.0000000001.0000000001.m.00.0000000-0000000\",\n" +
                        "                \"type\" : \"phrase\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void test_FlattenParentChild() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "a:1 and #child<data>(field:value or field2:value or field:value2)", false, false);

        json = qr.rewriteQuery().toString();

        assertEquals("test_FlattenParentChild",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"should\" : [ {\n" +
                        "          \"terms\" : {\n" +
                        "            \"field\" : [ \"value\", \"value2\" ],\n" +
                        "            \"execution\" : \"plain\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"term\" : {\n" +
                        "            \"field2\" : \"value\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testMixedFieldnamesNoProx() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();

        qr = new QueryRewriter(mock.client, mock.request, "outside:(one and inside:two)", true, true);

        assertEquals("testMixedFieldnamesNoProx",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      null=<schema.table.idxname>null\n" +
                        "      And\n" +
                        "         Word (fieldname=outside, operator=CONTAINS, value=one, index=schema.table.idxname)\n" +
                        "         Word (fieldname=inside, operator=CONTAINS, value=two, index=schema.table.idxname)\n",
                qr.dumpAsString());
    }

    @Test
    public void testMixedFieldnamesWithProx() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();

        qr = new QueryRewriter(mock.client, mock.request, "outside:(one w/4 (inside:two))", true, true);

        assertEquals("testMixedFieldnamesWithProx",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      null=<schema.table.idxname>null\n" +
                        "      Proximity (fieldname=outside, operator=CONTAINS, distance=4, index=schema.table.idxname)\n" +
                        "         Word (fieldname=outside, operator=CONTAINS, value=one, index=schema.table.idxname)\n" +
                        "         Word (fieldname=inside, operator=CONTAINS, value=two, index=schema.table.idxname)\n",
                qr.dumpAsString());
    }

    @Test
    public void testProximalProximity() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:( ('1 3' w/2 '2 3') w/4 (get w/8 out) )", true, true);

        json = qr.rewriteQuery().toString();

        assertEquals("textProximalProximity-AST",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      null=<schema.table.idxname>null\n" +
                        "      Proximity (fieldname=phrase_field, operator=CONTAINS, distance=4, index=schema.table.idxname)\n" +
                        "         Proximity (fieldname=phrase_field, operator=CONTAINS, distance=2, index=schema.table.idxname)\n" +
                        "            Phrase (fieldname=phrase_field, operator=CONTAINS, value=1 3, ordered=true, index=schema.table.idxname)\n" +
                        "            Phrase (fieldname=phrase_field, operator=CONTAINS, value=2 3, ordered=true, index=schema.table.idxname)\n" +
                        "         Proximity (fieldname=phrase_field, operator=CONTAINS, distance=8, index=schema.table.idxname)\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=get, index=schema.table.idxname)\n" +
                        "            Word (fieldname=phrase_field, operator=CONTAINS, value=out, index=schema.table.idxname)\n",
                qr.dumpAsString());

        assertEquals("textProximalProximity-json",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_near\" : {\n" +
                        "              \"clauses\" : [ {\n" +
                        "                \"span_near\" : {\n" +
                        "                  \"clauses\" : [ {\n" +
                        "                    \"span_term\" : {\n" +
                        "                      \"phrase_field\" : {\n" +
                        "                        \"value\" : \"1\"\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  }, {\n" +
                        "                    \"span_term\" : {\n" +
                        "                      \"phrase_field\" : {\n" +
                        "                        \"value\" : \"3\"\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  } ],\n" +
                        "                  \"slop\" : 0,\n" +
                        "                  \"in_order\" : true\n" +
                        "                }\n" +
                        "              }, {\n" +
                        "                \"span_near\" : {\n" +
                        "                  \"clauses\" : [ {\n" +
                        "                    \"span_term\" : {\n" +
                        "                      \"phrase_field\" : {\n" +
                        "                        \"value\" : \"2\"\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  }, {\n" +
                        "                    \"span_term\" : {\n" +
                        "                      \"phrase_field\" : {\n" +
                        "                        \"value\" : \"3\"\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  } ],\n" +
                        "                  \"slop\" : 0,\n" +
                        "                  \"in_order\" : true\n" +
                        "                }\n" +
                        "              } ],\n" +
                        "              \"slop\" : 2,\n" +
                        "              \"in_order\" : false\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_near\" : {\n" +
                        "              \"clauses\" : [ {\n" +
                        "                \"span_term\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"get\"\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              }, {\n" +
                        "                \"span_term\" : {\n" +
                        "                  \"phrase_field\" : {\n" +
                        "                    \"value\" : \"out\"\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              } ],\n" +
                        "              \"slop\" : 8,\n" +
                        "              \"in_order\" : false\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 4,\n" +
                        "          \"in_order\" : false\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testProximalProximity2() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();

        qr = new QueryRewriter(mock.client, mock.request, "fulltext:( (\"K F\" w/1 \"D T\") w/7 \"KN\" )", true, true);

        assertEquals("textProximalProximity2",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      null=<schema.table.idxname>null\n" +
                        "      Proximity (fieldname=fulltext, operator=CONTAINS, distance=7, index=schema.table.idxname)\n" +
                        "         Proximity (fieldname=fulltext, operator=CONTAINS, distance=1, index=schema.table.idxname)\n" +
                        "            Phrase (fieldname=fulltext, operator=CONTAINS, value=k f, ordered=true, index=schema.table.idxname)\n" +
                        "            Phrase (fieldname=fulltext, operator=CONTAINS, value=d t, ordered=true, index=schema.table.idxname)\n" +
                        "         Word (fieldname=fulltext, operator=CONTAINS, value=kn, index=schema.table.idxname)\n",
                qr.dumpAsString());
    }

    @Test
    public void test_AggregateQuery() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "#tally(cp_case_name, \"^.*\", 5000, \"term\") #options(fk_doc_cp_link_doc = <documents.es_idx_test_documents>pk_doc, fk_doc_cp_link_cp = <cases.es_idx_test_cases>pk_cp) #parent<xact>((((_xmin = 5353919 AND _cmin < 0 AND (_xmax = 0 OR (_xmax = 5353919 AND _cmax >= 0))) OR (_xmin_is_committed = true AND (_xmax = 0 OR (_xmax = 5353919 AND _cmax >= 0) OR (_xmax <> 5353919 AND _xmax_is_committed = false)))))) AND ((( ( pk_doc_cp = \"*\" ) )))", true, true);

        json = qr.rewriteQuery().toString();

        assertEquals("test_AggregateQuery",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"has_parent\" : {\n" +
                        "            \"filter\" : {\n" +
                        "              \"bool\" : {\n" +
                        "                \"should\" : [ {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : [ {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmin\" : 5353919\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"range\" : {\n" +
                        "                        \"_cmin\" : {\n" +
                        "                          \"from\" : null,\n" +
                        "                          \"to\" : 0,\n" +
                        "                          \"include_lower\" : true,\n" +
                        "                          \"include_upper\" : false\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"should\" : [ {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"_xmax\" : 0\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"bool\" : {\n" +
                        "                            \"must\" : [ {\n" +
                        "                              \"term\" : {\n" +
                        "                                \"_xmax\" : 5353919\n" +
                        "                              }\n" +
                        "                            }, {\n" +
                        "                              \"range\" : {\n" +
                        "                                \"_cmax\" : {\n" +
                        "                                  \"from\" : 0,\n" +
                        "                                  \"to\" : null,\n" +
                        "                                  \"include_lower\" : true,\n" +
                        "                                  \"include_upper\" : true\n" +
                        "                                }\n" +
                        "                              }\n" +
                        "                            } ]\n" +
                        "                          }\n" +
                        "                        } ]\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"bool\" : {\n" +
                        "                    \"must\" : [ {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"_xmin_is_committed\" : true\n" +
                        "                      }\n" +
                        "                    }, {\n" +
                        "                      \"bool\" : {\n" +
                        "                        \"should\" : [ {\n" +
                        "                          \"term\" : {\n" +
                        "                            \"_xmax\" : 0\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"bool\" : {\n" +
                        "                            \"must\" : [ {\n" +
                        "                              \"term\" : {\n" +
                        "                                \"_xmax\" : 5353919\n" +
                        "                              }\n" +
                        "                            }, {\n" +
                        "                              \"range\" : {\n" +
                        "                                \"_cmax\" : {\n" +
                        "                                  \"from\" : 0,\n" +
                        "                                  \"to\" : null,\n" +
                        "                                  \"include_lower\" : true,\n" +
                        "                                  \"include_upper\" : true\n" +
                        "                                }\n" +
                        "                              }\n" +
                        "                            } ]\n" +
                        "                          }\n" +
                        "                        }, {\n" +
                        "                          \"bool\" : {\n" +
                        "                            \"must\" : [ {\n" +
                        "                              \"not\" : {\n" +
                        "                                \"filter\" : {\n" +
                        "                                  \"term\" : {\n" +
                        "                                    \"_xmax\" : 5353919\n" +
                        "                                  }\n" +
                        "                                }\n" +
                        "                              }\n" +
                        "                            }, {\n" +
                        "                              \"term\" : {\n" +
                        "                                \"_xmax_is_committed\" : false\n" +
                        "                              }\n" +
                        "                            } ]\n" +
                        "                          }\n" +
                        "                        } ]\n" +
                        "                      }\n" +
                        "                    } ]\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            },\n" +
                        "            \"parent_type\" : \"xact\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"exists\" : {\n" +
                        "            \"field\" : \"pk_doc_cp\"\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testCVSIX_2886() throws Exception {
        assertEquals("testCVSIX_2886-1",
                "QueryTree\n" +
                        "   Phrase (fieldname=phrase_field, operator=CONTAINS, value=RESPONSIVE BATCH EDIT, ordered=true)\n",
                QueryRewriter.dumpAsString("phrase_field:[\"RESPONSIVE BATCH EDIT\"]"));

        assertEquals("testCVSIX_2886-2",
                "QueryTree\n" +
                        "   Array (fieldname=phrase_field, operator=CONTAINS) (OR)\n" +
                        "      Phrase (fieldname=phrase_field, operator=CONTAINS, value=RESPONSIVE BATCH EDIT, ordered=true)\n" +
                        "      Phrase (fieldname=phrase_field, operator=CONTAINS, value=Insider Trading, ordered=true)\n",
                QueryRewriter.dumpAsString("phrase_field:[\"RESPONSIVE BATCH EDIT\", \"Insider Trading\"]"));
    }

    @Test
    public void testCVSIX_2874() throws Exception {
        assertEquals("ttestCVSIX_2874",
                "QueryTree\n" +
                        "   Expansion\n" +
                        "      cvgroupid=<this.index>cvgroupid\n" +
                        "         LeftField (value=cvgroupid)\n" +
                        "         IndexName (value=this.index)\n" +
                        "         RightField (value=cvgroupid)\n" +
                        "      NestedGroup (fieldname=review_data_ridge, operator=CONTAINS)\n" +
                        "         Wildcard (fieldname=review_data_ridge.review_set_name, operator=CONTAINS, value=*beer*)\n",
                QueryRewriter.dumpAsString("( #expand<cvgroupid=<this.index>cvgroupid> ( ( ( review_data_ridge.review_set_name:*beer* ) ) ) )"));
    }

    @Test
    public void testCVSIX_2792() throws Exception {
        assertEquals("testCVSIX_2792",
                "QueryTree\n" +
                        "   And\n" +
                        "      NestedGroup (fieldname=cars, operator=CONTAINS)\n" +
                        "         And\n" +
                        "            Array (fieldname=cars.cid, operator=CONTAINS) (OR)\n" +
                        "               Word (fieldname=cars.cid, operator=CONTAINS, value=2)\n" +
                        "               Word (fieldname=cars.cid, operator=CONTAINS, value=3)\n" +
                        "               Word (fieldname=cars.cid, operator=CONTAINS, value=1)\n" +
                        "            Array (fieldname=cars.make, operator=CONTAINS) (OR)\n" +
                        "               Word (fieldname=cars.make, operator=CONTAINS, value=BUICK)\n" +
                        "               Word (fieldname=cars.make, operator=CONTAINS, value=CHEVY)\n" +
                        "               Word (fieldname=cars.make, operator=CONTAINS, value=FORD)\n" +
                        "               Word (fieldname=cars.make, operator=CONTAINS, value=VOLVO)\n" +
                        "      NestedGroup (fieldname=cp_agg_wit, operator=CONTAINS)\n" +
                        "         Array (fieldname=cp_agg_wit.cp_wit_link_witness_status, operator=CONTAINS) (OR)\n" +
                        "            Word (fieldname=cp_agg_wit.cp_wit_link_witness_status, operator=CONTAINS, value=ALIVE)\n" +
                        "            Word (fieldname=cp_agg_wit.cp_wit_link_witness_status, operator=CONTAINS, value=ICU)\n",
                QueryRewriter.dumpAsString("( ( (cp_agg_wit.cp_wit_link_witness_status:[\"ALIVE\",\"ICU\"]) AND ( (cars.cid:[\"2\",\"3\",\"1\"]) AND (cars.make:[\"BUICK\",\"CHEVY\",\"FORD\",\"VOLVO\"]) ) ) )"));
    }

    @Test
    public void testCVSIX_2990() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "phrase_field:(beer wo/5 wine)", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("testCVSIX_2990",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"query\" : {\n" +
                        "        \"span_near\" : {\n" +
                        "          \"clauses\" : [ {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"beer\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }, {\n" +
                        "            \"span_term\" : {\n" +
                        "              \"phrase_field\" : {\n" +
                        "                \"value\" : \"wine\"\n" +
                        "              }\n" +
                        "            }\n" +
                        "          } ],\n" +
                        "          \"slop\" : 5,\n" +
                        "          \"in_order\" : true\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
    }

    @Test
    public void testCVSIX_3030_ast() throws Exception {
        assertEquals("testCVSIX_3030_ast",
                "QueryTree\n" +
                        "   And\n" +
                        "      Phrase (fieldname=custodian, operator=EQ, value=QUERTY, SUSAN, ordered=true)\n" +
                        "      Not\n" +
                        "         Not\n" +
                        "            NestedGroup (fieldname=review_data_cv623beta, operator=CONTAINS)\n" +
                        "               Word (fieldname=review_data_cv623beta.state, operator=EQ, value=CAAT)\n" +
                        "            Word (fieldname=review_data_cv623beta.state, operator=EQ, value=DOOG)\n",
                QueryRewriter.dumpAsString("( ( custodian = \"QUERTY, SUSAN\" AND NOT NOT review_data_cv623beta.state = CAAT NOT review_data_cv623beta.state = DOOG ) )"));
    }

    @Test
    public void testCVSIX_3030_json() throws Exception {
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String json;

        qr = new QueryRewriter(mock.client, mock.request, "( ( custodian = \"QUERTY, SUSAN\" AND NOT NOT review_data_cv623beta.state = CAAT NOT review_data_cv623beta.state = DOOG ) )", false, true);

        json = qr.rewriteQuery().toString();

        assertEquals("testCVSIX_3030_json",
                "{\n" +
                        "  \"filtered\" : {\n" +
                        "    \"query\" : {\n" +
                        "      \"match_all\" : { }\n" +
                        "    },\n" +
                        "    \"filter\" : {\n" +
                        "      \"bool\" : {\n" +
                        "        \"must\" : [ {\n" +
                        "          \"term\" : {\n" +
                        "            \"custodian\" : \"querty, susan\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"bool\" : {\n" +
                        "            \"must_not\" : {\n" +
                        "              \"bool\" : {\n" +
                        "                \"must_not\" : [ {\n" +
                        "                  \"nested\" : {\n" +
                        "                    \"filter\" : {\n" +
                        "                      \"term\" : {\n" +
                        "                        \"review_data_cv623beta.state\" : \"caat\"\n" +
                        "                      }\n" +
                        "                    },\n" +
                        "                    \"join\" : true,\n" +
                        "                    \"path\" : \"review_data_cv623beta\"\n" +
                        "                  }\n" +
                        "                }, {\n" +
                        "                  \"term\" : {\n" +
                        "                    \"review_data_cv623beta.state\" : \"doog\"\n" +
                        "                  }\n" +
                        "                } ]\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        } ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                json);
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
        QueryRewriter qr;
        MockClientAndRequest mock = new MockClientAndRequest();
        String tree;

        qr = new QueryRewriter(mock.client, mock.request, "#options(user_data:(owner_user_id=<so_users.idxso_users>id), comment_data:(id=<so_comments.idxso_comments>post_id)) " +
                "(user_data.display_name:j* and comment_data.user_display_name:j*)", false, true);

        tree = qr.dumpAsString();

        assertEquals("testIssue_13",
                "QueryTree\n" +
                        "   Options\n" +
                        "      user_data:(owner_user_id=<schema.so_users.idxso_users>id)\n" +
                        "         LeftField (value=owner_user_id)\n" +
                        "         IndexName (value=schema.so_users.idxso_users)\n" +
                        "         RightField (value=id)\n" +
                        "      comment_data:(id=<schema.so_comments.idxso_comments>post_id)\n" +
                        "         LeftField (value=id)\n" +
                        "         IndexName (value=schema.so_comments.idxso_comments)\n" +
                        "         RightField (value=post_id)\n" +
                        "   And\n" +
                        "      Expansion\n" +
                        "         comment_data:(id=<schema.so_comments.idxso_comments>post_id)\n" +
                        "            LeftField (value=id)\n" +
                        "            IndexName (value=schema.so_comments.idxso_comments)\n" +
                        "            RightField (value=post_id)\n" +
                        "         Prefix (fieldname=comment_data.user_display_name, operator=CONTAINS, value=j, index=schema.so_comments.idxso_comments)\n" +
                        "      Expansion\n" +
                        "         user_data:(owner_user_id=<schema.so_users.idxso_users>id)\n" +
                        "            LeftField (value=owner_user_id)\n" +
                        "            IndexName (value=schema.so_users.idxso_users)\n" +
                        "            RightField (value=id)\n" +
                        "         Prefix (fieldname=user_data.display_name, operator=CONTAINS, value=j, index=schema.so_users.idxso_users)\n",
                tree);
    }

    @Test
    public void testIssue_37_RangeAggregateParsing() throws Exception {
        MockClientAndRequest mock = new MockClientAndRequest();
        AbstractAggregationBuilder aggregationBuilder;
        QueryRewriter qr;

        qr = new QueryRewriter(mock.client, mock.request, "#range(page_count, '[{\"key\":\"first\", \"to\":100}, {\"from\":100, \"to\":150}, {\"from\":150}]')", false, true);
        aggregationBuilder = qr.rewriteAggregations();

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
                aggregationBuilder.toXContent(JsonXContent.contentBuilder().prettyPrint(), null).string());
    }

    @Test
    public void testIssue_99_DateRangeAggregateParsing() throws Exception {
        MockClientAndRequest mock = new MockClientAndRequest();
        AbstractAggregationBuilder aggregationBuilder;
        QueryRewriter qr;

        qr = new QueryRewriter(mock.client, mock.request, "#range(sent, '[{\"key\": \"early\", \"to\":\"2009-01-01 00:00:00\"}, {\"from\":\"2009-01-01 00:00:00\", \"to\":\"2010-01-01 00:00:00\"}, {\"from\":\"2010-01-01 00:00:00\"}]')", false, true);
        aggregationBuilder = qr.rewriteAggregations();

        assertEquals("testIssue_99_DateRangeAggregateParsing",
                        "\n\"sent\"{\n" +
                        "  \"date_range\" : {\n" +
                        "    \"field\" : \"sent.date\",\n" +
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
                aggregationBuilder.toXContent(JsonXContent.contentBuilder().prettyPrint(), null).string());
    }
}
