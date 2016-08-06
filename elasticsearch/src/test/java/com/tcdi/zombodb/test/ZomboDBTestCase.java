/*
 * Portions Copyright 2015-2016 ZomboDB, LLC
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
package com.tcdi.zombodb.test;

import com.tcdi.zombodb.highlight.AnalyzedField;
import com.tcdi.zombodb.query_parser.QueryRewriter;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequestBuilder;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

// import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.*;

public abstract class ZomboDBTestCase {
    protected static final String DEFAULT_INDEX_NAME = "db.schema.table.index";

    private static final File HOME = new File("/tmp/zdb_es-" + System.currentTimeMillis());
    private static final File CONFIG = new File(HOME.getAbsolutePath() + "/config");
    private static final File LOGS = new File(HOME.getAbsolutePath() + "/logs");

    private static Node node;

    @BeforeClass
    public static void beforeClass() throws Exception {
        node = bootstrapElasticsearch();

        for (String indexName : new String[]{DEFAULT_INDEX_NAME, "db.schema.so_users.idxso_users", "db.schema.so_comments.idxso_comments"}) {
            createIndex(indexName);
        }
    }

    private static Node bootstrapElasticsearch() throws Exception {
        // copy our custom 'logging.xml' to the right place
        CONFIG.mkdirs();
        TestingHelper.copyFile(ZomboDBTestCase.class.getResourceAsStream("logging.yml"), new File(CONFIG.getAbsolutePath() + "/logging.yml"));

        // Settings settings = settingsBuilder()
        Settings settings = Settings.builder()
                .put("http.enabled", false)
                .put("network.host", "127.0.0.1")
                .put("cluster.name", "ZomboDB_JUnit_Cluster")
                .put("node.name", "tester")
                .put("path.home", HOME.getAbsolutePath())
                .put("path.conf", CONFIG.getAbsolutePath())
                .put("path.logs", LOGS.getAbsolutePath())
                .build();

        // make sure ES' logging system knows where to find our custom logging.xml
        LogConfigurator.configure(settings, true);

        // startup a standalone node to use for tests
        return nodeBuilder()
                .settings(settings)
                .local(true)
                // .loadConfigSettings(false)
                .node();
    }

    private static void createIndex(String indexName) throws Exception {
        String settings = resource(ZomboDBTestCase.class, indexName + "-mapping.json");

        // CreateIndexRequestBuilder builder = new CreateIndexRequestBuilder(client().admin().indices(), indexName);
        // CreateIndexRequestBuilder builder = new CreateIndexRequestBuilder(client(), CreateIndexAction.INSTANCE);
        CreateIndexRequestBuilder builder = client().admin().indices().prepareCreate(indexName);
        builder.setSource(settings);

        CreateIndexResponse response = client().admin().indices().create(builder.request()).get();
        response.writeTo(new OutputStreamStreamOutput(System.out));
        // client().admin().indices().flush(new FlushRequestBuilder(client().admin().indices()).setIndices(indexName).setForce(true).request()).get();
        client().admin().indices().flush(new FlushRequestBuilder(client(), FlushAction.INSTANCE).setIndices(indexName).setForce(true).request()).get();
        // client().admin().indices().refresh(new RefreshRequestBuilder(client().admin().indices()).setIndices(indexName).request()).get();
        client().admin().indices().refresh(new RefreshRequestBuilder(client(), RefreshAction.INSTANCE).setIndices(indexName).request()).get();
    }

    protected static Client client() {
        return node.client();
    }

    @AfterClass
    public static void afterClass() {
        try {
            node.close();
        } finally {
            TestingHelper.deleteDirectory(HOME);
        }
    }

    protected static String resource(Class relativeTo, String name) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(relativeTo.getResourceAsStream(name), "UTF-8"))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (sb.length() > 0) sb.append("\n");
                sb.append(line);
            }
            return sb.toString();
        }
    }


    protected QueryRewriter qr(String query) {
        return new QueryRewriter(client(), DEFAULT_INDEX_NAME, query, null, true, false) { /* anonymous implementation */ };
    }

    protected void assertJson(String query, String expectedJson) throws Exception {
        String res = qr(query).rewriteQuery().toString().trim();
        try{
            JSONAssert.assertEquals(expectedJson.trim(), res, false);
        } catch (AssertionError e) {
            e.printStackTrace();
            System.out.println("Result:");
            System.out.println(res);
            fail("JSONAssert exception: " + e.getMessage());
        }
    }

    protected void assertAST(String query, String expectedAST) throws Exception {
        assertEquals(expectedAST.trim(), toAST(query).trim());
    }

    protected String toJson(String query) {
        return qr(query).rewriteQuery().toString();
    }

    protected String toAST(String query) {
        return qr(query).dumpAsString();
    }

    protected void sortHighlightTokens(List<AnalyzedField.Token> highlights) {
        Collections.sort(highlights, new Comparator<AnalyzedField.Token>() {
            @Override
            public int compare(AnalyzedField.Token o1, AnalyzedField.Token o2) {
                int cmp = o1.getFieldName().compareTo(o2.getFieldName());
                return cmp == 0 ? o1.getPosition() - o2.getPosition() : cmp;
            }
        });
    }


}
