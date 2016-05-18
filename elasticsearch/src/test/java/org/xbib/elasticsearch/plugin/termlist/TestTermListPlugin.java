/**
 Copyright (C) 2011-2015 Jörg Prante

 Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 specific language governing permissions and limitations under the License.
 */
package org.xbib.elasticsearch.plugin.termlist;

import com.tcdi.zombodb.test.ZomboDBTestCase;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Test;
import org.xbib.elasticsearch.action.termlist.TermlistRequestBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.*;

public class TestTermListPlugin extends ZomboDBTestCase {

    @Test
    public void testPlugin() throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("content")
                .field("type", "string")
                .field("analzyer", "german")
                .endObject()
                .endObject()
                .endObject();
        CreateIndexRequestBuilder createIndexRequestBuilder = new CreateIndexRequestBuilder(client().admin().indices())
                .setIndex("test")
                .addMapping("docs", builder);
        createIndexRequestBuilder.execute().actionGet();
        for (int i = 0; i < 10; i++) {
            String content = join(makeList(), " ");
            //logger.info("{} -> {}", i, content);
            IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(client())
                    .setIndex("test")
                    .setType("docs")
                    .setId(Integer.toString(i))
                    .setSource("content", content);
            indexRequestBuilder.setRefresh(true).execute().actionGet();
        }
        TermlistRequestBuilder termlistRequestBuilder = new TermlistRequestBuilder(client());
        termlistRequestBuilder.execute().actionGet();
    }

    private List<String> makeList() throws IOException {
        InputStream in = getClass().getResourceAsStream("/navid-kermani.txt");
        String s = Streams.copyToString(new InputStreamReader(in, "UTF-8"));
        in.close();
        StringTokenizer tokenizer = new StringTokenizer(s);
        List<String> list = new LinkedList<String>();
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (!token.isEmpty()) {
                list.add(token);
            }
        }
        Random random = new Random();
        Collections.shuffle(list);
        return list.subList(0, Math.min(10, random.nextInt(list.size())));
    }

    private String join(List<String> s, String delimiter) {
        if (s == null || s.isEmpty()) return "";
        Iterator<String> it = s.iterator();
        StringBuilder builder = new StringBuilder(it.next());
        while (it.hasNext()) {
            builder.append(delimiter).append(it.next());
        }
        return builder.toString();
    }
}