package llc.zombodb.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestCompactHashSet {

    @Test
    public void testSerialization() throws Exception {
        CompactHashSet set = new CompactHashSet();
        CompactHashSet set2;
        for (int i=0; i<632768; i++) {
            set.add(""+i);
        }
        set.add("a");
        set.add("b");
        set.add("c");
        set.add(null);
        set.remove("a");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(baos);
        set.writeTo(out);
        out.flush();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        StreamInput in = new InputStreamStreamInput(bais);
        set2 = new CompactHashSet(in);

        assertEquals(set, set2);
    }
}
