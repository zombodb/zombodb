package llc.zombodb.fast_terms;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class ShardFastTermsRequest extends BroadcastShardRequest{
    private String index;
    private FastTermsRequest request;

    public ShardFastTermsRequest() {

    }

    public ShardFastTermsRequest(String index, ShardId shardId, FastTermsRequest request) {
        super(shardId, request);
        this.index = index;
        this.request = request;
    }

    public String getIndex() {
        return index;
    }

    public FastTermsRequest getRequest() {
        return request;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        request = FastTermsRequest.from(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        request.writeTo(out);
    }
}
