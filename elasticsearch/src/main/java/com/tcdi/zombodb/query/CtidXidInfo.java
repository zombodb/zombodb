package com.tcdi.zombodb.query;

import org.apache.lucene.util.BytesRef;

class CtidXidInfo {
    final BytesRef id;
    final BytesRef prevctid;
    final long xid;

    CtidXidInfo(BytesRef id, BytesRef prevctid, long xid) {
        String ctid = id.utf8ToString().split("[#]")[1];
        this.id = new BytesRef("data#" + ctid);
        this.prevctid = prevctid;
        this.xid = xid;
    }
}
