BEGIN;

CREATE TABLE issue_72 AS SELECT 0::bigint AS id, '"________________________________________________________________________\n_________________8Qlχ.\npùvwS½WD∂Ͻff0íȎ¾4rÕȐoU⊃dĖL4Bó §ÑþïHsbýRƯr48ϖGαFh®Ė0t4B 3€5eSY×1oȺËÔ\n¢ÊV82yÑȈTó2jNαw¥ÌGëTKXSΡFΦI GEQÝӦnuÒ0N7hUS 383ÈT8794Ӊ⇐5ÊÐȄφFÔ¶\njrÙÐB5qPKɆC9¤5Só4C9TQâΒe ßω1iDÖz3ßŘPc².ǙÔ×⊇ÀGôv5CS5«¡B!"'::zdb.fulltext;
ALTER TABLE issue_72 ADD PRIMARY KEY (id);
CREATE INDEX idxissue72 ON issue_72 USING zombodb((issue_72.*));

ABORT;