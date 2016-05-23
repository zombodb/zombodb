## SIREn Support

SIREn is an Elasticsearch plugin that provides cross-index join capabilities.  Read their [blog post here](http://siren.solutions/relational-joins-for-elasticsearch-the-siren-join-plugin/).

SIREn is open-source, licensed under the GNU AFFERO GENERAL PUBLIC LICENSE.  This may or may not be important to your deployment of the plugin.

If SIREn is installed in your Elasticsearch cluster, ZomboDB will automatically detect it and use it for any query that makes use of [index links](INDEX-OPTIONS.md#index-links).  Note that SIREn needs to be installed on every node in your cluster.

While ZomboDB has its own implementation capable of doing the same thing, SIREn is significantly faster and definitely makes the use case of searching/joining normalized data very compelling.

For details on SIREn, please see its [product page](http://siren.solutions/searchplugins/join/).