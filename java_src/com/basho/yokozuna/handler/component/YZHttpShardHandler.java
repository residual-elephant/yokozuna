package com.basho.yokozuna.handler.component;

import org.apache.http.client.HttpClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.component.HttpShardHandler;
import org.apache.solr.handler.component.ShardRequest;

public class YZHttpShardHandler extends HttpShardHandler {

    public YZHttpShardHandler(YZHttpShardHandlerFactory shf, HttpClient client) {
        super(shf, client);
    }

    @Override
    public void submit(final ShardRequest sreq, final String shard, final ModifiableSolrParams params) {
        // TODO: deal with _yz_fq
        System.out.println("HEY FUCKERS: " + params);
        super.submit(sreq, shard, params);
    }
}
