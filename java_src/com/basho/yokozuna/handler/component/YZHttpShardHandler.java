package com.basho.yokozuna.handler.component;

public class YZHttpShardHandler extends HttpShardHandler {

  @Override
  public void submit(final ShardRequest sreq, final String shard, final ModifiableSolrParams params) {
      // TODO: deal with _yz_fq
      System.out.println("HEY FUCKERS: " ++ params);
      super(sreq, shard, params);
  }
}
