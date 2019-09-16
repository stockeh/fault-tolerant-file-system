package cs555.system.util;

/**
 * Interface defining the global constants between controller, client,
 * and chunk servers.
 *
 * @author stock
 *
 */
public interface Constants {

  final String SYSTEM_TYPE_REPLICATION = "replication";

  final String SYSTEM_TYPE_ERASURE = "erasure";

  final int CLIENT_ID = 0;

  final int SERVER_ID = 1;

  final byte SUCCESS = ( byte ) 200;

  final byte FAILURE = ( byte ) 500;

  final int CHUNK_SIZE = 64000;

  final int REPLICATION_CHUNK_SLICE_SIZE = 8000;

  final int ERASURE_TOTAL_SHARDS = 9;

  final int ERASURE_PARITY_SHARDS = 3;

  final int ERASURE_DATA_SHARDS = ERASURE_TOTAL_SHARDS - ERASURE_PARITY_SHARDS;

  final int ERASURE_SHARD_SIZE = CHUNK_SIZE / ERASURE_DATA_SHARDS + 1;

  final int NUMBER_OF_REPLICATIONS =
      Properties.SYSTEM_DESIGN_SCHEMA.equals( SYSTEM_TYPE_REPLICATION ) ? 3
          : ERASURE_TOTAL_SHARDS;
}
