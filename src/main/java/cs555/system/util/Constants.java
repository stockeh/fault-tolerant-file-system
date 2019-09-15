package cs555.system.util;

/**
 * Interface defining the global constants between controller, client,
 * and chunk servers.
 *
 * @author stock
 *
 */
public interface Constants {

  // Application properties

  final String SYSTEM_TYPE_REPLICATION = "replication";

  final String SYSTEM_TYPE_ERASURE = "erasure";

  final String CONF_NAME = "application.properties";

  final String CONTROLLER_HOST =
      Configurations.getInstance().getProperty( "controller.host" );

  final String CONTROLLER_PORT =
      Configurations.getInstance().getProperty( "controller.port" );

  final String CLIENT_OUTBOUND_DIRECTORY =
      Configurations.getInstance().getProperty( "client.outbound.directory" );

  final String SYSTEM_DESIGN_SCHEMA = Configurations.getInstance()
      .getProperty( "system.design.schema", SYSTEM_TYPE_REPLICATION );

  final boolean SYSTEM_DEBUG_MODE = Boolean.parseBoolean( Configurations
      .getInstance().getProperty( "system.debug.mode", "false" ) );

  // Application constants

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
      SYSTEM_DESIGN_SCHEMA.equals( SYSTEM_TYPE_REPLICATION ) ? 3
          : ERASURE_TOTAL_SHARDS;
}
