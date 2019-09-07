package cs555.system.util;

/**
 * Interface defining the global constants between controller, client,
 * and chunk servers.
 *
 * @author stock
 *
 */
public interface Constants {

  final int CLIENT_ID = 0;

  final int CHUNK_ID = 1;

  final byte SUCCESS = ( byte ) 200;

  final byte FAILURE = ( byte ) 500;

  final int CHUNK_SIZE = 64000;

  final int CHUNK_SLICE_SIZE = 8000;

  final int NUMBER_OF_REPLICATIONS = 1;

  // Application properties specific constants.

  final String CONF_NAME = "application.properties";

  final String CONTROLLER_HOST =
      Configurations.getInstance().getProperty( "controller.host" );

  final String CONTROLLER_PORT =
      Configurations.getInstance().getProperty( "controller.port" );

  final String CLIENT_OUTBOUND_DIRECTORY =
      Configurations.getInstance().getProperty( "client.outbound.directory" );

  final String SYSTEM_DESIGN_SCHEMA =
      Configurations.getInstance().getProperty( "system.design.schema" );

}
