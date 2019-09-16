package cs555.system.util;

/**
 * Interface defining the properties for the application.
 *
 * @author stock
 *
 */
public interface Properties {

  final String PROPERTIES_NAME = "application.properties";

  final String CONTROLLER_HOST =
      Configurations.getInstance().getProperty( "controller.host" );

  final String CONTROLLER_PORT =
      Configurations.getInstance().getProperty( "controller.port" );

  final String CLIENT_OUTBOUND_DIRECTORY =
      Configurations.getInstance().getProperty( "client.outbound.directory" );

  final String SYSTEM_DESIGN_SCHEMA = Configurations.getInstance()
      .getProperty( "system.design.schema", Constants.SYSTEM_TYPE_REPLICATION );

  final String SYSTEM_LOG_LEVEL =
      Configurations.getInstance().getProperty( "system.log.level", "INFO" );
}
