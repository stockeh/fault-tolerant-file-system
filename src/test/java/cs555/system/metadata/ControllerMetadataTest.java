package cs555.system.metadata;

import static org.junit.Assert.fail;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import cs555.system.util.Constants;

public class ControllerMetadataTest {
  ControllerMetadata metadata;

  @Before
  public void initialize() {
    metadata = new ControllerMetadata();
  }

  @Test
  public void testGetChunkServers() {
    metadata.addConnection( "a", null );
    metadata.getConnections().get( "a" ).setNumberOfChunks( 3 );
    metadata.updateServerInformation( "a", 100 );

    metadata.addConnection( "b", null );
    metadata.getConnections().get( "b" ).setFreeDiskSpace( 2 );
    metadata.updateServerInformation( "b", 101 );

    metadata.addConnection( "c", null );
    metadata.getConnections().get( "c" ).setFreeDiskSpace( 2 );
    metadata.updateServerInformation( "c", 102 );

    boolean isOriginalFile = true;
    String[] servers = metadata.getChunkServers( isOriginalFile );
    if ( Constants.NUMBER_OF_REPLICATIONS == 3 )
    {
      Assert.assertArrayEquals( servers, new String[] { "c", "b", "a" } );
    } else if ( Constants.NUMBER_OF_REPLICATIONS == 1 )
    {
      System.out.println( "HERE" );
      Assert.assertArrayEquals( servers, new String[] { "c" } );
    } else
    {
      fail( "Invalid number of replications." );
    }
  }

}
