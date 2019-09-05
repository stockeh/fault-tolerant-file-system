package cs555.system.metadata;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ControllerMetadataTest {
  ControllerMetadata metadata;

  @Before
  public void initialize() {
    metadata = new ControllerMetadata();
  }

  @Test
  public void testGetChunkServers() {
    metadata.addConnection( "a", null );
    metadata.updateServerInformation( "a", 100, 3 );

    metadata.addConnection( "b", null );
    metadata.updateServerInformation( "b", 101, 2 );

    metadata.addConnection( "c", null );
    metadata.updateServerInformation( "c", 102, 2 );

    boolean isOriginalFile = true;
    String[] servers = metadata.getChunkServers( isOriginalFile );

    Assert.assertArrayEquals( servers, new String[] { "c", "b", "a" } );
  }

}
