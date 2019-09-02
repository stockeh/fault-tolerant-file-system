package cs555.system.metadata;

import java.util.HashMap;
import java.util.Map;
import cs555.system.transport.TCPConnection;
import cs555.system.util.Logger;

/**
 * Maintains information about the files and chunk servers connected
 * to the controller.
 * 
 * @author stock
 *
 */
public class ControllerMetadata {

  private static Logger LOG = new Logger();

  /**
   * Files stored on the chunk servers <k: filename , v: file
   * information>
   */
  private final Map<String, FileMetadata> files = new HashMap<>();

  /**
   * Connections to all the chunk servers. <k: host:port , v: chunk
   * server connection>
   */
  private final Map<String, ChunkServerMetadata> connections = new HashMap<>();

  /**
   * Add a new connection ( chunk server ) to the controllers metadata.
   * 
   * @param connectionDetails
   * @param connection
   */
  public void addConnection(String connectionDetails,
      TCPConnection connection) {
    connections.put( connectionDetails, new ChunkServerMetadata( connection ) );
  }

  /**
   * Remove an existing connection ( chunk server ) from the controllers
   * metadata.
   * 
   * @param connectionDetails
   */
  public void removeConnection(String connectionDetails) {
    connections.remove( connectionDetails );
  }

  /**
   * Provide the number of connected chunk servers there are with the
   * controller.
   * 
   * @return the number of chunk servers
   */
  public int numberOfConnections() {
    return connections.size();
  }

  /**
   * Check if the connections contain a specific chunk server as
   * identified by the connection details.
   * 
   * @param connectionDetails
   * @return true if the connection exists, false otherwise.
   */
  public boolean connectionsContainsKey(String connectionDetails) {
    return connections.containsKey( connectionDetails );
  }

  /**
   * Print out all of the connected chunk servers that have connected.
   * 
   */
  public void displayConnections() {
    if ( connections.size() == 0 )
    {
      LOG.error(
          "There are no connections in the controller. Initialize a new chunk server." );
    } else
    {
      System.out
          .println( "\nThere are " + connections.size() + " total links:\n" );
      connections.forEach( (k, v) -> System.out.println( "\t" + k ) );
      System.out.println();
    }
  }

  /**
   * Maintains information about the chunks and the chunk servers for a
   * given file.
   * 
   * Note: the static nested class does not have access to the members
   * of the enclosing class.
   * 
   * @author stock
   *
   */
  private static class FileMetadata {

    /**
     * How many servers replicate the chunks
     */
    private final int NUMBER_OF_REPLICATIONS = 3;

    /**
     * chunk_1: chunk_server_a, chunk_server_b, ... chunk_2: ... ...
     * 
     * TODO: Need to know how many chunks make up a file.
     */
    private String[][] chunks;

    private FileMetadata(int numberOfChunks) {
      chunks = new String[ numberOfChunks ][ NUMBER_OF_REPLICATIONS ];
    }
  }

  /**
   * Maintains information about the connected chunk servers.
   * 
   * Note: the static nested class does not have access to the members
   * of the enclosing class.
   * 
   * @author stock
   *
   */
  private static class ChunkServerMetadata {

    private TCPConnection connection;

    private long freeDiskSpace;

    private long numberOfChunks;

    private ChunkServerMetadata(TCPConnection connection) {
      this.connection = connection;
      this.freeDiskSpace = 0L;
      this.numberOfChunks = 0L;
    }

  }

}
