package cs555.system.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import cs555.system.transport.TCPConnection;
import cs555.system.util.Constants;
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
  private final Map<String, FileInformation> files = new HashMap<>();

  /**
   * Connections to all the chunk servers. <k: host:port , v: chunk
   * server connection>
   * 
   * TODO: check if needs to be concurrent. Speed implications?
   */
  private final Map<String, ServerInformation> connections =
      new ConcurrentHashMap<>();

  private final Comparator<ServerInformation> comparator = Comparator
      .comparing( ServerInformation::getNumberOfChunks ).thenComparing(
          ServerInformation::getFreeDiskSpace, Collections.reverseOrder() );

  /**
   * Add a file to the metadata if it does not already exist. Otherwise
   * return from method signaling the file is not original.
   * 
   * @param name of the file to maintain
   * @param numberOfChunks that make up the file
   * 
   * @return true if the file is original, false otherwise
   */
  public boolean addFile(String name, int numberOfChunks) {
    boolean isOriginalFile = !files.containsKey( name );

    if ( isOriginalFile )
    {
      files.put( name, new FileInformation( numberOfChunks ) );
    }

    return isOriginalFile;
  }

  /**
   * Add a new connection ( chunk server ) to the controllers metadata.
   * 
   * @param connectionDetails
   * @param connection
   */
  public void addConnection(String connectionDetails,
      TCPConnection connection) {
    connections.put( connectionDetails,
        new ServerInformation( connection, connectionDetails ) );
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
   * Update metadata from heartbeat messages.
   * 
   * @param connectionDetails
   * @param freeDiskSpace
   * @param numberOfChunks
   */
  public void update(String connectionDetails, int freeDiskSpace,
      int numberOfChunks) {
    ServerInformation server = connections.get( connectionDetails );
    server.setFreeDiskSpace( freeDiskSpace );
    server.setNumberOfChunks( numberOfChunks );
  }

  /**
   * Computationally decide which chunk servers a given chunk should be
   * written too. A list of servers will be returned with the associated
   * connection identifiers.
   * 
   * TODO: Check if there are any chunk servers, if not respond with
   * error.
   * 
   * TODO: Check if the file already exists and there is a version
   * increase. Get existing locations if exists
   * 
   * @param isOriginalFile computes the list of new chunk servers to
   *        write too, otherwise will retrieve the existing server
   *        locations
   * 
   * @return a list of chunk servers for the client to send data too
   */
  public String[] getChunkServers(boolean isOriginalFile) {

    List<ServerInformation> list = new ArrayList<>( connections.values() );

    // see comparator for sort details
    Collections.sort( list, comparator );

    int numberOfConnections =
        connections.size() < Constants.NUMBER_OF_REPLICATIONS
            ? connections.size()
            : Constants.NUMBER_OF_REPLICATIONS;

    String[] output = new String[ numberOfConnections ];

    for ( int i = 0; i < numberOfConnections; ++i )
    {
      output[ i ] = list.get( i ).getConnectionDetails();
    }

    return output;
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
  private static class FileInformation {

    /**
     * chunk_1: chunk_server_a, chunk_server_b, ... chunk_2: ... ...
     */
    private String[][] chunks;

    private FileInformation(int numberOfChunks) {
      chunks = new String[ numberOfChunks ][ Constants.NUMBER_OF_REPLICATIONS ];
    }

    /**
     * 
     * @return the chunk server locations associated for each chunk within
     *         the file
     */
    private String[][] getChunks() {
      return chunks;
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
  private static class ServerInformation {

    private TCPConnection connection;

    private String connectionDetails;

    private int freeDiskSpace;

    private int numberOfChunks;

    /**
     * Default constructor
     * 
     * @param connection
     * @param connectionDetails
     */
    private ServerInformation(TCPConnection connection,
        String connectionDetails) {
      this.connection = connection;
      this.connectionDetails = connectionDetails;
      this.freeDiskSpace = 0;
      this.numberOfChunks = 0;
    }

    private TCPConnection getConnection() {
      return this.connection;
    }

    private String getConnectionDetails() {
      return this.connectionDetails;
    }

    private long getFreeDiskSpace() {
      return this.freeDiskSpace;
    }

    private long getNumberOfChunks() {
      return this.numberOfChunks;
    }

    private void setNumberOfChunks(int numberOfChunks) {
      this.numberOfChunks = numberOfChunks;
    }

    private void setFreeDiskSpace(int freeDiskSpace) {
      this.freeDiskSpace = freeDiskSpace;
    }
  }
}
