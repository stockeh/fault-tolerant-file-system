package cs555.system.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import cs555.system.metadata.ServerMetadata.ChunkInformation;
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
  private final Map<String, FileInformation> files = new ConcurrentHashMap<>();

  /**
   * Connections to all the chunk servers. <k: host:port , v: chunk
   * server connection>
   * 
   * TODO: check if needs to be concurrent. Speed implications?
   */
  private final Map<String, ServerInformation> connections =
      new ConcurrentHashMap<>();

  /**
   * Connections to all the clients. Does not contain any identifier -
   * all clients are treated the same.
   */
  private final List<TCPConnection> clientConnections = new ArrayList<>();

  public static final Comparator<ServerInformation> COMPARATOR = Comparator
      .comparing( ServerInformation::getNumberOfChunks ).thenComparing(
          ServerInformation::getFreeDiskSpace, Collections.reverseOrder() );

  /**
   * 
   * @return the map of files containing file names and information
   */
  public Map<String, FileInformation> getFiles() {
    return files;
  }

  /**
   * 
   * @return the map of connections containing chunk server identifier
   *         and information
   */
  public Map<String, ServerInformation> getConnections() {
    return connections;
  }

  /**
   * 
   * @return a list of all the clients that have registered with the
   *         controller.
   */
  public List<TCPConnection> getClientConnections() {
    return clientConnections;
  }

  /**
   * Add a file to the metadata if it does not already exist. Otherwise
   * return from method signaling the file is not original.
   * 
   * @param filename of the file to maintain
   * @param filelength
   * @param numberOfChunks that make up the file
   * 
   * @return true if the file is original, false otherwise
   */
  public boolean addFile(String filename, int filelength, int numberOfChunks) {
    boolean isOriginalFile = !files.containsKey( filename );

    if ( isOriginalFile )
    {
      files.put( filename, new FileInformation( filelength, numberOfChunks ) );
    }

    return isOriginalFile;
  }

  /**
   * Add a new connection ( chunk server ) to the controllers metadata.
   * 
   * @param connectionDetails
   * @param connection with socket information to talk back with a
   *        server
   */
  public void addConnection(String connectionDetails,
      TCPConnection connection) {
    connections.put( connectionDetails,
        new ServerInformation( connection, connectionDetails ) );
  }

  /**
   * Add a new client to the controllers metadata.
   * 
   * @param connection with socket information to talk back with a
   *        client
   */
  public void addClientConnection(TCPConnection connection) {
    clientConnections.add( connection );
  }

  /**
   * Remove an existing connection ( chunk server ) from the controllers
   * metadata.
   * 
   * @param connectionDetails
   */
  public ServerInformation removeConnection(String connectionDetails) {
    return connections.remove( connectionDetails );
  }

  /**
   * Remove an existing client connection from the controllers metadata.
   * 
   * @param connection the actual <tt>TCPConnection</tt> referenced from
   *        the event.
   */
  public void removeClientConnection(TCPConnection connection) {
    clientConnections.remove( connection );
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
   */
  public void updateServerInformation(String connectionDetails,
      long freeDiskSpace) throws NullPointerException {
    ServerInformation server = connections.get( connectionDetails );
    if ( server == null )
    {
      throw new NullPointerException( "Chunk server connection: "
          + connectionDetails + ", does not exist on controller." );
    }
    server.setFreeDiskSpace( freeDiskSpace );
  }

  /**
   * Iterate the map and update the files with their respective chunk
   * server locations.
   * 
   * @param filesFromServer map of <k: file_name, v: List((sequence,
   *        position))>
   * @param connectionDetails the host:port associated with the chunk
   *        server
   */
  public void updateFileInformation(
      Map<String, List<ChunkInformation>> filesFromServer,
      String connectionDetails) throws NullPointerException {
    for ( Entry<String, List<ChunkInformation>> entry : filesFromServer
        .entrySet() )
    {
      FileInformation information = files.get( entry.getKey() );
      if ( information == null )
      {
        throw new NullPointerException( "Unable to update because the file: "
            + entry.getKey() + ", does not exist on controller." );
      }
      String[][] chunks = information.getChunks();

      for ( ChunkInformation chunk : entry.getValue() )
      {
        chunks[ chunk.getSequence() ][ chunk.getReplication() ] =
            connectionDetails;
      }
    }
  }

  /**
   * Computationally decide which chunk servers a given chunk should be
   * written too. A list of servers will be returned with the associated
   * connection identifiers.
   * 
   * The total number of chunks for a given connected chunk server is
   * incremented with the assumption that the chunk will be written.
   * 
   * TODO: Check if there are any chunk servers, if not respond with
   * error.
   * 
   * TODO: Check if the file already exists and there is a version
   * increase. Get existing locations if exists
   * 
   * @param filename is the name of the file that will be added to the
   *        server information for each chunk.
   * @param isOriginalFile computes the list of new chunk servers to
   *        write too, otherwise will retrieve the existing server
   *        locations
   * 
   * @return a list of chunk servers for the client to send data too
   */
  public String[] getChunkServers(String filename, boolean isOriginalFile) {

    List<ServerInformation> list = new ArrayList<>( connections.values() );

    // see comparator for sort details
    Collections.sort( list, ControllerMetadata.COMPARATOR );

    int numberOfConnections =
        connections.size() < Constants.NUMBER_OF_REPLICATIONS
            ? connections.size()
            : Constants.NUMBER_OF_REPLICATIONS;

    String[] output = new String[ numberOfConnections ];

    for ( int i = 0; i < numberOfConnections; ++i )
    {
      String connectionDetails = list.get( i ).getConnectionDetails();
      output[ i ] = connectionDetails;
      connections.get( connectionDetails ).addFileOnServer( filename );
      connections.get( connectionDetails ).incrementNumberOfChunks();
    }

    return output;
  }

  /**
   * Iterate over all the files and capture a list of readable items,
   * i.e., all chunks have been written for some files.
   * 
   * @return a list of readable files
   */
  public List<String> getReadableFiles() {
    List<String> readableFiles = new ArrayList<>();
    for ( Entry<String, FileInformation> entry : files.entrySet() )
    {
      boolean readable = true;

      String[][] chunks = entry.getValue().getChunks();
      for ( int i = 0; i < chunks.length; i++ )
      {
        if ( chunks[ i ][ 0 ] == null )
        {
          LOG.debug( "File \'" + entry.getKey() + "\' is not readable." );
          readable = false;
          break;
        }
      }
      if ( readable )
      {
        readableFiles.add( entry.getKey() );
      }
    }
    return readableFiles;
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
  public static class FileInformation {

    /**
     * chunk_1: chunk_server_a, chunk_server_b, ... chunk_2: ... ...
     */
    private String[][] chunks;

    private int filelenth;

    /**
     * 
     * @param filelength
     * @param numberOfChunks
     */
    private FileInformation(int filelength, int numberOfChunks) {
      this.chunks =
          new String[ numberOfChunks ][ Constants.NUMBER_OF_REPLICATIONS ];
      this.filelenth = filelength;
    }

    /**
     * 
     * @return the chunk server locations associated for each chunk within
     *         the file
     */
    public String[][] getChunks() {
      return chunks;
    }

    /**
     * 
     * @return the length of the file being returned
     */
    public int getFilelength() {
      return filelenth;
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
  public static class ServerInformation {

    private TCPConnection connection;

    private String connectionDetails;

    // Set to maintain uniqueness of filenames
    private Set<String> filesOnServer;

    private long freeDiskSpace;

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
      this.filesOnServer = new HashSet<>();
      this.freeDiskSpace = 0;
      this.numberOfChunks = 0;
    }

    /**
     * 
     * @return the TCPConnection associated with the server
     */
    public TCPConnection getConnection() {
      return connection;
    }

    /**
     * 
     * @return the host:port connection details of the server
     */
    public String getConnectionDetails() {
      return connectionDetails;
    }

    public Set<String> getFilesOnServer() {
      return filesOnServer;
    }

    /**
     * 
     * @return the free disk space on the server as last updated by the
     *         minor heartbeat
     */
    private long getFreeDiskSpace() {
      return freeDiskSpace;
    }

    /**
     * 
     * @return the number of chunks written to a server
     */
    private long getNumberOfChunks() {
      return numberOfChunks;
    }

    /**
     * Add a given filename to the server information.
     * 
     * @param filename
     * @return true if the server file set did not already contain the
     *         specified element
     */
    public boolean addFileOnServer(String filename) {
      return filesOnServer.add( filename );
    }

    /**
     * Update the number of chunks for a specified server.
     * 
     * @param numberOfChunks
     */
    public void setNumberOfChunks(int numberOfChunks) {
      this.numberOfChunks = numberOfChunks;
    }

    /**
     * Update the amount of free disk space for a specified server.
     * 
     * @param freeDiskSpace
     */
    public void setFreeDiskSpace(long freeDiskSpace) {
      this.freeDiskSpace = freeDiskSpace;
    }

    /**
     * Increment the number of chunks for a specific chunk server.
     * 
     */
    public void incrementNumberOfChunks() {
      ++numberOfChunks;
    }
  }
}
