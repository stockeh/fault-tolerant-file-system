package cs555.system.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import cs555.system.metadata.ServerMetadata.ChunkInformation;
import cs555.system.transport.TCPConnection;
import cs555.system.util.Constants;
import cs555.system.util.Logger;
import cs555.system.util.Properties;

/**
 * Maintains information about the files and chunk servers connected
 * to the controller.
 * 
 * @author stock
 *
 */
public class ControllerMetadata {

  private static Logger LOG = Logger.getInstance();

  /**
   * Comparator for how the servers should sorted when choosing places
   * for a new chunk file.
   * 
   */
  public static final Comparator<ServerInformation> COMPARATOR = Comparator
      .comparing( ServerInformation::getNumberOfChunks ).thenComparing(
          ServerInformation::getFreeDiskSpace, Collections.reverseOrder() );

  /**
   * Files stored on the chunk servers <k: filename , v: file
   * information>
   */
  private final Map<String, FileInformation> files;

  /**
   * Connections to all the chunk servers. <k: host:port , v: chunk
   * server connection>
   * 
   */
  private final Map<String, ServerInformation> connections;

  /**
   * Connections to all the clients. Does not contain any identifier -
   * all clients are treated the same.
   */
  private final List<TCPConnection> clientConnections;

  /**
   * Default constructor -
   * 
   */
  public ControllerMetadata() {
    this.files = new ConcurrentHashMap<>();
    this.connections = new ConcurrentHashMap<>();
    this.clientConnections = new ArrayList<>();
  }

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
   * @param sequence
   * 
   * @return true if the file is original, false otherwise
   */
  public boolean addFile(String filename, int filelength, int numberOfChunks,
      int sequence) {
    FileInformation info = files.get( filename );
    if ( info == null )
    {
      files.put( filename, new FileInformation( filelength, numberOfChunks ) );
      return true;
    } else if ( info != null && sequence == 0 )
    {
      FileInformation oldFileInformation = files.remove( filename );
      String[][] oldChunkLocations = oldFileInformation.getChunks();

      files.put( filename, new FileInformation( filelength, numberOfChunks ) );
      String[][] newFileLocations = files.get( filename ).getChunks();

      for ( int i = 0; i < oldChunkLocations.length; i++ )
      {
        for ( int j = 0; j < oldChunkLocations[ i ].length; j++ )
        {
          newFileLocations[ i ][ j ] = oldChunkLocations[ i ][ j ];
        }
      }
      info.setIsOriginalFile( false );
    }
    return info.isOriginalFile();
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
      FileInformation fileInformation = files.get( entry.getKey() );
      if ( fileInformation == null )
      {
        throw new NullPointerException( "Unable to update because the file: "
            + entry.getKey() + ", does not exist on controller." );
      }
      String[][] chunks = fileInformation.getChunks();

      for ( ChunkInformation chunkInformation : entry.getValue() )
      {
        chunks[ chunkInformation.getSequence() ][ chunkInformation
            .getReplication() ] = connectionDetails;
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
   * @param filename is the name of the file that will be added to the
   *        server information for each chunk.
   * @param sequence
   * @param isOriginalFile computes the list of new chunk servers to
   *        write too, otherwise will retrieve the existing server
   *        locations
   * 
   * @return a list of chunk servers for the client to send data too
   */
  public synchronized String[] getChunkServers(String filename, int sequence,
      boolean isOriginalFile) {

    FileInformation information = files.get( filename );
    if ( information != null )
    {
      String[] chunkLocations = information.getChunks()[ sequence ];

      boolean nonNullLocation =
          Arrays.stream( chunkLocations ).allMatch( Objects::nonNull );

      if ( !isOriginalFile && nonNullLocation )
      {
        LOG.debug( "Forwarding existing chunk information." );
        return chunkLocations;
      } else if ( !isOriginalFile && sequence == 0 )
      { // the case where the file is not original, but the chunk locations
        // have null entries since no heartbeats were received yet.
        return null;
      }
    }
    List<ServerInformation> list = new ArrayList<>( connections.values() );
    if ( list.size() == 0 )
    {
      return null;
    }
    // see comparator for sort details
    Collections.sort( list, ControllerMetadata.COMPARATOR );

    int numberOfConnections = list.size();

    int numberOfReplications = Constants.NUMBER_OF_REPLICATIONS;

    if ( Properties.SYSTEM_DESIGN_SCHEMA
        .equals( Constants.SYSTEM_TYPE_ERASURE ) )
    {
      numberOfReplications = Constants.ERASURE_TOTAL_SHARDS;
    } else if ( Constants.NUMBER_OF_REPLICATIONS > numberOfConnections )
    {
      numberOfReplications = numberOfConnections;
    }

    String[] output = new String[ numberOfReplications ];

    for ( int replication =
        0; replication < numberOfReplications; ++replication )
    {
      String connectionDetails =
          list.get( replication % numberOfConnections ).getConnectionDetails();
      output[ replication ] = connectionDetails;
      ServerInformation connection = connections.get( connectionDetails );

      connection.addFileOnServer( filename, sequence, replication );
      connection.incrementNumberOfChunks();
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
      for ( int i = 0; i < chunks.length; ++i )
      {
        if ( !Arrays.stream( chunks[ i ] ).anyMatch( Objects::nonNull ) )
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

    private boolean isOriginalFile;

    /**
     * Default constructor -
     * 
     * @param filelength
     * @param numberOfChunks
     */
    private FileInformation(int filelength, int numberOfChunks) {
      this.chunks =
          new String[ numberOfChunks ][ Constants.NUMBER_OF_REPLICATIONS ];
      this.filelenth = filelength;
      this.isOriginalFile = true;
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

    public boolean isOriginalFile() {
      return isOriginalFile;
    }

    public void setIsOriginalFile(boolean isOriginalFile) {
      this.isOriginalFile = isOriginalFile;
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

    /**
     * Map < k: filename, v: list(sequence) >
     */
    private Map<String, List<SequenceReplicationPair>> filesOnServer;

    private long freeDiskSpace;

    private AtomicInteger numberOfChunks;

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
      this.filesOnServer = new HashMap<>();
      this.freeDiskSpace = 0;
      this.numberOfChunks = new AtomicInteger( 0 );
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

    public Map<String, List<SequenceReplicationPair>> getFilesOnServer() {
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
      return numberOfChunks.get();
    }

    /**
     * Add a given filename to the server information.
     * 
     * @param filename
     * @param sequence
     * @param replication
     */
    public void addFileOnServer(String filename, int sequence,
        int replication) {
      filesOnServer.putIfAbsent( filename,
          new ArrayList<SequenceReplicationPair>() );
      filesOnServer.get( filename )
          .add( new SequenceReplicationPair( sequence, replication ) );
    }

    /**
     * Update the number of chunks for a specified server.
     * 
     * @param numberOfChunks
     */
    public void setNumberOfChunks(int numberOfChunks) {
      this.numberOfChunks.set( numberOfChunks );
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
      numberOfChunks.incrementAndGet();
    }

    /**
     * Naive object containing the pair of sequence and replication
     * numbers for some file.
     * 
     * @author stock
     *
     */
    public static class SequenceReplicationPair {

      private int sequence;

      private int replication;

      /**
       * Default constructor -
       * 
       * @param sequence
       * @param replication
       */
      private SequenceReplicationPair(int sequence, int replication) {
        this.sequence = sequence;
        this.replication = replication;
      }

      /**
       * 
       * @return the sequence number
       */
      public int getSequence() {
        return sequence;
      }

      /**
       * 
       * @return the replication position
       */
      public int getReplication() {
        return replication;
      }

    }

  }
}
