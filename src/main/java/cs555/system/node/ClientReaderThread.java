package cs555.system.node;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import cs555.system.exception.ClientReadException;
import cs555.system.metadata.ClientMetadata;
import cs555.system.transport.TCPConnection;
import cs555.system.util.ConnectionUtilities;
import cs555.system.util.Constants;
import cs555.system.util.Logger;
import cs555.system.util.ProgressBar;
import cs555.system.util.Properties;
import cs555.system.util.ReedSolomonUtilities;
import cs555.system.wireformats.ReadChunkRequest;
import cs555.system.wireformats.ReadChunkResponse;
import cs555.system.wireformats.ReadFileResponse;
import erasure.ReedSolomon;

/**
 * Client reader responsible for sending requests to servers to obtain
 * chunks for a specified file.
 * 
 * @author stock
 *
 */
public class ClientReaderThread implements Runnable {

  private static final Logger LOG = Logger.getInstance();

  private final Object lock;

  private ReadChunkResponse readChunkResponse;

  private ReadFileResponse readFileResponse;

  private final ClientMetadata metadata;

  private final Client node;


  /**
   * Default constructor -
   * 
   * @param node
   * @param metadata
   * @param readFileResponse
   */
  protected ClientReaderThread(Client node, ClientMetadata metadata,
      ReadFileResponse readFileResponse) {
    this.lock = new Object();
    this.node = node;
    this.metadata = metadata;
    this.readFileResponse = readFileResponse;
  }

  /**
   * Set the ReadChunkResponse() from the chunk server
   * 
   * @param response
   */
  public void setReadChunkResponse(ReadChunkResponse response) {
    this.readChunkResponse = response;
  }

  /**
   * Wake the sender thread upon receiving routing information for a
   * given chunk.
   * 
   */
  protected void unlock() {
    synchronized ( lock )
    {
      lock.notify();
    }
  }

  /**
   * Continuously send requests to the chunk servers for chunk data.
   * Writing to disk if all chunks are successfully returned.
   * 
   */
  @Override
  public void run() {
    SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );
    LOG.info(
        "Started reading file at " + sdf.format( System.currentTimeMillis() ) );
    String[][] chunkServers = readFileResponse.getChunks();
    ConnectionUtilities connections = new ConnectionUtilities();

    byte[][] bytes = processIncomingChunks( chunkServers, connections );
    if ( bytes != null )
    {
      int numberOfChunks = bytes.length;
      ByteBuffer fileBytes =
          ByteBuffer.allocate( numberOfChunks * bytes[ 0 ].length );
      for ( int i = 0; i < numberOfChunks; ++i )
      {
        fileBytes.put( bytes[ i ] );
      }
      // truncate file to original file length before writing
      byte[] file = Arrays.copyOfRange( fileBytes.array(), 0,
          readFileResponse.getFilelength() );
      try
      {
        writeFileToDisk( readFileResponse.getFilename(), file );
      } catch ( IOException e )
      {
        LOG.error( "Unable to save file \'" + readFileResponse.getFilename()
            + "\' to disk." );
        e.printStackTrace();
      }
    } else
    {
      LOG.error( "Unable to read file due to missing / invalid chunks." );
      metadata.removeReadableFile( readFileResponse.getFilename() );
    }
    LOG.info( "Finished reading file at "
        + sdf.format( System.currentTimeMillis() ) + "\n" );
    connections.setAbleToClear( true );
    connections.closeCachedConnections();
  }

  /**
   * Iterate over the connection details from the controller, requesting
   * chunk data from each chunk server.
   * 
   * @param chunkServers containing the chunk server connection details
   *        for each chunk in the file
   * @param connections
   * @return a 2-dimensional array of chunk bytes for the given server
   */
  private byte[][] processIncomingChunks(String[][] chunkServers,
      ConnectionUtilities connections) {
    byte[][] fileBytes =
        new byte[ chunkServers.length ][ Constants.CHUNK_SIZE ];
    String filename = readFileResponse.getFilename();
    filename = filename.substring( filename.lastIndexOf( File.separator ) );
    ProgressBar progress = new ProgressBar( filename );
    int replication = 0;
    for ( int sequence = 0; sequence < chunkServers.length; ++sequence )
    {
      progress.update( sequence, chunkServers.length );
      try
      {
        if ( Properties.SYSTEM_DESIGN_SCHEMA
            .equals( Constants.SYSTEM_TYPE_ERASURE ) )
        {
          fileBytes[ sequence ] =
              decodeErasureCodes( chunkServers, sequence, connections );
          if ( fileBytes[ sequence ] == null )
          {
            progress.reset();
            LOG.error(
                "The chunk could not be reconstructed since there are not"
                    + " enough data shards." );
            return null;
          }
        } else
        {
          sendReadChunkRequest( chunkServers, sequence, replication,
              connections );
          if ( readChunkResponse.getStatus() == Constants.FAILURE )
          {
            throw new ClientReadException( "The chunk sequence \'" + sequence
                + "\' was returned as invalid." );
          }
          fileBytes[ sequence ] = readChunkResponse.getMessage();
        }
      } catch ( IOException | InterruptedException | ClientReadException e )
      {
        LOG.debug( "Unable to retrieve message on chunk server \'"
            + chunkServers[ sequence ][ replication ]
            + "\' trying next replication if possible. " + e.getMessage() );

        if ( ++replication >= chunkServers[ 0 ].length )
        {
          progress.reset();
          LOG.error(
              "File is not readable beacause a given chunk can not be returned"
                  + " by any servers." );
          return null;
        }
        // Attempt to use next replication location for the same chunk.
        --sequence;
        continue;
      }
      replication = 0;
    }
    return fileBytes;
  }

  /**
   * Convert all the shards into the original chunk content
   * 
   * @param chunkServers
   * @param sequence
   * @param connections
   * @return the bytes of the original chunk
   */
  private byte[] decodeErasureCodes(String[][] chunkServers, int sequence,
      ConnectionUtilities connections) {
    byte[][] shards =
        new byte[ Constants.ERASURE_TOTAL_SHARDS ][ Constants.ERASURE_SHARD_SIZE ];
    boolean[] shardPresent = new boolean[ Constants.ERASURE_TOTAL_SHARDS ];

    int shardCount = 0;

    for ( int shard = 0; shard < Constants.ERASURE_TOTAL_SHARDS; shard++ )
    {
      // send request to chunk server if successful
      try
      {
        sendReadChunkRequest( chunkServers, sequence, shard, connections );
        shards[ shard ] = readChunkResponse.getMessage();
        shardPresent[ shard ] = true;
        if ( ++shardCount >= Constants.ERASURE_DATA_SHARDS )
        {
          break;
        }
      } catch ( ClientReadException | IOException | InterruptedException e )
      {
        LOG.debug( "Unable to retrieve message on chunk server \'"
            + chunkServers[ sequence ][ shard ]
            + "\' trying next shard if possible. " + e.getMessage() );
      }
    }
    if ( shardCount < Constants.ERASURE_DATA_SHARDS )
    {
      return null;
    }
    ReedSolomon codec = new ReedSolomon( Constants.ERASURE_DATA_SHARDS,
        Constants.ERASURE_PARITY_SHARDS );
    codec.decodeMissing( shards, shardPresent, 0,
        Constants.ERASURE_SHARD_SIZE );

    return ReedSolomonUtilities.shardsToArray( shards );
  }

  /**
   * Send a request to the chunk server for a given sequence
   * 
   * @param chunkServers to establish new connections
   * @param sequence to index into the chunkServers
   * @param replication to index into the chunkServers
   * @param connections that maintain cached connections
   * @throws IOException
   * @throws ClientReadException
   * @throws InterruptedException
   */
  private void sendReadChunkRequest(String[][] chunkServers, int sequence,
      int replication, ConnectionUtilities connections)
      throws IOException, ClientReadException, InterruptedException {
    if ( chunkServers[ sequence ][ replication ] == null )
    {
      throw new ClientReadException(
          "The server for the ( sequence, replication ) pair is null." );
    }
    String[] initialConnection =
        chunkServers[ sequence ][ replication ].split( ":" );

    TCPConnection connection =
        connections.cacheConnection( node, initialConnection, true );

    byte[] request =
        new ReadChunkRequest( readFileResponse.getFilename(), sequence )
            .getBytes();
    connection.getTCPSender().sendData( request );
    synchronized ( lock )
    {
      lock.wait();
    }
    LOG.debug( "sequence: " + sequence + ", status: "
        + readChunkResponse.getStatus() );
  }

  /**
   * Save the file requested for read to disk.
   * 
   * @param filename to write back to disk
   * @param file contain the content of the file
   * @throws IOException
   */
  private void writeFileToDisk(String filename, byte[] file)
      throws IOException {
    Timestamp timestamp = new Timestamp( System.currentTimeMillis() );
    String updatedFilename = filename + "_" + timestamp.toInstant();
    Path path = Paths.get( updatedFilename );
    Files.createDirectories( path.getParent() );
    Files.write( path, file );
  }
}
