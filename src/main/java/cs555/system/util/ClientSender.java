package cs555.system.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import cs555.system.transport.TCPConnection;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.WriteQuery;

public class ClientSender implements Runnable {

  private static final Logger LOG = new Logger();

  private List<File> files;

  private Object lock;

  private TCPConnection controllerConnection;

  public ClientSender(List<File> files, Object lock,
      TCPConnection controllerConnection) {
    this.files = files;
    this.lock = lock;
    this.controllerConnection = controllerConnection;
  }

  /**
   * Iterate through the files and send them to the chunk servers a
   * chunk at a time. A request to the controller will provide details
   * of which servers to communicate with.
   * 
   * @param files to send to the controller
   */
  @Override
  public void run() {
    byte[] b = new byte[ Protocol.CHUNK_SIZE ];
    final int numberOfFiles = files.size();
    synchronized ( lock )
    {
      for ( File file : files )
      {
        try ( InputStream is = new FileInputStream( file ) )
        {
          int numberOfChunks = ( int ) Math.ceil( file.length() / 1000.0 );
          byte[] request =
              ( new WriteQuery( file.getAbsolutePath(), numberOfChunks ) )
                  .getBytes();
          @SuppressWarnings( "unused" )
          int readBytes = 0;
          while ( ( readBytes = is.read( b ) ) != -1 )
          {
            this.controllerConnection.getTCPSender().sendData( request );
            LOG.debug( "START WAITING" );
            lock.wait();
            LOG.debug( "FINISHED WAITING" );
          }
        } catch ( IOException e )
        {
          LOG.error( "Unable to process the file " + file.getName() + ", "
              + e.getMessage() );
          e.printStackTrace();
        } catch ( InterruptedException e )
        {
          Thread.currentThread().interrupt();
        }
      }
    }
    LOG.info( "Finished sending " + Integer.toString( numberOfFiles )
        + "file(s) to the controller.\n" );
    return;
  }

}
