package cs555.system.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import cs555.system.transport.TCPConnection;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.WriteQuery;

/**
 * 
 * @author stock
 *
 */
public class ClientSenderThread implements Runnable {

  private static final Logger LOG = new Logger();

  private List<File> files;

  private final Object lock = new Object();

  private TCPConnection controllerConnection;

  private String[] routes;

  private boolean running = false;

  /**
   * Default constructor
   * 
   * @param controllerConnection
   */
  public ClientSenderThread(TCPConnection controllerConnection) {
    this.controllerConnection = controllerConnection;
  }

  /**
   * Wake the sender thread upon receiving routing information for a
   * given chunk.
   * 
   */
  public void unlock() {
    synchronized ( this.lock )
    {
      this.lock.notify();
    }
  }

  /**
   * Check if the thread is in a running state.
   * 
   * @return true if running, false otherwise
   */
  public boolean isRunning() {
    return this.running;
  }

  /**
   * Set the files to process from the outbound directory.
   * 
   * @param files
   */
  public void setFiles(List<File> files) {
    this.files = files;
  }

  /**
   * Calling method checked for validity of routes;
   * 
   * @param routes
   */
  public void setRoutes(String[] routes) {
    this.routes = routes;
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
    running = true;
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
            lock.wait();
            LOG.debug( Arrays.toString( this.routes ) );
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
        + " file(s) to the controller.\n" );
    running = false;
    return;
  }

}
