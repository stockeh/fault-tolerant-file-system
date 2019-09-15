package cs555.system.metadata;

import java.util.ArrayList;
import java.util.List;

/**
 * Maintains information about what files can be accessed on the
 * server.
 * 
 * @author stock
 *
 */
public class ClientMetadata {

  /**
   * List of filenames
   */
  private List<String> readableFiles;

  /**
   * Default constructor -
   * 
   */
  public ClientMetadata() {
    this.setReadableFiles( new ArrayList<>() );
  }

  /**
   * 
   * @return the readable files on the controller since last list
   *         request
   */
  public List<String> getReadableFiles() {
    return readableFiles;
  }

  /**
   * Update the readable files known to the client from the controller
   * 
   * @param readableFiles
   */
  public void setReadableFiles(List<String> readableFiles) {
    this.readableFiles = readableFiles;
  }

  /**
   * Remove a readable file, forcing the client to ask the controller to
   * list all files again.
   * 
   * @param filename
   * @return
   */
  public boolean removeReadableFile(String filename) {
    return readableFiles.remove( filename );
  }

  /**
   * Clear all the readable files stored since the last fetch to the
   * controller.
   * 
   */
  public void clearReadableFiles() {
    readableFiles.clear();
  }

}
