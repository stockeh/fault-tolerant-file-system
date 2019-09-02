package cs555.system.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Utility class for all file related activities.
 * 
 * @author stock
 *
 */
public class FileUtilities {

  /**
   * Returns the size, in bytes, of the specified <tt>path</tt>. If the
   * given path is a regular file, trivially its size is returned. Else
   * the path is a directory and its contents are recursively explored,
   * returning the total sum of all files within the directory.
   * <p>
   * If an I/O exception occurs, it is suppressed within this method and
   * <tt>0</tt> is returned as the size of the specified <tt>path</tt>.
   * 
   * @param path path whose size is to be returned
   * @return size of the specified path
   */
  public static long calculateSize(Path path) {
    try
    {
      if ( Files.isRegularFile( path ) )
      {
        return Files.size( path );
      }

      return Files.list( path ).mapToLong( FileUtilities::calculateSize ).sum();
    } catch ( IOException e )
    {
      return 0L;
    }
  }

}
