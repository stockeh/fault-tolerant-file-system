package cs555.system.exception;

/**
 * Exception thrown when the client detects the a read failure from a
 * chunk server due to invalid content.
 * 
 * @author stock
 *
 */
public class ClientWriteException extends Exception {

  /**
   * Generated serial version ID
   */
  private static final long serialVersionUID = 3501513898703364750L;

  /**
   * 
   * @param message
   */
  public ClientWriteException(String message) {
    super( message );
  }

  /**
   * 
   * @param message
   * @param cause
   */
  public ClientWriteException(String message, Throwable cause) {
    super( message, cause );
  }

  /**
   * 
   * @param cause
   */
  public ClientWriteException(Throwable cause) {
    super( cause );
  }
}
