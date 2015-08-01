package org.cojen.tupl;

/**
 * Created by tylaar on 15/8/2.
 */
public class ReadOnlyModeException extends DatabaseException {

    private static final long serialVersionUID = 1L;
    private String message;

    public ReadOnlyModeException(String message) {
        super();
        this.message = message;
    }

    @Override
    boolean isRecoverable() {
        return true;
    }
}
