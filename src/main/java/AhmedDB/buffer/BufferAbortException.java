package AhmedDB.buffer;

/**
 * A runtime exception indicating that the transaction
 * needs to abort because a buffer request could not be satisfied.
 * Usually, that means that there is no buffer available to be pinned
 */
public class BufferAbortException extends RuntimeException {}
