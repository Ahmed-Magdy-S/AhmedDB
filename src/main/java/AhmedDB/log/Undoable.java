package AhmedDB.log;

/**
 * 
 */
public interface Undoable {
    void undo(int transactionNumber);
}
