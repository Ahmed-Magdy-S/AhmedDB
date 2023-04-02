package AhmedDB.buffer;


import AhmedDB.file.FileManager;
import AhmedDB.file.LogicalBlock;
import AhmedDB.file.Page;
import AhmedDB.log.LogManager;

/**
 * An individual buffer. A data buffer wraps a page
 * and stores information about its status,
 * such as the associated disk block,
 * the number of times the buffer has been pinned,
 * whether its contents have been modified,
 * and if so, the id and lsn of the modifying transaction.
 */
public class Buffer {
    private final FileManager fileManager;
    private final LogManager logManager;
    private final Page associatedPage;
    /**
     * A reference to the block assigned to the page of the buffer.
     * If no block is assigned, then thevalue is null.
     */
    private LogicalBlock associatedLogicalBlock = null;
    /**
     * The number of times the page is pinned.
     * The pin count is incremented on each pin and decremented on each unpin
     */
    private int pins = 0;
    /**
     * indicating if the page has been modified. A value of 1 indicates that
     * the page has not been changed; otherwise, the integer identifies the transaction
     * that made the change.
     */
    private int txnum = -1;
    /**
     * Log information. If the page has been modified, then the buffer holds the (log sequence number) LSN of
     * the most recent log record. LSN values are never negative. If a client calls
     * {@link Buffer#setModified(int txNum, int lsn)} method with a negative LSN,
     * t indicates that a log record was not generated for that update.
     */
    private int lsn = -1;

    public Buffer(FileManager fileManager, LogManager logManager) {
        this.fileManager = fileManager;
        this.logManager = logManager;
        associatedPage = new Page(fileManager.blockSize());
    }

    public Page getAssociatedPage() {
        return associatedPage;
    }

    /**
     * Returns a reference to the disk block
     * allocated to the buffer.
     * @return a reference to a disk block
     */
    public LogicalBlock getAssociatedLogicalBlock() {
        return associatedLogicalBlock;
    }

    /**
     * If the client modifies the page, then it is also responsible for
     * generating an appropriate log record and calling the buffer’s setModified method
     * @param txNum the modifying transaction
     * @param lsn log sequence number (the generated log record)
     */
    public void setModified(int txNum, int lsn) {
        this.txnum = txNum;
        if (lsn >= 0)
            this.lsn = lsn;
    }

    /**
     * Return true if the buffer is currently pinned
     * (that is, if it has a nonzero pin count).
     * @return true if the buffer is pinned
     */
    public boolean isPinned() {
        return pins > 0;
    }

    public int modifyingTx() {
        return txnum;
    }

    /**
     * Reads the contents of the specified block into
     * the contents of the buffer.
     * If the buffer was dirty, then its previous contents
     * are first written to disk.
     * @param b a reference to the data block
     */
    void assignToBlock(LogicalBlock b) {
        flush();
        associatedLogicalBlock = b;
        fileManager.read(associatedLogicalBlock, associatedPage);
        pins = 0;
    }

    /**
     * Write the buffer to its disk block if it is dirty.
     * Ensures that the buffer’s assigned disk block has the same values as its page.
     * If the page has not been modified, then the method need not do anything
     */
    void flush() {
        //If it has been modified, then the method first calls LogManager.flush method to
        //ensure that the corresponding log record is on disk; then it writes the page to disk.
        if (txnum >= 0) {
            logManager.flush(lsn);
            fileManager.write(associatedLogicalBlock, associatedPage);
            txnum = -1;
        }
    }

    /**
     * Increase the buffer's pin count.
     */
    void pin() {
        pins++;
    }

    /**
     * Decrease the buffer's pin count.
     */
    void unpin() {
        pins--;
    }
}