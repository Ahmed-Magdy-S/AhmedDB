package AhmedDB.log;

import AhmedDB.file.FileManager;
import AhmedDB.file.LogicalBlock;
import AhmedDB.file.Page;

import java.util.Iterator;

/**
 * A class that provides the ability to move through the
 * records of the log file in reverse order.
 */
class LogIterator implements Iterator<byte[]> {
    private final FileManager fileManager;
    private LogicalBlock logicalBlock;
    private final Page page;
    private int currentPos;
    /**
     * It contains the offset of the most recently added record
     * This strategy enables the log iterator to read records in reverse order by reading from left
     * to right. The boundary value is written to the first four bytes of the page so that the
     * iterator will know where the records begin
     */
    private int boundary;

    /**
     * Creates an iterator for the records in the log file,
     * positioned after the last log record.
     * A LogIterator object allocates a page to hold the contents of a log block. The
     * constructor positions the iterator at the first record in the last block of the log (which
     * is, remember, where the last log record was written)
     */
    public LogIterator(FileManager fileManager, LogicalBlock logicalBlock) {
        this.fileManager = fileManager;
        this.logicalBlock = logicalBlock;
        byte[] b = new byte[fileManager.blockSize()];
        page = new Page(b);
        moveToBlock(logicalBlock);
    }

    /**
     * Determines if the current log record
     * is the earliest record in the log file.
     * @return true if there is an earlier record
     */
    @Override
    public boolean hasNext() {
        return currentPos < fileManager.blockSize() || logicalBlock.getNumber() > 0;
    }

    /**
     * Moves to the next log record in the block.
     * If there are no more log records in the block,
     * then move to the previous block
     * and return the log record from there.
     * @return the next earliest log record
     */
    @Override
    public byte[] next() {
        if (currentPos == fileManager.blockSize()) {
            logicalBlock = new LogicalBlock(logicalBlock.getFileName(), logicalBlock.getNumber()-1);
            moveToBlock(logicalBlock);
        }
        byte[] rec = page.getBytes(currentPos);
        currentPos += Integer.BYTES + rec.length;
        return rec;
    }

    /**
     * Moves to the specified log block
     * and positions it at the first record in that block
     * (i.e., the most recent one).
     */
    private void moveToBlock(LogicalBlock logicalBlock) {
        fileManager.read(logicalBlock, page);
        boundary = page.getInt(0);
        currentPos = boundary;
    }
}
