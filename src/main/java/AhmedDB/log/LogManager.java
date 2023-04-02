package AhmedDB.log;

import AhmedDB.file.FileManager;
import AhmedDB.file.LogicalBlock;
import AhmedDB.file.Page;

import java.util.Iterator;

/**
 * The log manager, which is responsible for
 * writing log records into a log file. The tail of
 * the log is kept in a bytebuffer, which is flushed
 * to disk when needed.
 * The {@link LogManager} sees each log record as a byte array. Each
 * kind of log record has its own class, which is responsible for embedding the
 * appropriate values in the byte array. The first value in each array will be an integer
 * that denotes the operator of the record; the operator can be one of the constants in {@link LogOperator}
 * The remaining values depend on the operator — a quiescent checkpoint record has no
 * other values, an update record has five other values, and the other records have one
 * other value.
 * The database engine has one {@link LogManager} object, which is created during system
 * startup.
 */
public class LogManager {

    private final FileManager fileManager;
    private final String logFile;
    private final Page logPage;
    private LogicalBlock currentBlock;
    /**
     * log sequence number, it identifies the new log record.
     */
    private int latestLSN = 0;
    private int lastSavedLSN = 0;

    /**
     * Creates the manager for the specified log file.
     * If the log file does not yet exist, it is created
     * with an empty first block.
     * The database engine (AhmedDB) has one LogManager object, which is created during system startup.
     * The arguments to the constructor are a reference to the file manager and the name of the log file.
     * @param fileManager the file manager
     * @param logFile the name of the log file
     */
    public LogManager(FileManager fileManager, String logFile) {
        this.fileManager = fileManager;
        this.logFile = logFile;
        byte[] logRecordBytes = new byte[fileManager.blockSize()];
        logPage = new Page(logRecordBytes);
        int logSize = fileManager.length(logFile);
        if (logSize == 0)
            currentBlock = appendNewBlock();
        else {
            currentBlock = new LogicalBlock(logFile, logSize-1);
            fileManager.read(currentBlock, logPage);
        }
    }

    /**
     * Ensures that the log record corresponding to the
     * specified LSN has been written to disk.
     * All earlier log records will also be written to disk (because they are in the same page).
     * A client can force a specific log record to disk by calling the
     * method flush. The argument to flush is the LSN of a log record; the method
     * ensures that this log record (and all previous log records) is written to disk.
     * @param lsn the LSN of a log record
     */
    public void flush(int lsn) {
        if (lsn >= lastSavedLSN)
            flush();
    }

    /**
     * The iterator method flushes the log (in order to ensure that the entire log is on
     * disk) and then returns a LogIterator object.
     * A client calls the method iterator to read the records in the log, this method
     * returns a Java iterator for the log records. Each call to the iterator’s next method
     * will return a byte array denoting the next record in the log. The records returned by
     * the iterator method are in reverse order, starting at the most recent record and
     * moving backwards through the log file. The records are returned in this order
     * because that is how the recovery manager wants to see them.
     * @return a Java iterator for the log records
     */
    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fileManager, currentBlock);
    }

    /**
     * Appends a log record to the log buffer.
     * The record consists of an arbitrary array of bytes.
     * Log records are written right to left in the buffer.
     * The size of the record is written before the bytes.
     * The beginning of the buffer contains the location
     * of the last-written record (the "boundary").
     * Storing the records backwards makes it easy to read
     * them in reverse order.
     * Appending a record to the log does not guarantee that the record will get written
     * to disk; instead, the log manager chooses when to write log records to disk.
     * @param logRecord a byte buffer containing the bytes, The only constraint is that the array must fit inside a page
     * @return the log sequence number (LSN) of the final value, it identifies the new log record.
     */
    public synchronized int append(byte[] logRecord) {
        //The beginning of the buffer contains the location of the last-written record
        //In first creation, the first position of logPage will contain the number of block size.
        //Generally, it contains the offset of the most recently added record
        int boundary = logPage.getInt(0);
        // The record consists of an arbitrary array of bytes
        int recordSize = logRecord.length;
        //The size of the record is written before the bytes, so we need to consider integer bytes
        int numOfBytesNeeded = recordSize + Integer.BYTES;
        if (boundary - numOfBytesNeeded < Integer.BYTES) { // the log record doesn't fit,
            flush();        // so move to the next block.
            currentBlock = appendNewBlock();
            boundary = logPage.getInt(0);
        }
        int recordPosition = boundary - numOfBytesNeeded;

        logPage.setBytes(recordPosition, logRecord);
        logPage.setInt(0, recordPosition); // the new boundary
        latestLSN += 1;
        return latestLSN;
    }

    /**
     * Initialize the bytebuffer and append it to the log file.
     */
    private LogicalBlock appendNewBlock() {
        LogicalBlock blk = fileManager.append(logFile);
        logPage.setInt(0, fileManager.blockSize());
        fileManager.write(blk, logPage);
        return blk;
    }

    /**
     * Write the buffer to the log file.
     */
    private void flush() {
        fileManager.write(currentBlock, logPage);
        lastSavedLSN = latestLSN;
    }
}
