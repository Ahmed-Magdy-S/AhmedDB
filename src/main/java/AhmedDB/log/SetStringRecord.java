package AhmedDB.log;

import AhmedDB.file.LogicalBlock;
import AhmedDB.file.Page;

public class SetStringRecord extends LogRecord {

    private int transactionNumber;
    private int offset;
    private String stringValue;
    private LogicalBlock logicalBlock;

    /**
     * It determines the offset of each value within the page and extracts them,
     * so that we can use that data to create a log record of type SETSTRING.<br>
     * <strong>Record Example:</strong><br>
     * <code>[operator, txId, filename, blockNumber, blockOffset, newStringValue]</code><br>
     * @param page the memory page that contains the record data
     */
    public SetStringRecord(Page page){
        int transactionPosition = Integer.BYTES; // passing the log operator bytes length
        transactionNumber = page.getInt(transactionPosition);
        int fileNamePosition = transactionPosition + Integer.BYTES; // the Integer.BYTES refers to the transactionPosition bytes, so we pass them
        String fileName = page.getString(fileNamePosition);
        int blockNumberPosition = fileNamePosition + Page.maxLength(fileName.length());
        int blockNumber = page.getInt(blockNumberPosition);
        logicalBlock = new LogicalBlock(fileName,blockNumber);
        int offsetPosition = blockNumberPosition + Integer.BYTES;
        offset = page.getInt(offsetPosition);
        int valuePosition = offsetPosition + Integer.BYTES;
        stringValue = page.getString(valuePosition);


    }

    /**
     * A static method to write a setString record to the log.
     * This log record contains the {@code LogOperator.SETSTRING} operator,
     * followed by the transaction id, the filename, number,
     * and offset of the modified block, and the previous
     * integer value at that offset.<br>
     * It will be printed as the following:<br>
     * < SETSTRING, transactionId, fileName, blockNumber, blockOffset, newStringValue ><br>
     * < SETSTRING, 2, testFile, 1, 40, hello > <br>
     * It first calculates the size of the byte array and
     * the offset within that array of each value of the record to get the accurate position for each value.
     * It then creates a byte array of that size, wraps it in a Page object,
     * and uses the page’s setInt and setString methods
     * to write the values in the appropriate locations
     * @param logManager it will be used for writing (appending) SETSTRING log record to the log file.
     * @param transactionNumber the transaction id number
     * @param logicalBlock the block that will be modified by writing string value into it.
     * @param offset a specific offset of a block that the value will be written into.
     * @param val the new string value that will be stored
     * @return the LSN (log sequence number) of the last log value
     */
    public static int writeToLog(LogManager logManager, int transactionNumber, LogicalBlock logicalBlock, int offset, String val){
        //To write a position for each value according to the following sequence:
        //<operator, txId, filename, blockNumber, blockOffset, newStringValue>
        int transactionPosition = Integer.BYTES; // LogOperator takes 1 integer, so we pass (exceed) its value bytes to put transaction num after it.
        int fileNamePosition = transactionPosition + Integer.BYTES; //passing both tx and lopOp
        int blockNumberPosition = fileNamePosition + Page.maxLength(logicalBlock.getFileName().length()); ////passing tx, lopOp, and filename length
        int blockOffsetPosition = blockNumberPosition + Integer.BYTES; // passing the above 4 values
        int valuePosition = blockOffsetPosition + Integer.BYTES; // passing the above 5 values
        int recordLength = valuePosition + Page.maxLength(val.length()); // passing the above 6 values

        byte[] recordBytes = new byte[recordLength];

        Page page = new Page(recordBytes);

        //Create a record with data filled into it, according to the sequence mentioned above
        page.setInt(0,LogOperator.SETSTRING.value);
        page.setInt(transactionPosition,transactionNumber);
        page.setString(fileNamePosition,logicalBlock.getFileName());
        page.setInt(blockNumberPosition,logicalBlock.getNumber());
        page.setInt(blockOffsetPosition,offset);
        page.setString(blockOffsetPosition,val);

        return logManager.append(recordBytes);
    }



    @Override
    public int getRecordOperatorNumber() {
        return LogOperator.SETSTRING.value;
    }

    @Override
    public int getTransactionNumber() {
        return this.transactionNumber;
    }

    @Override
    public void undo(int transactionNumber) {
    }
}
