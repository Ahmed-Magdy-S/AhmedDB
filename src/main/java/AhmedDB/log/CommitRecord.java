package AhmedDB.log;

import AhmedDB.file.Page;

public class CommitRecord extends LogRecord {
    private final int transactionNumber;
    public CommitRecord(Page page) {
        int transactionNumberPosition = Integer.BYTES; // passing the first value of record that belongs to LogOperator integer value.
        transactionNumber = page.getInt(transactionNumberPosition);
    }

    public static int writeToLog(LogManager logManager, int transactionNumber) {
        byte[] recordBytes = new byte[2*Integer.BYTES]; // 1 for LogOperator and 1 for transactionId
        Page page = new Page(recordBytes);
        page.setInt(0,LogOperator.COMMIT.value);
        page.setInt(Integer.BYTES,transactionNumber);
        return logManager.append(recordBytes);
    }

    @Override
    public int getRecordOperatorNumber() {
        return LogOperator.COMMIT.value;
    }

    @Override
    public int getTransactionNumber() {
        return transactionNumber;
    }

    @Override
    public String toString(){
        return "<COMMIT " + transactionNumber + ">";
    }

}
