package AhmedDB.log;

import AhmedDB.file.Page;

public class RollBackRecord extends LogRecord {

    private final int transactionNumber;

    public RollBackRecord(Page page) {
        transactionNumber = page.getInt(Integer.BYTES);
    }

    @Override
    public int getRecordOperatorNumber() {
        return LogOperator.ROLLBACK.value;
    }

    @Override
    public int getTransactionNumber() {
        return transactionNumber;
    }

    @Override
    public String toString() {
        return "<ROLLBACK " + transactionNumber + ">";
    }

    public static int writeToLog(LogManager logManager, int transactionNumber) {
        byte[] recordBytes = new byte[2*Integer.BYTES];
        Page p = new Page(recordBytes);
        p.setInt(0, LogOperator.ROLLBACK.value);
        p.setInt(Integer.BYTES, transactionNumber);
        return logManager.append(recordBytes);
    }


}
