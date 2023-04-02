package AhmedDB.log;

import AhmedDB.file.Page;

public class StartRecord extends LogRecord {

    private final int transactionNumber;

    public StartRecord(Page page) {
        transactionNumber = page.getInt(Integer.BYTES);
    }

    @Override
    public int getRecordOperatorNumber() {
        return LogOperator.START.value;
    }

    @Override
    public int getTransactionNumber() {
        return transactionNumber;
    }

    @Override
    public String toString() {
        return "<START " + transactionNumber + ">";
    }

    public static int writeToLog(LogManager logManager, int transactionNumber) {
        byte[] recordBytes = new byte[2*Integer.BYTES];
        Page p = new Page(recordBytes);
        p.setInt(0, LogOperator.START.value);
        p.setInt(Integer.BYTES, transactionNumber);
        return logManager.append(recordBytes);
    }


}
