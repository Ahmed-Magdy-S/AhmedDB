package AhmedDB.log;

import AhmedDB.file.Page;

public class CheckPointRecord extends LogRecord {


    @Override
    public int getRecordOperatorNumber() {
        return LogOperator.CHECKPOINT.value;
    }

    /**
     * checkpoint records has no transactions
     */
    @Override
    public int getTransactionNumber() {
        return -1;
    }

    @Override
    public String toString() {
        return "<CHECKPOINT>";
    }

    public static int writeToLog(LogManager logManager) {
        byte[] recordBytes = new byte[Integer.BYTES];
        Page p = new Page(recordBytes);
        p.setInt(0, LogOperator.CHECKPOINT.value);
        return logManager.append(recordBytes);
    }

}
