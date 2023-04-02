package AhmedDB.log;

import AhmedDB.file.Page;

public class StartRecord extends LogRecord {
    public StartRecord(Page page) {

    }

    @Override
    public int getRecordOperatorNumber() {
        return 0;
    }

    @Override
    public int getTransactionNumber() {
        return 0;
    }

    @Override
    public void undo(int transactionNumber) {

    }
}
