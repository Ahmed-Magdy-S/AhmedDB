package AhmedDB.log;

import AhmedDB.file.Page;

public class CommitRecord extends LogRecord {
    public CommitRecord(Page page) {

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
