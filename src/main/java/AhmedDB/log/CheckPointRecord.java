package AhmedDB.log;

public class CheckPointRecord extends LogRecord {


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
