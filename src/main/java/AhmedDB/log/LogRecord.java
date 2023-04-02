package AhmedDB.log;

import AhmedDB.file.Page;

/**
 * The {@link LogManager} sees each log record as a byte array. Each
 * kind of log record has its own class, which is responsible for embedding the
 * appropriate values in the byte array. The first value in each array will be an integer
 * that denotes the operator of the record; the operator can be one of the constants in {@link LogOperator} enum.
 * The remaining values depend on the operator — a quiescent checkpoint record has no other values,
 * an update record has five other values, and the other records have one other value.
 */
public abstract class LogRecord {

    /**
     * The method return a log operator number that is defined in {@link LogOperator} enum.
     * @return log operator number
     */
    public abstract int getRecordOperatorNumber();

    /**
     * It returns the ID of the transaction that wrote the log record.
     * This method makes sense for all log records except
     * checkpoint records, which return a dummy ID value.
     * @return Transaction ID Number
     */
    public abstract int getTransactionNumber();

    /**
     * restores any changes stored in that record.
     * @param transactionNumber the transaction ID number that will be restored.
     */
    public abstract void undo(int transactionNumber);


    public static LogRecord createLogRecord (byte[] bytes){
        Page page = new Page(bytes);
        int operatorNum = page.getInt(0);

        if (operatorNum == LogOperator.SETSTRING.value){
            return new SetStringRecord(page);
        }
        else if (operatorNum == LogOperator.SETINT.value){
            return new SetIntRecord(page);
        }
        else if (operatorNum == LogOperator.START.value){
            return new StartRecord(page);
        }
        else if (operatorNum == LogOperator.COMMIT.value){
            return new CommitRecord(page);
        }
        else if (operatorNum == LogOperator.ROLLBACK.value){
            return new RollBackRecord(page);
        }
        else if (operatorNum == LogOperator.CHECKPOINT.value){
            return new CheckPointRecord();
        }

        return null;


    }

}
