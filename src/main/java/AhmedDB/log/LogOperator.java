package AhmedDB.log;

/**
 * The {@link LogManager} sees each log record as a byte array. Each
 * kind of log record has its own class, which is responsible for embedding the
 * appropriate values in the byte array. The first value in each array will be an integer
 * that denotes the operator of the record, that integer is defined as a value of this {@link LogOperator} enum.
 */
public enum LogOperator {
    CHECKPOINT(0),
    START(1),
    COMMIT(2),
    ROLLBACK(3),
    SETINT(4),
    SETSTRING(5);

    public final int value;
    LogOperator(int value){
        this.value = value;
    }

}
