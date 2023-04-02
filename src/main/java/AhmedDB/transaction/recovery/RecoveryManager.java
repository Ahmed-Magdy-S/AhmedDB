package AhmedDB.transaction.recovery;

import AhmedDB.buffer.Buffer;
import AhmedDB.buffer.BufferManager;
import AhmedDB.file.LogicalBlock;
import AhmedDB.log.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Each transaction has its own RecoveryManager object, whose methods write the
 * appropriate log records for that transaction. For example, the constructor writes a
 * start log record to the log; the commit and rollback methods write
 * corresponding log records; and the setInt and setString methods extract the
 * old value from the specified buffer and write an update record to the log. The
 * rollback and recover methods perform the rollback (or recovery) algorithms.
 * A RecoveryManager object uses undo-only recovery with value-granularity data
 * items. Its code can be divided into two areas of concern: code to implement log
 * records, and code to implement the rollback and recovery algorithms.
 */
public class RecoveryManager {
   private LogManager logManager;
   private BufferManager bufferManager;
   private Transaction transaction;
   private int transactionNumber;

   public RecoveryManager(Transaction transaction, int transactionNumber, LogManager logManager, BufferManager bufferManager){
       this.transaction = transaction;
       this.transactionNumber = transactionNumber;
       this.logManager = logManager;
       this.bufferManager = bufferManager;
   }

   public void commit(){
       bufferManager.flushAll(transactionNumber);
       int lsn = CommitRecord.writeToLog(logManager, transactionNumber);
       logManager.flush(lsn);
   }

   public void rollback(){
       doRollback();
       bufferManager.flushAll(transactionNumber);
       int lsn = RollBackRecord.writeToLog(logManager, transactionNumber);
       logManager.flush(lsn);
   }

   public void recover(){
       doRecover();
       bufferManager.flushAll(transactionNumber);
       int lsn = CheckPointRecord.writeToLog(logManager);
       logManager.flush(lsn);
   }

   public int setInt(Buffer buffer, int offset, int newVal){
       //int oldVal = buffer.getAssociatedPage().getInt(offset);
       LogicalBlock logicalBlock = buffer.getAssociatedLogicalBlock();
       return  SetIntRecord.writeToLog(logManager,transactionNumber, logicalBlock, offset, newVal);
   }

    public int setString(Buffer buffer, int offset, String newVal){
       // String oldVal = buffer.getAssociatedPage().getString(offset);
        LogicalBlock logicalBlock = buffer.getAssociatedLogicalBlock();
        return  SetStringRecord.writeToLog(logManager,transactionNumber, logicalBlock, offset, newVal);
    }

   private void doRollback(){
       Iterator<byte[]> iterator = logManager.iterator();
       while (iterator.hasNext()){
           byte[] bytes = iterator.next();
           LogRecord logRecord = LogRecord.createLogRecord(bytes); // return a specific logRecord
           if (logRecord != null && logRecord.getTransactionNumber() == transactionNumber){
               if (logRecord.getRecordOperatorNumber() == LogOperator.START.value) return;
               logRecord.undo(transactionNumber);
           }
       }
   }

   private void  doRecover(){
       Collection<Integer> finishedTransactions = new ArrayList<>();
       Iterator<byte[]> iterator = logManager.iterator();
       while (iterator.hasNext()){
           byte[] bytes = iterator.next();
           LogRecord logRecord = LogRecord.createLogRecord(bytes);
           assert logRecord != null;
           if (logRecord.getRecordOperatorNumber() == LogOperator.CHECKPOINT.value) return;
           if (logRecord.getRecordOperatorNumber() == LogOperator.COMMIT.value || logRecord.getRecordOperatorNumber() == LogOperator.ROLLBACK.value){
               finishedTransactions.add(logRecord.getTransactionNumber());
           }
           else if (!finishedTransactions.contains(logRecord.getTransactionNumber())) logRecord.undo(transactionNumber);
       }
   }


}
