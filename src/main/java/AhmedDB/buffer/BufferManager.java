package AhmedDB.buffer;


import AhmedDB.file.FileManager;
import AhmedDB.file.LogicalBlock;
import AhmedDB.log.LogManager;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * Each database system has one {@link BufferManager} object, which is created during
 * system startup.
 */
public class BufferManager {
    private final Buffer[] bufferPool;
    private int numAvailable;
    private static final long MAX_TIME = 10000; // 10 seconds

    /**
     * Creates a buffer manager having the specified number
     * of buffer slots.
     * This constructor depends on a {@link AhmedDB.file.FileManager FileManager} and
     * {@link AhmedDB.log.LogManager LogManager} object.
     * @param numBuffs the number of buffer slots to allocate (size of buffer pool)
     */
    public BufferManager(FileManager fileManager, LogManager logManager, int numBuffs) {
        bufferPool = new Buffer[numBuffs];
        numAvailable = numBuffs;
        for (int i=0; i < numBuffs; i++)
            bufferPool[i] = new Buffer(fileManager, logManager);
    }

    /**
     * Returns the number of available (i.e. unpinned) buffers.
     * @return the number of available buffers
     */
    public synchronized int available() {
        return numAvailable;
    }

    /**
     * Flushes the dirty buffers modified by the specified transaction.
     * @param txNum the transaction's id number
     */
    public synchronized void flushAll(int txNum) {
        for (Buffer buff : bufferPool)
            if (buff.modifyingTx() == txNum)
                buff.flush();
    }


    /**
     * Unpins the specified data buffer. If its pin count
     * goes to zero, then notify any waiting threads.
     * @param buff the buffer to be unpinned
     */
    public synchronized void unpin(Buffer buff) {
        buff.unpin();
        if (!buff.isPinned()) {
            numAvailable++;
            notifyAll();
        }
    }

    /**
     * Pins a buffer to the specified block (assign a buffer to the block), potentially
     * waiting until a buffer becomes available.
     * If no buffer becomes available within a fixed
     * time period, then a {@link BufferAbortException} is thrown.
     * A disk read can occur only during a call to pin
     * when the specified block is not currently in a buffer.
     * A disk write can occur only during a call to pin or
     * flushAll methods. A call to pin will cause a disk write if the replaced page has been
     * modified, and a call to flushAll will cause a disk write for each page modified by
     * the specified transaction.
     * @param logicalBlock a reference to a disk block
     * @return the buffer pinned to that block
     */
    public synchronized Buffer pin(LogicalBlock logicalBlock) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buff = tryToPin(logicalBlock);
            while (buff == null && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
                //When a waiting thread resumes, it continues in its loop:
                //it will call tryToPin again after some time to check if it's possible to
                // pin or not, then returns buffer if it can be pined or null if can't.
                buff = tryToPin(logicalBlock);
            }
            //cannot pin a buffer to a given block
            if (buff == null) throw new BufferAbortException();
            return buff;
        }
        catch(InterruptedException e) {
            throw new BufferAbortException();
        }
    }

    private boolean waitingTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }

    /**
     * Tries to pin a buffer to the specified block.
     * If there is already a buffer assigned to that block
     * then that buffer is used;
     * otherwise, an unpinned buffer from the pool is chosen.
     * Returns a null value if there are no available buffers.
     * @param logicalBlock a reference to a disk block
     * @return the pinned buffer
     */
    private Buffer tryToPin(LogicalBlock logicalBlock) {
        Buffer buff = findExistingBuffer(logicalBlock);
        //if there is no block associated with the Buffer buff variable
        if (buff == null) {
            buff = chooseUnpinnedBuffer();
            //all buffer are pinned
            if (buff == null) return null;
            else buff.assignToBlock(logicalBlock);
        }

        if (!buff.isPinned()) numAvailable--;
        buff.pin();
        return buff;
    }

    /**
     * Loop through a buffer poo to check if there is a buffer contains
     * a block equals to the searching block parameter
     * @param logicalBlock searching by a passed block argument to find if it's contained inside a buffer
     * @return the buffer that contains the block found or it will return null.
     */
    private Buffer findExistingBuffer(LogicalBlock logicalBlock) {
        for (Buffer buff : bufferPool) {
            LogicalBlock b = buff.getAssociatedLogicalBlock();
            if (b != null && b.equals(logicalBlock))
                return buff;
        }
        return null;
    }

    /**
     * Loop through a buffer pool to return a buffer that is not pinned
     * @return unpinned buffer
     */
    private Buffer chooseUnpinnedBuffer() {
        for (Buffer buff : bufferPool)
            if (!buff.isPinned())
                return buff;
        return null;
    }
}
