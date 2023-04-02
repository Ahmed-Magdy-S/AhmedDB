package AhmedDB.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 * The {@link FileManager} class handles the actual interaction with the OS file system.
 */
public class FileManager {
    private final File dbDirectory;
    private final int blockSize;
    private final boolean isNew;

    private int blocksRead = 0;
    private int blocksWritten = 0;

    /**
     * Each RandomAccessFile object in the map openFiles corresponds to an
     * open file
     */
    private final Map<String, RandomAccessFile> openFiles = new HashMap<>();

    /**
     * The constructor takes two arguments: a string denoting the name of the database and an
     * integer denoting the size of each block. The database name is used as the name of the
     * folder that contains the files for the database; this folder is located in the engine’s
     * current directory. If no such folder exists, then a folder is created for a new database.
     * The method isNew returns true in this case and false otherwise. This method is
     * needed for the proper initialization of a new database.
     * @param dbDirectory the name of the database, it will be a folder name in this case.
     * @param blockSize denoting the size of each block.
     */
    public FileManager(File dbDirectory, int blockSize) throws IOException {

        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        isNew = !dbDirectory.exists();
        // create the directory if the database is new
        if (isNew) {
            boolean isDbDirectoryCreated = dbDirectory.mkdirs();
            if (isDbDirectoryCreated) System.out.println("A database directory has been created: " + dbDirectory.getAbsolutePath());
            else {
                System.err.println("Cannot create database directory");
                throw new IOException("cannot create database directory");
            }
        }
        // remove any leftover temporary tables
        for (String filename : dbDirectory.list())
            if (filename.startsWith("temp"))
                new File(dbDirectory, filename).delete();
    }

    /**
     * The method transfer the contents of the specified block in file into the specified memory page,
     * so that it can be read from memory.
     * @param block the logical block reference
     * @param page the memory page (a byteBuffer in memory) that hold block contents
     */
    public synchronized void read(LogicalBlock block, Page page) {
        try {
            RandomAccessFile randomAccessFile = getRandomAccessFile(block.getFileName());
            //move the pointer (cursor) to the given starting block position
            randomAccessFile.seek((long) block.getNumber() * blockSize);
            //transferring the reading of sequence of bytes of block into byteBuffer (memory allocated buffer)
            randomAccessFile.getChannel().read(page.getByteBufferContentsPosition());
            //track the number of reading blocks
            blocksRead++;
        }
        catch (IOException e) {
            throw new RuntimeException("cannot read block " + block);
        }
    }

    /**
     * The method transfer the contents of the specified memory page into the specified block in file
     * @param block the logical block reference
     * @param page the memory page (a byteBuffer in memory) that hold block contents
     */
    public synchronized void write(LogicalBlock block, Page page) {
        try {
            RandomAccessFile randomAccessFile = getRandomAccessFile(block.getFileName());
            randomAccessFile.seek((long) block.getNumber() * blockSize);
            randomAccessFile.getChannel().write(page.getByteBufferContentsPosition());

            //track the number of written block
            blocksWritten++;
        }
        catch (IOException e) {
            throw new RuntimeException("cannot write block" + block);
        }
    }

    /**
     * The method seeks to the end of the file and writes an empty array of bytes to it, which
     * causes the OS to automatically extend the file.
     * @param filename the file at which a block will be appended to.
     * @return reference of the logical block of the file
     */
    public synchronized LogicalBlock append(String filename) {
        int newBlockNum = length(filename);
        LogicalBlock blk = new LogicalBlock(filename, newBlockNum);
        byte[] emptyBytesArray = new byte[blockSize];
        try {
            RandomAccessFile randomAccessFile = getRandomAccessFile(blk.getFileName());
            randomAccessFile.seek((long) blk.getNumber() * blockSize);
            randomAccessFile.write(emptyBytesArray);
        }
        catch (IOException e) {
            throw new RuntimeException("cannot append block" + blk);
        }
        return blk;
    }

    public int length(String filename) {
        try {
            RandomAccessFile file = getRandomAccessFile(filename);
            return (int) (file.length() / blockSize);
        }
        catch (IOException e) {
            throw new RuntimeException("cannot access " + filename);
        }
    }

    public boolean isNew() {
        return isNew;
    }
    public int blockSize() {
        return blockSize;
    }

    private RandomAccessFile getRandomAccessFile(String filename) throws IOException {
        RandomAccessFile randomAccessFile = openFiles.get(filename);
        if (randomAccessFile == null) {
            //create a new file for a db table
            File dbTableFile = new File(dbDirectory, filename);
            /*
            Note that files are opened in “rws” mode. The “rw” portion specifies that
            the file is open for reading and writing. The “s” portion specifies that the operating
            system should not delay disk I/O in order to optimize disk performance; instead,
            every write operation must be written immediately to the disk. This feature
            ensures that the database engine knows exactly when disk writes occur, which will
            be especially important for implementing the data recovery algorithms.
             */
            randomAccessFile = new RandomAccessFile(dbTableFile, "rws");
            openFiles.put(filename, randomAccessFile);
        }
        return randomAccessFile;
    }

    public synchronized int blocksRead() {
        return blocksRead;
    }
    public synchronized int blocksWritten() {
        return blocksWritten;
    }

}
