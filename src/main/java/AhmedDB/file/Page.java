package AhmedDB.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * A Page object holds the contents of a disk block (act as a holder for one block data) in a specific size of memory.
 * Its first constructor creates a page that gets its memory from an operating system I/O buffer;
 * this constructor is used by the buffer manager.
 * Its second constructor creates a page that gets its memory from a Java array;
 * this constructor is used primarily by the log manager.
 * The various get and set methods enable clients to store or access values at
 * specified locations of the page.
 * A page can hold three value types: ints, strings,
 * and “blobs” (i.e., arbitrary arrays of bytes).
 */
public class Page {
    private final ByteBuffer byteBuffer;
    public static final Charset CHARSET = StandardCharsets.US_ASCII;

    /**
     * The constructor responsible for creating byte buffer in a given size in memory
     */
    public Page(int blockSize) {
        //allocate a size equals to blockSize of a memory so that we can use it to store some bytes data
        byteBuffer = ByteBuffer.allocateDirect(blockSize);
    }

    /**
     * The constructor responsible for creating log pages
     */
    public Page(byte[] bytes) {
        /*
         * The ByteBuffer.wrap() method creates a ByteBuffer object that wraps an existing byte array,
         * which means that changes made to the buffer will be reflected in the original array.
         */
        byteBuffer = ByteBuffer.wrap(bytes);
    }

    /**
     * Absolute get method for reading an int value.
     * Reads four bytes at the given index (offset),
     * composing them into an int value according to the current byte order.
     * @param offset The index (offset) from which the bytes will be read
     * @return The int value at the given index
     */
    public int getInt(int offset) {
        return byteBuffer.getInt(offset);
    }

    public void setInt(int offset, int number) {
        if (byteBuffer.capacity() - offset < Integer.BYTES) throw new RuntimeException("Value does not fit in page");
        byteBuffer.putInt(offset, number);
    }

    public void setShort(int offset, short number) {
        if (byteBuffer.capacity() - offset < Short.BYTES) throw new RuntimeException("Value does not fit in page");
        byteBuffer.putShort(offset, number);
    }

    public int getShort(int offset) {
        return byteBuffer.getShort(offset);
    }

    public void setBoolean(int offset, boolean bool) {
        byte b = bool ? (byte) 1 : 0;
        byteBuffer.put(offset, b);
    }

    public boolean getBoolean(int offset) {
        byte b = byteBuffer.get(offset);
        return b == 1;
    }

    public Date getDate(int offset) {
        return new Date(byteBuffer.getLong(offset));
    }
    public  void setDate(int offset, Date val) {
        byteBuffer.putLong(offset,val.getTime());
    }

    public byte[] getBytes(int offset) {
        byteBuffer.position(offset);
        int length = byteBuffer.getInt();
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        return bytes;
    }

    /**
     * Store bytes at specific location in byteBuffer.
     * saves a blob as two values: first the number of bytes in the specified blob
     * and then the bytes themselves.
     * The ByteBuffer class does not have methods to read and write strings, so
     * Page chooses to write string values as blobs
     *
     * @param offset the starting location to start storing
     * @param bytes  the bytes that will be stored
     */
    public void setBytes(int offset, byte[] bytes) {

        if (byteBuffer.capacity() - offset < bytes.length) throw new RuntimeException("Value does not fit in page");

        //Start location of saving bytes
        byteBuffer.position(offset);
        //save a number which is the number of bytes in the specified blob
        byteBuffer.putInt(bytes.length);
        //saving the blob bytes themselves.
        byteBuffer.put(bytes);
    }

    public String getString(int offset) {
        byte[] bytes = getBytes(offset);
        return new String(bytes, CHARSET);
    }

    /**
     * Store a string value into a specific location
     *
     * @param offset the location in which the value will be written
     * @param string the string that will be stored
     */
    public void setString(int offset, String string) {
        byte[] stringBytes = string.getBytes(CHARSET); //converts a string into a byte array
        setBytes(offset, stringBytes);
    }

    /**
     * The method determines the maximum length of the string, so it can determine the location
     * following the string.
     * It calculates the maximum size of the blob for a string having a specified number of characters.
     *
     * @param strlen the string length
     * @return the p
     */
    public static int maxLength(int strlen) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        // The Integer.BYTES represents a number which is
        // the number of bytes in the specified blob that come after that number.
        return Integer.BYTES + (strlen * (int) bytesPerChar);
    }

    /**
     * a package private method, needed by {@link FileManager }
     */
    ByteBuffer getByteBufferContentsPosition() {
        byteBuffer.position(0);
        return byteBuffer;
    }


}
