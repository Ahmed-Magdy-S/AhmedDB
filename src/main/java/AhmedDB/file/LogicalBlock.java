package AhmedDB.file;

/**
 * The class responsible for creating blocks references for block-level access to the files.
 * A {@link LogicalBlock} object identifies a specific logical block by its file name and
 * logical block number. For example, the statement:
 * <pre>{@code LogicalBlock blk = new LogicalBlock("student.tbl", 23)}</pre>
 * creates a <b>reference</b> to logical block 23 of the file student.tbl.
 */
public class LogicalBlock {

    private final String filename;
    private final int number;

    public LogicalBlock(String filename, int number ){
        this.filename = filename;
        this.number = number;
    }

    /**
     * the method get the file name that a given block is stored into.
     * @return the file name of the stored block
     */
    public String getFileName(){
        return filename;
    }

    /**
     *  The method get the logical block number that is stored in a memory page
     * @return the block number
     */
    public int getNumber(){
        return number;
    }

    @Override
    public String toString(){
        return "[file " + filename + ", logical block number " + number + "]";

    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LogicalBlock blk)) return false;
        return filename.equals(blk.filename) && number == blk.number;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }



}
