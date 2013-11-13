package distsys.kdfs;

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 11/7/13
 */
public class BlockInfo {

    private int blockID;
    private String filename;
    private int offset;
    private int fileLen;

    public BlockInfo(int blockID, String filename, int offset, int fileLen) {
        this.blockID = blockID;
        this.filename = filename;
        this.offset = offset;
        this.fileLen = fileLen;
    }

    /**
     * Returns true if pos is a position contained within this block
     *
     * @param pos Position offset in the file
     * @return Whether or not pos is in the file
     */
    public boolean containsPosition(long pos) {
        return (pos >= offset && pos < offset + fileLen);
    }

    public int getOffset() {
        return offset;
    }

    public int getFileLen() {
        return fileLen;
    }

}
