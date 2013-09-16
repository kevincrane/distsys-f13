package distsys.io;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 9/6/13
 */
public class TransactionalFileOutputStream extends OutputStream implements Serializable {
    private File file;
    private int currentOffset;

    public TransactionalFileOutputStream(File file) throws FileNotFoundException {
        this.file = file;
        this.currentOffset = 0;
    }

    // Writes a single byte to output stream
    @Override
    public void write(int i) throws IOException {
        RandomAccessFile out = null;
        try {
            out = makeOutputStream();
            out.write(i);
            currentOffset++;
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    // Writes bytes.length bytes from the specified byte array to this output stream.
    @Override
    public void write(byte[] bytes) throws java.io.IOException {
        RandomAccessFile out = null;
        try {
            out = makeOutputStream();
            out.write(bytes);
            currentOffset+=bytes.length;
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    // Writes len bytes from the specified byte array starting at offset off to this output stream.
    @Override
    public void write(byte[] bytes, int offset, int length) throws java.io.IOException {
        RandomAccessFile out = null;
        try {
            out = makeOutputStream();
            out.write(bytes, offset, length);
            currentOffset+=length;
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    protected RandomAccessFile makeOutputStream() throws IOException {
        RandomAccessFile out = new RandomAccessFile(file, "rw");
        out.seek(currentOffset);
        return out;
    }

    @Override
    public void close() throws IOException {
        currentOffset = 0;
        super.close();
    }
}
