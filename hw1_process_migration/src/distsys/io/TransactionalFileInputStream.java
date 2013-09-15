package distsys.io;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth, kevin
 * Date: 9/6/13
 */

public class TransactionalFileInputStream extends InputStream implements Serializable {
    private boolean isMigrating = false;
    private File file;
    private int currentOffset;

    public TransactionalFileInputStream(File file) {
        this.file = file;
        this.currentOffset = 0;
    }

    // Reads the next byte of data from the input stream.
    @Override
    public int read() throws java.io.IOException {
        RandomAccessFile in = null;
        try {
            in = makeInputStream();
            int i = in.read();
            currentOffset++;
            return i;
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    // Reads some number of bytes from the input stream and stores them into the buffer array bytes.
    @Override
    public int read(byte[] bytes) throws java.io.IOException {
        RandomAccessFile in = null;
        try {
            in = makeInputStream();
            int bytesRead = in.read(bytes);
            if (bytesRead > 0) {
                currentOffset+=bytesRead;
            }
            return bytesRead;
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    // Reads up to len bytes of data from the input stream into an array of bytes.
    @Override
    public int read(byte[] bytes, int offset, int length) throws java.io.IOException {
        RandomAccessFile in = null;
        try {
            in = makeInputStream();
            int bytesRead = in.read(bytes, offset, length);
            if (bytesRead > 0) {
                currentOffset+=bytesRead;
            }
            return bytesRead;
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    protected RandomAccessFile makeInputStream() throws IOException {
        RandomAccessFile in = new RandomAccessFile(file, "r");
        FileInputStream sin = new FileInputStream(file);
        in.seek(currentOffset);
        return in;
    }

    @Override
    public void close() throws IOException {
        currentOffset = 0;
        super.close();
    }

}