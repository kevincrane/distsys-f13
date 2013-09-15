package distsys.io;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Prashanth
 * Date: 9/12/13
 * Time: 3:58 AM
 * To change this template use File | Settings | File Templates.
 */
public class TrasactionalIOTest {
    public static void main(String args[]) throws IOException {
        byte b;
        String s;
        byte[] bytes;
        TransactionalFileInputStream tin = null;
        TransactionalFileOutputStream tio = null;

        try {
            tin = new TransactionalFileInputStream(new File("/Users/Prashanth/Documents/workspace/distsys-f13/yolo.txt"));
            do {
                b = (byte) tin.read();
                bytes = new byte[1];
                bytes[0] = b;
                s = new String(bytes, "UTF-8");
                System.out.print(s);
            } while (b != -1);

            tio = new TransactionalFileOutputStream(new File("/Users/Prashanth/Documents/workspace/distsys-f13/yolo.txt"));
            s = "Yolo is the motto. hyfr!";
            bytes = s.getBytes();
            for (byte b1 : bytes) {
                tio.write(b1);
            }
        } finally {
            if (tio != null) {
                tio.close();
            }
            if (tin != null) {
                tin.close();
            }
        }
    }
}
