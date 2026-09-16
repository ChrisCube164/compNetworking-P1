import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;


public class TimeClient
{
    public static void main(String[] args) throws Exception {
        try (var socket = new Socket("127.0.0.1", 21037)) {
            socket.setSoTimeout(3000);

            InputStream input = socket.getInputStream();
            byte[] data = input.readNBytes(4);

            if (data.length < 4) {
                System.err.println("Server sent only " + data.length + " bytes");
                return;
            }

            int signed = ByteBuffer.wrap(data).getInt();
            long unsigned = Integer.toUnsignedLong(signed);
            long window = unsigned / 60;

            System.out.println("Raw bytes:       " + data.length);
            System.out.println("Signed int:      " + signed);
            System.out.println("Unsigned long:    " + unsigned);
            System.out.println("Window (÷ 60):   " + window);

        }
    }
}
