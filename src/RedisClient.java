import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class RedisClient
{
    public static void main(String[] args) throws Exception {
        try (var socket = new Socket("127.0.0.1", 21379)) {
            socket.setSoTimeout(3000);

            InputStream input = socket.getInputStream();
            OutputStream output = socket.getOutputStream();

            String key = "test:mykey";
            String command = "*2\r\n"
                    + "$4\r\n"
                    + "INCR\r\n"
                    + "$" + key.length() + "\r\n"
                    + key + "\r\n";




            output.write(command.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            output.flush();


            var line = new java.io.ByteArrayOutputStream();
            while (true) {
                int next = input.read();
                if (next == -1) {
                    if (line.size() == 0) {
                        throw new IOException("EOF was reached before CRLF");
                    }
                    throw new IOException("connection ended inside a query");
                }
                if (next == '\r') {
                    if (input.read() != '\n') {
                        throw new IOException("queries must end with CRLF");
                    }
                    break;
                }
                if (next == '\n') {
                    throw new IOException("queries must end with CRLF");
                }
                line.write(next);
            }

            System.out.println(line.toString(StandardCharsets.US_ASCII));

        }
    }
}
