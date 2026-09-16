import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class WhoisClient
{
    public static void main(String[] args) throws Exception {
        try (var socket = new Socket("127.0.0.1", 21043)) {
            socket.setSoTimeout(3000);

            InputStream input = socket.getInputStream();
            OutputStream output = socket.getOutputStream();

            output.write("alice\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            output.flush();

            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[1024];
            int bytesRead;
            while ((bytesRead = input.read(chunk)) != -1) {
                buffer.write(chunk, 0, bytesRead);
                if (buffer.size() > 8192) {
                    throw new IOException("WHOIS response too large");
                }
            }
            String response = buffer.toString(StandardCharsets.US_ASCII);


            System.out.println(response);


        }
    }
}
