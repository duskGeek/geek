package log4j;


import com.amazonaws.util.Base64;

public class Log4jTest {
    public static void main(String[] args) {

        String str="e4264881ca550293eb2539709c705972";
        byte[] a=Base64.decode(str);

        for (byte b: a) {
            System.out.println(b);
        }
    }
}
