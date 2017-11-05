package main.java.com.ch.pos;


import java.security.MessageDigest;
import java.util.Scanner;

/**
 * Created by ch on 2017/5/25.
 * Email: 824203453@qq.com
 */
public class GetUserPwd {
    private static final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private static String getFormattedText(byte[] bytes) {
        int len = bytes.length;
        StringBuilder buf = new StringBuilder(len * 2);
        for (int j = 0; j < len; j++) {
            buf.append(HEX_DIGITS[(bytes[j] >> 4) & 0x0f]);
            buf.append(HEX_DIGITS[bytes[j] & 0x0f]);
        }
        return buf.toString();
    }

    private static String getUserIdentify(String k1, String k2, String k3) {
//        String str = k1.toUpperCase().replaceAll("-", "") + k2 + k3;
        return compressString("SHA1",compressString("SHA1", k3 +k2+k1)+"user");
    }

    public static void main(String[] args) {

        try {

        if(args.length!=3){
            String str = new GetUserPwd().getStr();
            System.out.println("请牢记您的密码：" + str);
        }/*else{
        String k1 = args[0];
        String k2 = args[1];
        String k3 = args[2];
            //
        String str = new GetUserPwd().getUserIdentify(k1, k2, k3);
        System.out.println("请牢记您的密码：" + str);
        }*/
        }catch (Exception e){
//            e.printStackTrace();
        }

    }
    private String getStr() throws Exception {
        Process process = Runtime.getRuntime().exec(
                new String[]{"wmic", "cpu", "get", "ProcessorId"});
        process.getOutputStream().close();
        Scanner sc1 = new Scanner(process.getInputStream());
        String property = sc1.next();
        String str1 = sc1.next();

        Process process2 = Runtime.getRuntime().exec(
                new String[]{"wmic", "bios", "get", "serialnumber"});
        process2.getOutputStream().close();
        Scanner sc2 = new Scanner(process2.getInputStream());
        String property2 = sc2.next();
        String str2 = sc2.next();

//        InetAddress localInetAddress = InetAddress.getLocalHost();
//        System.out.println(localInetAddress);
//        byte[] arrayOfByte = NetworkInterface.getByInetAddress(localInetAddress).getHardwareAddress();

//        System.out.println(arrayOfByte);
       /* StringBuffer localStringBuffer = new StringBuffer("");

        for (int j = 0; j < arrayOfByte.length; j++) {
            if (j != 0) {
                localStringBuffer.append("-");
            }
            int k = arrayOfByte[j] & 0xFF;
            String str3 = Integer.toHexString(k);
            if (str3.length() == 1) {
                localStringBuffer.append("0" + str3);
            } else {
                localStringBuffer.append(str3);
            }
        }
        String str1 = localStringBuffer.toString().toUpperCase().replaceAll("-", "");
        */

        Process process3 = Runtime.getRuntime().exec(
                new String[]{"wmic", "bios", "get", "SoftwareElementID"});
        process3.getOutputStream().close();
        Scanner sc3 = new Scanner(process3.getInputStream());
        String property3 = sc3.next();
        String str3 = sc3.next();
        return compressString("SHA1",compressString("SHA1", str3 +str2+str1)+"user");
    }

    private static String compressString(String algorithm, String str) {
        if (str == null) {
            return null;
        }
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(algorithm);
            messageDigest.update(str.getBytes());
            return getFormattedText(messageDigest.digest());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
