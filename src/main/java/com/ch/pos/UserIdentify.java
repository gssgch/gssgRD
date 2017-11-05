package main.java.com.ch.pos;

import java.security.MessageDigest;
import java.util.Scanner;

/**
 * Created by ch on 2017/5/25.
 * Email: 824203453@qq.com
 */
public class UserIdentify {
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
/*
    private static String getUserIdentify(String k1, String k2, String k3) {
        String str = k1.toUpperCase().replaceAll("-", "") + k2 + k3;
        return compressString("SHA1", str);
    }


    public static void main(String[] args) throws IOException {

        String k1 = args[0];
        String k2 = args[1];
        String k3 = args[2];
        String str = new UserIdentify().getUserIdentify(k1, k2, k3);
        System.out.println("请牢记您的密码：" + str);
        */
/*
        Process process = Runtime.getRuntime().exec(
                new String[]{"wmic", "cpu", "get", "ProcessorId"});
        process.getOutputStream().close();
        Scanner sc2 = new Scanner(process.getInputStream());
        String property = sc2.next();
        String serial = sc2.next();
        System.out.println(property + ": " + serial);

        Process process2 = Runtime.getRuntime().exec(
                new String[]{"wmic", "bios", "get", "serialnumber"});
        process.getOutputStream().close();
        Scanner sc3 = new Scanner(process2.getInputStream());
        String property2 = sc3.next();
        String serial2 = sc3.next();
        System.out.println(property2 + ": " + serial2);


        InetAddress localInetAddress = InetAddress.getLocalHost();
        System.out.println(localInetAddress);
        byte[] arrayOfByte = NetworkInterface.getByInetAddress(localInetAddress).getHardwareAddress();

        System.out.println(arrayOfByte);
        StringBuffer localStringBuffer = new StringBuffer("");

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
        System.out.println(str1);
//        String str2 = compressString(str1 + "user").replace("=", "");
        String str2 = compressString("SHA1", str1 + serial + serial2);*//*

    }
*/

    public static Boolean checkUser(String str1, String str2) {
        try {
            String pwd = new UserIdentify().getStr();
            if ("user".equals(str1) && str2.equals(pwd))
                return true;
        } catch (Exception e) {
            System.out.println("checkUser catch error");
            return false;
        }
        return false;
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

   /* public static String compressString(String paramString) {
        if ((paramString == null) || (paramString.length() == 0)) {
            return paramString;
        }
        ByteArrayOutputStream localByteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream localGZIPOutputStream = null;
        try {
            localGZIPOutputStream = new GZIPOutputStream(localByteArrayOutputStream);
            localGZIPOutputStream.write(paramString.getBytes());

        } catch (Exception localException) {
            localException.printStackTrace();
        } finally {
            if (localGZIPOutputStream != null) {
                try {
                    localGZIPOutputStream.close();
                } catch (IOException localIOException3) {
                    localIOException3.printStackTrace();
                }
            }
        }
        return new BASE64Encoder().encode(localByteArrayOutputStream.toByteArray());
    }


    public static String uncompressString(String paramString) {
        if ((paramString == null) || (paramString.length() == 0))
            return null;
        ByteArrayOutputStream localByteArrayOutputStream = new ByteArrayOutputStream();
        ByteArrayInputStream localByteArrayInputStream = null;
        GZIPInputStream localGZIPInputStream = null;
        byte[] arrayOfByte1 = null;
        String str = null;
        try {
            arrayOfByte1 = new BASE64Decoder().decodeBuffer(paramString);
            localByteArrayInputStream = new ByteArrayInputStream(arrayOfByte1);
            localGZIPInputStream = new GZIPInputStream(localByteArrayInputStream);
            byte[] arrayOfByte2 = new byte[1024];
            int i = -1;
            while ((i = localGZIPInputStream.read(arrayOfByte2)) != -1) {
                localByteArrayOutputStream.write(arrayOfByte2, 0, i);
            }
            str = localByteArrayOutputStream.toString();
        } catch (IOException localIOException2) {
            localIOException2.printStackTrace();
        } finally {
            try {
                if (localGZIPInputStream != null)
                    localGZIPInputStream.close();
                if (localByteArrayInputStream != null)
                    localByteArrayInputStream.close();
                if (localByteArrayOutputStream != null)
                    localByteArrayOutputStream.close();
            } catch (IOException localIOException4) {
                localIOException4.printStackTrace();
            }
        }
        return str;
    }

    public String getMAC() {
        String mac = null;
        try {
            Process pro = Runtime.getRuntime().exec("cmd.exe /c ipconfig/all");

            InputStream is = pro.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String message = br.readLine();

            int index = -1;
            while (message != null) {
                if ((index = message.indexOf("Physical Address")) > 0) {
//                if ((index = message.indexOf("物理地址")) > 0) {
                    mac = message.substring(index + 36).trim();
                    break;
                }
                message = br.readLine();
            }
            System.out.println(mac);
            br.close();
            pro.destroy();
        } catch (IOException e) {
            System.out.println("Can't get mac address!");
            return null;
        }
        return mac;
    }
*/
}
