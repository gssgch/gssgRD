package main.java.com.ch.test;

import scala.Char;

/**
 * Created by ch on 2016/12/30.
 */
public class BinaryTest {
    public static void main(String[] args) {
//        toHex(60);
//        toBinary(-6);


    }

    // 10进制 => 十六进制
    public static void toHex(int num) {
        transfer(num, 15, 4);
    }

    // 10进制 => 2进制
    public static void toBinary(int num) {
        transfer(num, 1, 1);
    }

    // 10进制 => 8进制
    public static void toOctal(int num) {
        transfer(num, 7, 3);
    }

    public static void transfer(int num, int base, int offset) {
        if (num == 0) {
            System.out.println("0");
            return;
        }
        // 对应关系表
        char[] chs = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
        char arr[] = new char[32];
        int pos = arr.length;
        while (num != 0) {
            int temp = num & base;
            arr[--pos] = chs[temp];
            num = num >>> offset;
        }
        for (int x = pos; x < arr.length; x++) {
            System.out.print(arr[x]);
        }
    }
}
