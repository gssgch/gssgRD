package main.java.com.ch.algorithm.old;

/**
 * 给一组数列，找出最大数和最大数所在的位置，然后把100放在最大数的前面
 */
public class AddNum {

    public static void main(String[] args) {
        int[] a = new int[] { 18, 25, 7, 36, 89, 13, 2, 63 };
        int[] a1 = new int[] { 18, 25, 7, 36, 89, 13, 2, 63 };
        int[] b = new int[9];
        int d = 0;
        for (int i = 0; i < a1.length; i++) {
            if (a1[i] > a1[d]) {
                a1[d] = a1[i];
                d = i;
            }
        }
        System.out.print("最大数是" + a1[d] + "位置在" + d + "\n");
        for (int i = 0; i < 8; i++) {
            b[i] = a[i];
            System.out.print(b[i] + " ");
        }
        System.out.println();
        for (int k = 8; k >= 4; k--) {
            b[k] = b[k - 1];
        }
        b[4] = 100;
        for (int n : b) {
            System.out.print(n + " ");
        }
        System.out.println();
    }
}