package main.java.com.ch.algorithm.old;

//定义一个包含30个整形元素的数组，按顺序依次赋值为从2开始的偶数，然后按照顺序每5个数求出一个平均值，
//将所得的平均值依次存放在令一个数组中输出
public class Pingjunshu {

    public static void main(String[] args) {

        int[] a = new int[30];
        for (int i = 0; i < 30; i++) {
            a[i] = (i + 1) * 2;
        }
        for (int n : a) {
            System.out.print(n + " ");
        }
        System.out.println();

        int t = 0;
        int j = 0;
        int[] b = new int[6];
        for (int i = 0; i < a.length; i++) {
            t += a[i];
            if (((i + 1) % 5) == 0) {
                b[j] = t / 5;
                j++;
                t = 0;
            }
        }
        for (int n : b) {
            System.out.print(n + " ");
        }

    }

}
