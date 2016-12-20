package main.java.com.ch.algorithm.old;

//在一组数组中找出最小数，并确定其所在位置
public class Min {

    public static void main(String[] agrs) {
        int[] a = new int[] { 18, 25, 7, 36, 13, 2, 89, 63 };
        System.out.println("排列前的遍历结果是：");
        for (int n : a) {
            System.out.print(n + " ");
        }
        int q = 0;
        for (int i = 0; i < a.length; i++) {

            if (a[i] < a[q]) {
                q = i;
                a[q] = a[i];
            }
        }

        System.out.println("\n最小的积分是：" + a[q] + ",位置在：" + q);

    }
}
