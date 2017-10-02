package main.java.com.ch.algorithm;

/**
 * Created by ch on 2017/3/14.
 */
public class Test {
    public static void main(String[] args) {
        int arrs[] = {1, 31, 5, 2, 10, 4, 6, 3, 9};
        printValue(arrs);
        /*for (int i = 0; i < arrs.length; i++) {
            for (int j = 0; j < arrs.length-1-i; j++) {
                if (arrs[j] < arrs[j + 1]) {
                    swap(arrs, j, j + 1);
                }
            }
        }*/

       /* for (int i = 0; i < arrs.length ; i++) {
            int min = i;
            for (int j = i + 1; j < arrs.length; j++) {
                if (arrs[j] < arrs[min]) {
                    min = j; // 记下第i小的数，比较后，得到最小
                }
            }
            if (min != i) { // 如果第i小的数不在i位置上，交换
                swap(arrs, min, i);
            }
        }*/


        quick(arrs, 0, arrs.length - 1);

        printValue(arrs);
    }

    public static void quick(int[] arrs, int start, int len) {
        int s = start;
        int end = len;
        int key = arrs[s];
        while (s < end) {
            while (s < end && arrs[end] >= key) {
                end--;
            }
            if (s < end) {
                swap(arrs, s, end);
            }
            while (s < end && arrs[s] <= key) {
                s++;
            }
            if (s < end) {
                swap(arrs, s, end);
            }
        }
        if (s - start > 1) {
            quick(arrs, start, s - 1);
        }
        if (len - end > 1) {
            quick(arrs, end+1, len);
        }
    }

    public static void swap(int[] arr, int i, int j) {
        arr[i] = arr[i] ^ arr[j];
        arr[j] = arr[i] ^ arr[j];
        arr[i] = arr[i] ^ arr[j];
    }

    private static void printValue(int[] arr) {
        for (int n : arr) {
            System.out.print(n + "\t");
        }
        System.out.println();
    }
}
