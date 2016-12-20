package main.java.com.ch.algorithm.old;

import java.util.Arrays;

//使用多种方式排序
public class Demo {
    public static void main(String[] args) {
        int[] array = new int[] { 4, 8, 6, 0, 1, 9, 5, 2, 3, 7 };
        int[] array2 = new int[] { 4, 8, 6, 0, 1, 9, 5, 2, 3, 7 };
        int[] array3 = new int[] { 4, 8, 6, 0, 1, 9, 5, 2, 3, 7 };
        System.out.println("排序前的數據:");
        for (int m : array) {
            System.out.print(m + "  ");
        }
        System.out.println("\n---------------");

        int temp = 0;
        for (int i = 0; i < (array.length - 1); i++) {
            for (int j = 0; j < (array.length - 1 - i); j++) {
                if (array[j] > array[j + 1]) {
                    temp = array[j];
                    array[j] = array[j + 1];
                    array[j + 1] = temp;
                }
            }
        }
        System.out.println("冒泡排序后的顺序:");
        for (int m : array) {
            System.out.print(m + "  ");
        }
        System.out.println("\n-----------------------");
        // 选择排序法
        int mIndex;
        for (int i = 0; i < (array2.length - 1); i++) {
            mIndex = i;
            for (int j = i + 1; j < array2.length; j++) {
                if (array2[j] < array2[mIndex]) {
                    mIndex = j;
                }
            }
            if (mIndex != i) {
                temp = array2[i];
                array2[i] = array2[mIndex];
                array2[mIndex] = temp;

            }
        }
        System.out.println("选择排序后的顺序:");
        for (int i = 0; i < array2.length; i++) {
            System.out.print(array2[i] + "  ");
        }
        System.out.println("\n-------------------");

        // Array.sort()方法
        Arrays.sort(array3);
        System.out.println("Arrays排序后的数据:");
        for (int i : array3) {
            System.out.print(i + "  ");
        }
        System.out.println("\n-------------------------");

    }

}
