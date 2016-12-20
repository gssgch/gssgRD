package main.java.com.ch.algorithm;

/**
 * Created by ch on 2016/12/20.
 * sort Demo
 */
public class SortDemo {

    public static void main(String[] args) {
        int[] arr = new int[]{11, 55, 33, 10, 177, 111, 215, 45};
        System.out.println("before sort-----:");
        printValue(arr);
        System.out.println("\n");


//        BubbleSort0(arr, arr.length);
//        BubbleSort1(arr, arr.length);
//        BubbleSort2(arr, arr.length);
        BubbleSort3(arr, arr.length);

        System.out.println("\nafter sort-----:");
        printValue(arr);
    }

    private static void printValue(int[] arr) {
        for (int n : arr) {
            System.out.print(n + "\t");
        }
    }

    /**
     * BubbleSort 冒泡排序
     * 循环遍历，每一次比较相邻两位
     *
     * @param arr
     */
    private static void BubbleSort0(int[] arr, int len) {
        for (int i = 0; i < len - 1; i++) { // 轮次
            for (int j = 0; j < len - 1 - i; j++) { // swap
                if (arr[j] > arr[j + 1]) {
                    swap(arr, j, j + 1);
                }
                System.out.print("第" + (i + 1) + "次排序,第" + (j + 1) + "次交换：");
                printValue(arr);
                System.out.println();
            }
            /*System.out.print("第"+(i+1)+"次排序：");
            printValue(arr);
            System.out.println();*/
        }
    }

    // 加数据交换判断标志
    private static void BubbleSort1(int[] arr, int len) {
        for (int i = 0; i < len - 1; i++) { // 轮次
            boolean swap = false;
            for (int j = 0; j < len - 1 - i; j++) { // swap
                if (arr[j] > arr[j + 1]) {
                    swap(arr, j, j + 1);
                    swap = true;
                }
            }
            // 一轮下来木有交换，则已经排好序了
            if (!swap) {
                break;
            }
        }
    }

    // 记录数据最后交换位置
    private static void BubbleSort2(int[] arr, int len) {
        int i = len - 1; // begin 最后位置不变
        while (i > 0) {
            int pos = 0;  // 每趟开始时，无数据交换
            for (int j = 0; j < i; j++) {
                if (arr[j] > arr[j + 1]) {
                    pos = j;
                    swap(arr, j, j + 1);
                }
            }
            i = pos;
        }
    }

    // 双向冒泡
    private static void BubbleSort3(int[] arr, int len) {
        int start = 0;
        int end = len - 1;
        while (start < end) {
            for (int j = start; j < end; j++) { // 正向冒泡，找max
                if (arr[j] > arr[j + 1]) {
                    swap(arr, j, j + 1);
                }
            }
            --end;
            for (int j = end; j > start; j--) {
                if (arr[j] < arr[j - 1]) {
                    swap(arr, j, j - 1);
                }
            }
            ++start;
        }
    }

    private static void BubbleSortxxx(int[] arr) {
        for (int i = 0; i < arr.length; i++) {
            for (int j = i + 1; j < arr.length; j++) {
                // 交换
                if (arr[i] > arr[j]) {
                    swap(arr, i, j);
                }
                System.out.println("i=" + i + ",j=" + j);
                printValue(arr);
            }
        }
    }

    private static void swap(int[] arr, int i, int j) {
        arr[i] = arr[i] ^ arr[j];
        arr[j] = arr[i] ^ arr[j];
        arr[i] = arr[i] ^ arr[j];
    }

}
