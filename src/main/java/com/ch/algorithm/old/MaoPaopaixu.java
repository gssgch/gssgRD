package main.java.com.ch.algorithm.old;

//冒泡排序法
public class MaoPaopaixu {

    public static void main(String[] args) {
        int[] array = new int[] { 12, 5, 23, 87, 45, 9, 0 };
        System.out.println("排序前遍历结果：");
        for (int n : array) {
            System.out.print(n + "\t");
        }
        System.out.println();
        //冒泡排序的主要逻辑
        for (int i = 0; i < (array.length - 1); i++) {
            for (int j = 0; j < (array.length - 1 - i); j++) {
                int temp = 0;
                if (array[j] > array[j + 1]) {
                    temp = array[j];
                    array[j] = array[j + 1];
                    array[j + 1] = temp;
                }
            }
        }
        System.out.println("冒泡排序后的遍历结果是：");
        for (int i : array) {
            System.out.print(i + "\t");
        }
        System.out.println();
    }
}
