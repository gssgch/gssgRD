package main.java.com.ch.algorithm.old;

//选择排序法
public class SelectionSort {

    public static void main(String[] args) {
        int[] array = new int[] { 12, 5, 23, 87, 45, 9, 0 };
        SelectionSort sort = new SelectionSort();
        sort.chooseSort(array);
        //		method();
    }

    public static void method() {
        int[] array = new int[] { 12, 5, 23, 87, 45, 9, 0 };
        System.out.println("排序前的顺序：");
        for (int n : array) {
            System.out.print(n + "\t");
        }
        System.out.println();
        //选择排序法逻辑
        int mIndex;
        int temp = 0;
        for (int i = 0; i < array.length; i++) {
            mIndex = i;
            /*
             * 查找第i小的数，直到记下第i小数的位置*/
            for (int j = i + 1; j < array.length; j++) {
                if (array[j] < array[mIndex]) {
                    mIndex = j;//记下第i小的数的位置，再次比较，直到最小
                }
            }
            if (mIndex != i) {//如果第i小数的位置不在i,则交换位置，使第i小数在第i位
                temp = array[i];
                array[i] = array[mIndex];
                array[mIndex] = temp;
            }
        }
        System.out.println("排序后的结果：");
        for (int i = 0; i < array.length; i++) {
            System.out.print(array[i] + "\t");
        }
    }

    //两种写法都行
    private void chooseSort(int[] array) {
        long start = System.currentTimeMillis();
        for (int k = 0; k < 1000000; k++) {
            int temp;
            for (int i = 0; i < array.length; i++) {
                temp = array[i];
                //最小数下标
                int smallLocation = i;
                for (int j = i + 1; j < array.length; j++) {
                    if (array[j] < temp) {
                        //取出最小数
                        temp = array[j];
                        //最小数下标
                        smallLocation = j;
                    }
                }
                array[smallLocation] = array[i];
                array[i] = temp;
            }
        }
        System.out.println("选择排序如下，耗时：" + (System.currentTimeMillis() - start));
        for (int i = 0; i < array.length; i++) {
            System.out.print(array[i] + "\t");
        }
    }
}
