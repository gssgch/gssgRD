package main.java.com.ch.algorithm.old;

import java.util.Arrays;

//颠倒已知数组的元素顺序
public class SwapDemo {

    public static void main(String[] args) {
        int[] a = new int[] { (int) (Math.random() * 1000),
                (int) (Math.random() * 1000), (int) (Math.random() * 1000),
                (int) (Math.random() * 1000), (int) (Math.random() * 1000),
                (int) (Math.random() * 1000) };
        System.out.println(a.length + "//" + a[0]);
        System.out.println(Arrays.toString(a));
        swap(a);
        System.out.println(Arrays.toString(a));
    }

    public static void swap(int a[]) {
        int len = a.length;
        //		for(int i=0,j=a.length-1;i<j;i++,j--){
        for (int i = 0; i < (len / 2); i++) { //for循环里面的两种写法
            int temp = a[i];
            a[i] = a[len - 1 - i];
            a[len - 1 - i] = temp;
        }
    }
}
