package main.java.com.ch.algorithm.old;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListPaixu {

    public static void main(String[] args) {
        List<Integer> list = new ArrayList<Integer>();

        long time = System.currentTimeMillis();
        for (int i = 0; i < 30; i++) {
            Integer a = (int) ((Math.random() * 100) + 1);
            list.add(a);

        }
        Collections.sort(list);//对整数list的遍历

        //		for(int i=0;i<list.size()-1;i++){
        //			for(int j=0;j<list.size()-1-i;j++){
        //				if(list.get(j)>list.get(j+1)){
        //				}
        //			}
        //		}
        for (int a : list) {
            System.out.print(a + " ");
        }
        System.out.println();
        long endTime = System.currentTimeMillis();
        System.out.println("共用时：" + (endTime - time) + "毫秒");

    }
}
