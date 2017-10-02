package main.java.com.ch.algorithm.hash;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ch on 2017/3/24.
 * Email: 824203453@qq.com
 */
public class HashMapTest {
    public static void main(String[] args) {
        Map<Integer,String> map = new HashMap();
        map.put(1,"A");
        map.put(2,"B");
        map.put(3,"C");
        map.put(4,"a");
        map.put(5,"aab");
        for(String v :map.values()){
            System.out.println(v);
        }
    }
}
