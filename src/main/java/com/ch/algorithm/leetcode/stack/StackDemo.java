package main.java.com.ch.algorithm.leetcode.stack;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * Created by ch on 2017/3/26.
 * Email: 824203453@qq.com
 */
public class StackDemo {
    public static void main(String[] args) {
        int[] findNums = {2, 4};
        int[] nums = {1, 2, 3, 4};
//        printArray(nextGreaterElement(findNums, nums));
//        printArray(nextGreaterElement2(findNums, nums));

        printArray(nextGreaterElements(nums));


    }

    /**
     * 下一个更大的数，假定数组循环
     * 同样利用栈，和一个返回的数组
     */
    public static int[] nextGreaterElements(int[] nums) {
        int len = nums.length;
        int next[] = new int[len];
        Arrays.fill(next,-1); // 赋初值
        Stack<Integer> stack = new Stack<Integer>();
        for (int i = 0; i < len * 2; i++) { // 2倍长度，相当于循环
            int num = nums[i % len];
            while (!stack.empty() && nums[stack.peek()] < num) {
                next[stack.pop()] = num;
            }
            if (i < len) stack.push(i);
        }
        return next;
    }

    /**
     * leetcode 大神思路
     * 建立每个数字和其右边第一个较大数之间的映射，没有的话就是-1。
     * 我们遍历原数组中的所有数字，如果此时栈不为空，且栈顶元素小于当前数字，
     * 说明当前数字就是栈顶元素的右边第一个较大数，那么建立二者的映射，
     * 并且去除当前栈顶元素，最后将当前遍历到的数字压入栈。
     * 当所有数字都建立了映射，那么最后我们可以直接通过哈希表快速的找到子集合中数字的右边较大值
     */
    public static int[] nextGreaterElement2(int[] findNums, int[] nums) {
        Map<Integer, Integer> map = new HashMap(); // map from x to next greater element of x
        Stack<Integer> stack = new Stack();
        for (int num : nums) {
            while (!stack.isEmpty() && stack.peek() < num)
                map.put(stack.pop(), num);
            stack.push(num);
        }
        for (int i = 0; i < findNums.length; i++)
            findNums[i] = map.get(findNums[i]) == null ? -1 : map.get(findNums[i]);
        return findNums;
    }

    /**
     * Next Greater Element I
     * Input: nums1 = [2,4], nums2 = [1,2,3,4].
     * Output: [3,-1]
     * 直接遍历循环
     */
    public static int[] nextGreaterElement(int[] findNums, int[] nums) {
        int[] res = new int[findNums.length];
        for (int i = 0; i < findNums.length; i++) {
            int j = 0, k;
            for (; j < nums.length; ++j) {
                if (nums[j] == findNums[i]) {
                    break;
                }
            }
            for (k = j + 1; k < nums.length; ++k) {
                if (nums[k] > nums[j]) {
                    res[i] = nums[k];
                    break;
                }
            }
            if (k == nums.length) {
                res[i] = -1;
            }
        }
        return res;
    }

    public static void printArray(int[] nums) {
        for (int i = 0; i < nums.length; i++) {
            System.out.print(nums[i] + "\t");
        }
    }
}
