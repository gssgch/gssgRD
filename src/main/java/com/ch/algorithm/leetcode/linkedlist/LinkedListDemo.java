package main.java.com.ch.algorithm.leetcode.linkedlist;

import java.util.*;
import java.util.List;

/**
 * Created by ch on 2017/3/10.
 * leetcode
 */
public class LinkedListDemo {

    public static void main(String[] args) {
        int[] arr = {3, 2, 4};
//        System.out.println(twoSum(arr, 6)[0]);
//        System.out.println(twoSum2(arr, 6)[0]);


        String[] arrs = {"Hello", "Alaska", "Dad", "Peace"};
//        System.out.println(findWords(arrs).length);
//        System.out.println(findWords2(arrs).length);


//        testAddTwoNumbers();
        ListNode l1 = createList(7);
//        printlnNode(l1);
//        deleteNode(l1);
//        printlnNode(l1);

//        printlnNode(reverseList(l1));

//        printlnNode(createListNodeTail(4));

//        printlnNode(l1);

//        printlnNode(reverseList2(l1));
//        printlnNode(reverseList3(l1));
//        printlnNode(reverseList4(l1));

//        printlnNode(oddEvenList(l1));
//        printlnNode(oddEvenList1(l1));
//        printlnNode(oddEvenList2(l1));

        ListNode l2 = createSortedList(new int[]{1, 1, 2, 4, 5, 5, 6, 7});
//        printlnNode(l2);
//        printlnNode(deleteDuplicates(l2));
//        printlnNode(deleteDuplicates2(l2));
//        printlnNode(deleteDuplicates3(l2));

//        ListNode l3 = createSortedList(new int[]{2, 3, 5, 6, 7});
//        printlnNode(mergeTwoLists(l2, l3));
//        printlnNode(mergeTwoLists2(l2, l3));

//        printlnNode(removeElements(l2,5));
//        printlnNode(removeElements2(l2,4));
//        printlnNode(removeElements3(l2,6));


//        System.out.println(hasCycle(createCycleList(new int[]{1, 4, 5, 6, 7})));
//        System.out.println(hasCycle2(createCycleList(new int[]{1, 4, 5, 6, 7})));
//        System.out.println(cycleLen(createCycleList(new int[]{1, 4, 5})));

//        printlnNode(detectCycle(createCycleList(new int[]{1, 4, 5, 6, 7})), 10);

        ListNode l3 = createSortedList(new int[]{1, 2, 3, 4,3, 2, 1});
//        printlnNode(l3);
//        System.out.println(isPalindrome(l3));

        ListNode l12 = createSortedList(new int []{9,8});
        ListNode l13 =  createSortedList(new int []{1});
        printlnNode(l12);
        printlnNode(l13);
        printlnNode(addTwoNumbers2(l12,l13));;

    }
    public static ListNode addTwoNumbers2(ListNode l1, ListNode l2) {
        if(l1==null){
            return l2;
        }
        if(l2==null) return l1;
        int sum = 0;
        int carry =0;
        ListNode head =null;
        ListNode tail =null;
        while(l1!=null&&l2!=null){
            sum = l1.val+l2.val;
            ListNode n = new ListNode(sum%10+carry);
            carry = sum / 10;
            if(head ==null){
                head = n;
            }else{
                tail.next =n;
            }
            tail =n;
            l1 = l1.next;
            l2 = l2.next;
            if((l1 ==null || l2 ==null) && carry!=0){
                ListNode n2 = new ListNode(carry);
                tail.next = n2;
                tail =n2;
            }
        }
        while(l1!=null){
            tail.next = l1;
            tail = l1;
            l1 = l1.next;
        }
        while(l2!=null){
            tail.next = l2;
            tail = l2;
            l2 = l2.next;
        }
        if(tail!=null){
            tail.next =null;
        }
        return head;
    }
    /**
     * 回文链表
     */
    public static boolean isPalindrome(ListNode head) {
        if (head == null || head.next == null) {
            return true;
        }
        ListNode slow = head, fast = head;
        while (fast != null && fast.next != null) {
            slow = slow.next;
            fast = fast.next.next;
        }
        if (fast != null) { // 链表长度是奇数 fast步长是2，
            slow = slow.next;
        }
        slow = reverseList4(slow);
//        fast = head;
        while (slow != null) {
            if (head.val != slow.val) {
//            if (fast.val != slow.val) {
                return false;
            }
            slow = slow.next;
            head = head.next;
//            fast = fast.next;
        }
        return true;
    }


    /**
     * 判断链表是否有环，如果有，返回环的入口位置
     */
    public static ListNode detectCycle(ListNode head) {
        if (head == null) {
            return head;
        }
        boolean isCycle = false;
        ListNode slow = head;
        ListNode fast = head;
        while (fast.next != null && fast.next.next != null) {
            slow = slow.next;
            fast = fast.next.next;
            if (slow == fast) { // 相遇了，有环
                isCycle = true;
                break;
            }
        }
        if (!isCycle) {
            return null;
        }
        // 或者用这个判断
       /* if(fast==null|| fast.next==null){
            return null;
        }*/
        slow = head;
        while (slow != fast) {
            slow = slow.next;
            fast = fast.next;
        }
        System.out.println(fast.val);
        return fast;
    }

    /**
     * 判断链表是否有环  因为fast走的快，所以不需要判断slow了
     */
    public static boolean hasCycle2(ListNode head) {
        if (head == null) {
            return false;
        }
        ListNode slow = head;
        ListNode fast = head;
        while (fast.next != null && fast.next.next != null) {
            slow = slow.next;
            fast = fast.next.next;
            if (slow == fast) { // 相遇了，有环
                return true;
            }
            printlnNode(head, 7);
        }
        return false;
    }

    /**
     * 判断链表是否有环
     */
    public static boolean hasCycle(ListNode head) {
        ListNode slow = head;
        ListNode fast = head;
        while (true) {
            if (slow.next != null && fast.next != null && fast.next.next != null) {
                slow = slow.next;
                fast = fast.next.next;
            } else { // 有null，则没环
                return false;
            }
            if (slow == fast) { // 相遇了，有环
                return true;
            }
            printlnNode(head, 7);
        }
    }

    /**
     * 获取有环链表的环长度  相遇时的步长就是链表长度
     */
    public static int cycleLen(ListNode head) {
        int len = 0;
        ListNode slow = head;
        ListNode fast = head;
        for (; ; ) {
//        while (true) {
            slow = slow.next;
            fast = fast.next.next;
            len++;
            if (slow == fast) {
                break;
            }
        }
        return len;
    }

    /**
     * 删除链表中的节点   递归
     */
    public static ListNode removeElements3(ListNode head, int val) {
        if (head == null) {
            return head;
        }
        head.next = removeElements3(head.next, val);
        return head.val == val ? head.next : head;
    }


    /**
     * Remove Linked List Elements
     */
    public static ListNode removeElements2(ListNode head, int val) {
        if (head == null) {
            return head;
        }
        ListNode p = head;
        while (p.next != null) { // 不是最后一个节点
            if (p.next.val == val) { // 相等，链接到下下一个
                p.next = p.next.next;
            } else { // 不相等，往后跳一步
                p = p.next;
            }
        }
        return head.val == val ? head.next : head; // 判断头结点
    }

    /**
     * 删除链表中节点值为给定值的结点
     * 使用stack+头插法+判断  最low的解法
     */
    public static ListNode removeElements(ListNode head, int val) {
        Stack<Integer> s = new Stack<Integer>();
        while (head != null) {
            s.push(head.val);
            head = head.next;
        }
        ListNode node = null;
        while (!s.empty()) {
            int v1 = s.pop();
            if (v1 != val) {
                ListNode n = new ListNode(v1);
                if (node == null) {
                    node = n;
                } else {
                    n.next = node;
                    node = n;
                }

            }
        }
        return node;
    }

    public static ListNode swapPairs2(ListNode head) {
        if ((head == null) || (head.next == null))
            return head;
        ListNode n = head.next;
        head.next = swapPairs2(head.next.next);
        n.next = head;
        return n;
    }

    public ListNode swapPairs(ListNode head) {
        ListNode dummy = new ListNode(0);
        dummy.next = head;
        ListNode current = dummy;
        while (current.next != null && current.next.next != null) {
            ListNode first = current.next;
            ListNode second = current.next.next;
            first.next = second.next;
            current.next = second;
            current.next.next = first;
            current = current.next.next;
        }
        return dummy.next;
    }

    public static ListNode swapPairs3(ListNode head) {
        if (head == null || head.next == null) return head;
        ListNode second = head.next;
        ListNode third = second.next;

        second.next = head;
        head.next = swapPairs3(third);

        return second;
    }

    /**
     * 合并两个有序列表
     */
    public static ListNode mergeTwoLists(ListNode l1, ListNode l2) {
        if (l1 == null) return l2;
        if (l2 == null) return l1;
        if (l1.val < l2.val) {
            l1.next = mergeTwoLists(l1.next, l2);
            return l1;
        } else {
            l2.next = mergeTwoLists(l1, l2.next);
            return l2;
        }
    }

    /**
     * 合并两个有序列表
     */
    public static ListNode mergeTwoLists2(ListNode l1, ListNode l2) {
        if (l1 == null) {
            return l2;
        }
        if (l2 == null) {
            return l1;
        }
        ListNode mergeHead;
        if (l1.val < l2.val) {
            mergeHead = l1;
            mergeHead.next = mergeTwoLists2(l1.next, l2);
        } else {
            mergeHead = l2;
            mergeHead.next = mergeTwoLists2(l1, l2.next);
        }
        return mergeHead;
    }


    /**
     * 删除有序列表的重复元素  递归  没完全看明白
     */
    public static ListNode deleteDuplicates3(ListNode head) {
        if (head == null || head.next == null) return head;
        head.next = deleteDuplicates(head.next);
        return head.val == head.next.val ? head.next : head;
    }

    /**
     * 删除有序列表的重复元素
     */
    public static ListNode deleteDuplicates2(ListNode head) {
        if (head == null || head.next == null) {
            return head;
        }
        ListNode dup = head;
        while (dup.next != null) {
            if (dup.next.val == dup.val) {
                dup.next = dup.next.next;
            } else {
                dup = dup.next;
            }
        }
        return head;
    }

    /**
     * 删除有序列表的重复元素  利用set  需要用尾插法，才能保证顺序
     * 如果列表无序，该方法不适用
     */
    public static ListNode deleteDuplicates(ListNode head) {
        Set<Integer> set = new TreeSet<Integer>();
        while (head != null) {
            set.add(head.val);
            head = head.next;
        }
        ListNode h2 = null;
        ListNode tail = null;
        for (Integer i : set) {
            ListNode n = new ListNode(i);
            if (h2 == null) {
                h2 = n;
            } else {
                tail.next = n;
            }
            tail = n;
        }
        if (tail == null) {
            tail.next = null;
        }
        return h2;
    }

    public static ListNode oddEvenList2(ListNode head) {
        if (head != null) {
            ListNode odd = head, even = head.next, evenHead = even;
            printlnNode("odd=", odd);
            printlnNode("even=", even);
            printlnNode("evenhead=", evenHead);
            while (even != null && even.next != null) {
                odd.next = odd.next.next; // 去掉了第一个偶数位
                printlnNode("odd=", odd);
                printlnNode("head=", head);
                even.next = even.next.next;
                printlnNode("even=", even);
                printlnNode("evenhead=", evenHead);
                printlnNode("head=", head);
                odd = odd.next;
                printlnNode("odd=", odd);
                printlnNode("head=", head);
                even = even.next;
                printlnNode("even=", even);
                printlnNode("evenhead=", evenHead);
                printlnNode("head=", head);
            }
            printlnNode("odd=", odd);
            printlnNode("evenhead=", evenHead);
            printlnNode("head=", head);
            odd.next = evenHead;
            printlnNode("odd=", odd);
            printlnNode("evenhead=", evenHead);
            printlnNode("head=", head);
        }
        return head;
    }

    public static ListNode oddEvenList1(ListNode head) {
        if (head == null || head.next == null)
            return head;
        ListNode odd = head;
        ListNode even = head.next;
        ListNode evenHead = even;
        while (odd.next != null && even.next != null) {
            odd.next = even.next;
            odd = odd.next;
            even.next = odd.next;
            even = even.next;
        }
        odd.next = evenHead;
        return head;
    }

    /*** 奇数位链表和偶数位链表
     * Given 1->2->3->4->5->NULL,
     return 1->3->5->2->4->NULL.
     不是值，而是位置
     */
    public static ListNode oddEvenList(ListNode head) {
        if (head == null || head.next == null) {
            return head;
        }
        Stack<Integer> odd = new Stack<Integer>();
        Stack<Integer> even = new Stack<Integer>();

        while (head != null) {
            odd.push(head.val);
            if (head.next != null) { // 例如3个节点，不判断就报错了
                head = head.next;
                even.push(head.val);
            }
            head = head.next;
        }
        ListNode node = new ListNode(even.pop());
        while (!even.empty()) {
            ListNode n = new ListNode(even.pop());
            n.next = node;
            node = n;
        }
        while (!odd.empty()) {
            ListNode n = new ListNode(odd.pop());
            n.next = node;
            node = n;
        }
        return node;
    }

    /**
     * 翻转链表 就地迭代法  没怎么看明白
     */
    public static ListNode reverseList4(ListNode head) {
    /* iterative solution */
        ListNode newHead = null;
        while (head != null) {
            ListNode next = head.next;
            head.next = newHead;
            newHead = head;
            head = next;
        }
        return newHead;
    }

    /**
     * 翻转链表3 递归 没怎么看明白
     */
    public static ListNode reverseList3(ListNode head) {
        if (head == null || head.next == null)
            return head;
        ListNode nextNode = head.next;
        ListNode newHead = reverseList3(nextNode);
        nextNode.next = head;
        head.next = null;
        return newHead;
    }

    /**
     * 翻转链表  利用头插法
     */
    public static ListNode reverseList2(ListNode node) {
        ListNode head = null;
        while (node != null) {
            ListNode n = new ListNode(node.val);
            if (head == null) {
                head = n;
            } else {
                n.next = head;
                head = n;
            }
            node = node.next;
        }
        return head;
    }

    /**
     * 最low的翻转，借助stack
     */
    public static ListNode reverseList(ListNode head) {
        Stack<Integer> s = new Stack<Integer>();
        while (head != null) {
            s.push(head.val);
            head = head.next;
        }
        if (s.empty()) {
            return head;
        }
        ListNode tail = null;
        ListNode h = null;
        while (!s.empty()) {
            ListNode n = new ListNode(s.pop());
            if (h == null) {
                h = n;
            } else {
                tail.next = n;
            }
            tail = n;
        }
        if (tail != null) {
            tail.next = null;
        }
        return h;
    }

    /**
     * 删除节点   删的永远是第一个
     */
    public static void deleteNode(ListNode node) {
        node.val = node.next.val;
        node.next = node.next.next;
    }

    public static ListNode createList(int len) {
        return createListHead(len, false);
    }

    public static ListNode createSortedList(int len) {
        return createListHead(len, true);
    }

    /**
     * 创建链表，指定链表长度和tail值 头插法
     */
    private static ListNode createListHead(int len, boolean isSorted) {
        ListNode l1 = null;
        for (int i = 0; i < len; i++) {
            int val = (int) Math.round(Math.random() * 10);
            if(val>=10){
                val = 9;
            }
            if (isSorted) {
                val = i;
            }
            ListNode head = new ListNode(val);
            if (l1 == null) {
                l1 = head;
            } else {
                head.next = l1;
                l1 = head;
            }

            // 如果用下面这种，l1的第一个节点是头结点，用上面这种则是尾结点
//            head.next = l1.next;
//            l1.next=head;
        }
        return l1;
    }

    /**
     * 头插法创建链表
     */
    private static ListNode createListHead2(int len) {
        ListNode l1 = new ListNode(0);
        for (int i = 1; i < len; i++) {
            ListNode head = new ListNode(i);
            head.next = l1;
            l1 = head;

            // 如果用下面这种，l1的第一个节点是头结点，用上面这种则是尾结点
//            head.next = l1.next;
//            l1.next=head;
        }
        return l1;
    }

    /**
     * 创建有环的链表  利用尾插法
     */
    public static ListNode createCycleList(int[] a) {
        return createListTail(a, 1);
    }

    /**
     * 创建有序列表  利用尾插法
     */
    public static ListNode createSortedList(int[] a) {
        return createListTail(a, 0);
    }

    /**
     * 尾插法创建链表 头尾 指针都先置为null
     */
    private static ListNode createListTail(int[] a, int type) {
        ListNode head = null;
        ListNode tail = null; // 尾
        for (int i = 0; i < a.length; i++) {
            ListNode n = new ListNode(a[i]);
            if (head == null) {
                head = n;// 新结点定义为头
            } else {
                tail.next = n;// 尾指针指向n，表尾终端结点的指针指向新结点
            }
            tail = n;// 指针移到n，即当前新结点定义为表尾终端结点
        }
        if (tail != null) {
            if (type == 0) {
                tail.next = null;// 尾指针指向为null
            } else // 创建有环链表，尾指针指向头
//                tail.next = head;
                // 测试，环链表 第三位开始形成环
                tail.next = head.next.next;
        }
        return head;
    }

    // 尾插法 先给定一个结点  该方法没看懂
    public static ListNode createListNodeTail2() {
        ListNode head = new ListNode(9);
        for (int i = 0; i < 4; i++) {
            ListNode n = new ListNode(i);
            //尾插法先要确定头结点是否为空，空的话先将第一个结点给头结点
            ListNode p = head;
            if (head == null) {
                n.next = head;
                head = n;
            } else {
                while (p.next != null) {
                    p = p.next;//p结点始终指向最后一个结点
                }
                n.next = p.next;//在尾部插入新结点
                p.next = n;
            }
        }
        return head;
    }

    /**
     * 两个链表相加
     */
    public static void testAddTwoNumbers() {
        ListNode l1 = createList(4);
        ListNode l2 = createList(3);

        printlnNode(l1);
        printlnNode(l2);
        printlnNode(addTwoNumbers(l1, l2));
    }

    public static void printlnNode(ListNode list) {
        printlnNode("", list, Integer.MAX_VALUE);
    }

    public static void printlnNode(ListNode list, int len) {
        printlnNode("", list, len);
    }

    /**
     * 打印链表内容  非循环链表 如果是循环链表，需要循环读并判断值
     */
    public static void printlnNode(String pre, ListNode list) {
        printlnNode(pre, list, Integer.MAX_VALUE);
    }

    /**
     * 有环的链表，只能打印指定长度
     */
    public static void printlnNode(String pre, ListNode list, int len) {
        String str = "";
        while (list != null && len >= 0) {
            len--;
            str += list.val;
            list = list.next;
            if (list != null) {
                str += "->";
            }
        }
        if (pre.length() > 0) {
            pre = pre + ":";
        }
        System.out.println(pre + str);

    }

    /** 两个链表相加 */
    public static ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        Stack<Integer> s1 = new Stack<Integer>();
        Stack<Integer> s2 = new Stack<Integer>();

        while (l1 != null) {
            s1.push(l1.val);
            l1 = l1.next;
        }
        while (l2 != null) {
            s2.push(l2.val);
            l2 = l2.next;
        }

        int sum = 0;
        ListNode list = new ListNode(0);
        while (!s1.empty() || !s2.empty()) {
            if (!s1.empty()) sum += s1.pop();
            if (!s2.empty()) sum += s2.pop();
            list.val = sum % 10;
            ListNode head = new ListNode(sum / 10);
            head.next = list;
            list = head;
            sum /= 10;

        }
        return list.val == 0 ? list.next : list;
    }

    // 找出键盘中，都在同一行的字符串
    public static String[] findWords(String[] words) {
        String s1 = "qwertyuiop";
        String s2 = "asdfghjkl";
        String s3 = "zxcvbnm";
        List<String> list = new ArrayList();
        for (String a : words) {
            String b = a.toLowerCase().replaceAll("(.)\\1+", "$1");
            Set<Integer> set = new HashSet();
            for (Character i : b.toCharArray()) {
                if (s1.contains(i.toString())) {
                    if (set.size() > 1) {
                        break;
                    }
                    set.add(0);
                }
                if (s2.contains(i.toString())) {
                    if (set.size() > 1) {
                        break;
                    }
                    set.add(1);
                }
                if (s3.contains(i.toString())) {
                    if (set.size() > 1) {
                        break;
                    }
                    set.add(2);
                }
            }
            if (set.size() == 1) {
                list.add(a);
            }
        }
        return list.toArray(new String[0]);
    }

    public static String[] findWords2(String[] words) {

        String[] keys = {"qwertyuiop", "asdfghjkl", "zxcvbnm"};
        Map<Character, Integer> map = new HashMap();
        for (int i = 0; i < keys.length; i++) {
            for (char c : keys[i].toCharArray()) {
                map.put(c, i);
            }
        }
        List<String> list = new ArrayList<String>();
        for (String word : words) {
            if (word.equals("")) continue;
            // 取第1个作为基准
            int index = map.get(word.toLowerCase().charAt(0));
            for (char c : word.toLowerCase().toCharArray()) {
                if (map.get(c) != index) {
                    index = -1;
                    break;
                }
            }
            if (index != -1) {
                list.add(word);
            }
        }
        return list.toArray(new String[0]);
    }

    // 大神一行代码解决问题 https://discuss.leetcode.com/topic/77754/java-1-line-solution-via-regex-and-stream
    public String[] findWords3(String[] words) {
        return new String[2];
//        return Stream.of(words).filter(s -> s.toLowerCase().matches("[qwertyuiop]*|[asdfghjkl]*|[zxcvbnm]*")).toArray(String[]::new);
    }

    // 双遍历 暴力求解  时间复杂度 O(n2) 空间 O(1)
    public static int[] twoSum(int[] nums, int target) {
        int[] a = new int[2];
        for (int i = 0; i < nums.length; i++) {
            for (int j = i + 1; j < nums.length; j++) {
                if (nums[i] + nums[j] == target) {
                    return new int[]{i, j};
                }
            }
        }
        throw new IllegalArgumentException("No two sum solution");
    }

    // 一遍hash 时间复杂度 O(n) 空间 O(n)
    public static int[] twoSum2(int[] nums, int target) {
        Map<Integer, Integer> map = new HashMap();
        for (int i = 0; i < nums.length; i++) {
            int num = target - nums[i];
            if (map.containsKey(num)) {
                return new int[]{map.get(num), i};
            }
            map.put(nums[i], i);
        }
        throw new IllegalArgumentException("No two sum solution");
    }
}
