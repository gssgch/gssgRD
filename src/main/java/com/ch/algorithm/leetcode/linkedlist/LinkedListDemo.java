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
        ListNode l1 = createList(6);
//        printlnNode(l1);
//        deleteNode(l1);
//        printlnNode(l1);
//        printlnNodeRec(l1);


//        printlnNode(reverseList(l1));

//        printlnNode(createListNodeTail(4));

        printlnNode(l1);

        printlnNode(reverseBetween(l1, 3, 5));
//        printlnNode(reverseList2(l1));
//        printlnNode(reverseList3(l1));
//        printlnNode(reverseList4(l1));
//        printlnNode(reverseList5(l1));

//        printlnNode(oddEvenList(l1));
//        printlnNode(oddEvenList1(l1));
//        printlnNode(oddEvenList2(l1));

        ListNode l2 = createSortedList(new int[]{1, 1, 2, 4});
//        printlnNode(l2);
//        printlnNode(deleteDuplicates(l2));
//        printlnNode(deleteDuplicates2(l2));
//        printlnNode(deleteDuplicates3(l2));

//        printlnNode(deleteDuplicatesII(l2));
//        printlnNode(deleteDuplicatesII2(l2));


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

        ListNode l3 = createSortedList(new int[]{1, 1, 2, 1});
//        printlnNode(l3);
//        System.out.println(isPalindrome(l3));

        ListNode l12 = createSortedList(new int[]{1, 2, 3, 4});
        ListNode l13 = createSortedList(new int[]{1, 2, 3, 4});

//        printlnNode(l12);
//        printlnNode(l13);
//        printlnNode(addTwoNumbers2(l12,l13));;

//        printlnNode(getIntersectionNode(l12,l13));
//        printlnNode(getIntersectionNode2(l12,l13));
//        printlnNode(getIntersectionNode3(l12,l13));

//        printlnNode(removeNthFromEnd(l12,2));
//        printlnNode(removeNthFromEnd2(l12,2));


        ListNode a = createSortedList(new int[]{1, 2, 4, 3, 5});
//        printlnNode(a);
//        printlnNode(partition(a, 3));

//        reorderList(a);
//        reorderList2(a);

//        printlnNode(rotateRight(a, 2));
//        printlnNode(rotateRight2(a, 2));

    }

    /**将升序链表转换为平衡二叉树*/
    public static TreeNode sortedListToBST(ListNode head) {
        return rec(head,null);
    }
    // 在区间[head, tail)里递归，后面的tail是包括在内的，这样可以避免要多用一个指针来记录mid前的节点
    public static TreeNode rec(ListNode head,ListNode tail){
        if(head==tail){
            return null;
        }
        // 一次遍历找到中点的方法：快慢指针
        ListNode fast =head; // 终点
        ListNode slow = head;// 中点
        while(fast!=tail&&fast.next!=tail){ // 注意停止条件
            fast = fast.next.next;
            slow = slow.next;
        }
        TreeNode root = new TreeNode(slow.val);
        root.left=rec(head,slow);
        root.right=rec(slow.next,tail);
        return root;

    }
    /**
     * 向右旋转链表，不借助多的指针
     */
    public static ListNode rotateRight2(ListNode head, int k) {
        if (head == null || head.next == null) return head;
        ListNode cpHead = head;
        int i = 0;
        while (cpHead.next != null) {
            cpHead = cpHead.next;
            i++;
        }
        cpHead.next = head;
        for (int j = i - k % i; j > 0; j--) {
            head = head.next;
        }
        cpHead = head.next; //
        head.next = null;
        return cpHead;
    }

    /**
     * 向右旋转
     * 1->2->3->4->5->NULL and k = 2,
     * return 4->5->1->2->3->NULL.
     */
    public static ListNode rotateRight(ListNode head, int k) {
        if (head == null || head.next == null) return head;
        ListNode node = new ListNode(0);
        node.next = head;
        ListNode slow = node, fast = node;
        int i = 0;
        // fast指向尾结点 算出head的长度
//        for (; fast.next != null; i++) {
//            fast = fast.next;
//        }
        while (fast.next != null) {
            fast = fast.next;
            i++;
        }
        // slow指向要旋转的位置，就是旋转后链表的尾结点
        for (int j = i - k % i; j > 0; j--) {
            slow = slow.next;
        }
        // 把链表后半段放到前面去
        fast.next = node.next;
        node.next = slow.next;
        slow.next = null;

        return node.next;
    }


    /**
     * 链表重排序
     * Given {1,2,3,4}, reorder it to {1,4,2,3}.
     */
    public static void reorderList(ListNode head) {
        if (head == null || head.next == null) {
            return;
        }
        ListNode slow = head, fast = head;

        // 下面这个判断和回文那个判断还不一样 这个正好fast走到终点 slow走到中点（奇数个）或差一步到中点（偶数个）
        while (fast.next != null && fast.next.next != null) {
            slow = slow.next;
            fast = fast.next.next;
        }
        printlnNode(head);
        ListNode head1 = head;
        ListNode head2 = slow.next;
        slow.next = null;
        head2 = reverseList4(head2);

        printlnNode("node1=", head1);
        printlnNode("node2=", head2);

        while (head2 != null && head1 != null) {
            ListNode next = head2.next;
            head2.next = head1.next;
            head1.next = head2;
            head1 = head2.next;
            head2 = next;
//            ListNode tmp1 = head1.next;
//            ListNode tmp2 = head2.next;
//            head1.next = head2;
//            head2.next = tmp1;
//            head1 = tmp1;
//            head2 = tmp2;
        }
        printlnNode(head);

    }

    /**
     * 对链表分区
     */
    public static ListNode partition(ListNode head, int x) {
        ListNode node1 = new ListNode(0), node2 = new ListNode(0);
        ListNode l1 = node1, l2 = node2;
        while (head != null) {
            if (head.val < x) {
                l1.next = head;
                l1 = head;
            } else {
                l2.next = head;
                l2 = head;
            }
            head = head.next;
        }
        l2.next = null;
        l1.next = node2.next;
        return node1.next;
    }

    /**
     * leetcode解法 没怎么看明白
     */
    public ListNode RemoveNthFromEnd3(ListNode head, int n) {
        ListNode h1 = head, h2 = head;
        while (n-- > 0) h2 = h2.next;
        if (h2 == null) return head.next;  // The head need to be removed, do it.
        h2 = h2.next;

        while (h2 != null) {
            h1 = h1.next;
            h2 = h2.next;
        }
        h1.next = h1.next.next;   // the one after the h1 need to be removed
        return head;
    }

    /**
     * 可以使用指针来完成单程解决方案。
     * 快速移动一个指针 向前移动n + 1个位置，以保持两个指针之间的间隙，然后以相同的速度移动两个指针。
     * 最后，当快速指针到达结束时，慢指针将在后面n + 1个位置 - 它正好可以跳过下一个节点。
     */
    public static ListNode removeNthFromEnd2(ListNode head, int n) {
        if (head == null) return head;
        ListNode start = new ListNode(0);
        ListNode fast = start, slow = start;
        slow.next = head;
        for (int i = 1; i <= n + 1; i++) {
            fast = fast.next;
            printlnNode("22fast=", fast);
        }

        while (fast != null) {
            slow = slow.next;
            fast = fast.next;
            printlnNode("33slow=", slow);
            printlnNode("33fast=", fast);
        }
        slow.next = slow.next.next;
        return start.next;
    }

    /**
     * 给定一个链表，从列表的末尾删除第n个节点并返回它的头。
     */
    public static ListNode removeNthFromEnd(ListNode head, int n) {
        if (head == null) return head;
        Stack<Integer> s = new Stack<Integer>();
        while (head != null) {
            s.push(head.val);
            head = head.next;
        }
        int i = 0;
        ListNode h = new ListNode(0);
        while (!s.empty()) {
            int val = s.pop();
            i++;
            if (i == n) {
                continue;
            } else {
                ListNode node = new ListNode(val);
                // 0这个特殊节点要放在头部
                node.next = h.next;
                h.next = node;
            }
        }
        return h.next;
    }


    /**
     * 单链表交集节点  leetcode大神
     * 我们可以使用两个迭代来做到这一点。
     * 在第一次迭代中，在到达尾节点之后，我们将把一个链表的指针重置为另一个链表的头。
     * 在第二次迭代中，我们将移动两个指针，直到它们指向同一个节点。
     * 我们在第一次迭代中的操作将帮助我们消除差异。
     * 因此，如果两个linkedlist相交，则第二次迭代中的会合点必须是交点。
     * 如果两个链表根本没有交集，则第二次迭代中的会议指针必须是两个列表的尾节点，它是空的
     * <p>
     * 本地测试一直不通过
     */
    public static ListNode getIntersectionNode2(ListNode headA, ListNode headB) {
        //boundary check
        if (headA == null || headB == null) return null;

        ListNode a = headA;
        ListNode b = headB;

        //if a & b have different len, then we will stop the loop after second iteration
        while (a != b) {
            //for the end of first iteration, we just reset the pointer to the head of another linkedlist
            a = a == null ? headB : a.next;
            b = b == null ? headA : b.next;
            // 这两行重点，第一次迭代结束，将指针重置为另一个链表的头
        }
        return a;
    }

    /**
     * leetcode上写法
     */
    public static ListNode getIntersectionNode3(ListNode headA, ListNode headB) {
        int lenA = calcLen(headA), lenB = calcLen(headB);
        // move headA and headB to the same start point
        while (lenA > lenB) {
            headA = headA.next;
            lenA--;
        }
        while (lenA < lenB) {
            headB = headB.next;
            lenB--;
        }
        // find the intersection until end
        // 本地要判断val，否则一直返回false，leetcode上可以用headA!=headB来提交，不能用.val来判断 ？？？
        while (headA.val != headB.val) {
            headA = headA.next;
            headB = headB.next;
        }
        return headA;
    }

    /**
     * 找到两个单链表 交集开始的节点
     */
    public static ListNode getIntersectionNode(ListNode headA, ListNode headB) {
        int len1 = calcLen(headA);
        int len2 = calcLen(headB);
        // head为长链表  tail为短链表
        ListNode head = len1 > len2 ? headA : headB;
        ListNode tail = len1 > len2 ? headB : headA;
        // 长链表先空转 长度差次
        for (int i = 0; i < Math.abs(len1 - len2); i++) {
            head = head.next;
        }
        // 同时走 判断是否相等 相等即有交点
        while (head != null && tail != null) {
            if (head.val == tail.val) {
                return head;
            }
            head = head.next;
            tail = tail.next;
        }
        return null;
    }

    public static int calcLen(ListNode head) {
        int len = 0;
        while (head != null) {
            len++;
            head = head.next;
        }
        return len;
    }


    /**
     * 链表相加 leetcode思路
     */
    public ListNode addTwoNumbers3(ListNode l1, ListNode l2) {
        ListNode c1 = l1;
        ListNode c2 = l2;
        ListNode sentinel = new ListNode(0);
        ListNode d = sentinel;
        int sum = 0;
        while (c1 != null || c2 != null) {
            sum /= 10;
            if (c1 != null) {
                sum += c1.val;
                c1 = c1.next;
            }
            if (c2 != null) {
                sum += c2.val;
                c2 = c2.next;
            }
            d.next = new ListNode(sum % 10);
            d = d.next;
        }
        if (sum / 10 == 1)
            d.next = new ListNode(1);
        return sentinel.next;
    }

    /**
     * 链表相加
     */
    public static ListNode addTwoNumbers2(ListNode l1, ListNode l2) {

        int carry = 0;
        ListNode head = null;
        ListNode tail = null;
        while (l1 != null || l2 != null) {
            int sum = 0;
            if (l1 != null) {
                sum += l1.val;
                l1 = l1.next;
            }
            if (l2 != null) {
                sum += l2.val;
                l2 = l2.next;
            }
//            sum = l1.val+l2.val;
            ListNode n = new ListNode((sum % 10 + carry) % 10);
            carry = (sum + carry) / 10;
            if (head == null) {
                head = n;
            } else {
                tail.next = n;
            }
            tail = n;

            if (l1 == null && l2 == null && carry != 0) {
                ListNode n2 = new ListNode(carry);
                tail.next = n2;
                tail = n2;
            }
        }

        if (tail != null) {
            tail.next = null;
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

    /**
     * 成对翻转链表  递归2
     */
    public static ListNode swapPairsRec2(ListNode head) {
        if (head == null || head.next == null) return head;
        ListNode second = head.next;
        ListNode third = second.next;
        second.next = head;
        head.next = swapPairsRec2(third);
        return second;
    }

    /**
     * 成对翻转链表  递归
     */
    public static ListNode swapPairsRec(ListNode head) {
        if ((head == null) || (head.next == null))
            return head;
        ListNode n = head.next;
        head.next = swapPairsRec(head.next.next);
        n.next = head;
        return n;
    }

    /**
     * 非递归版本  没看明白
     * 需要运用fakehead来指向原指针头，防止丢链，用两个指针，
     * ptr1始终指向需要交换的pair的前面一个node，ptr2始终指向需要交换的pair的第一个node。
     * 然后就是进行链表交换。
     * 需要用一个临时指针nextstart， 指向下一个需要交换的pair的第一个node，保证下一次交换的正确进行。
     * 然后就进行正常的链表交换，和指针挪动就好。
     */
    public ListNode swapPairs2(ListNode head) {
        if (head == null || head.next == null)
            return head;
        ListNode fakehead = new ListNode(-1);
        fakehead.next = head;
        ListNode ptr1 = fakehead;
        ListNode ptr2 = head;
        while (ptr2 != null && ptr2.next != null) {
            ListNode nextstart = ptr2.next.next;
            ptr2.next.next = ptr2;
            ptr1.next = ptr2.next;
            ptr2.next = nextstart;
            ptr1 = ptr2;
            ptr2 = ptr2.next;
        }
        return fakehead.next;
    }

    /**
     * 非递归版本  没看明白
     */
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
     * 删除所有重复元素 递归
     */
    public static ListNode deleteDuplicatesII2(ListNode head) {
        if (head == null || head.next == null) return head;
        if (head.next != null && head.val == head.next.val) {
            while (head.next != null && head.val == head.next.val) {
                head = head.next;
            }
            return deleteDuplicatesII2(head.next);
        } else {
            head.next = deleteDuplicatesII2(head.next);
        }
        return head;
    }

    /**
     * 删除所有的重复元素 1->1->2->3 => 2->3
     * 使用两个指针，pre跟踪节点之前的dup节点
     * cur找到dup的最后一个结点
     */
    public static ListNode deleteDuplicatesII(ListNode head) {
        if (head == null || head.next == null) return head;
        ListNode h = new ListNode(0);
        h.next = head;
        ListNode pre = h;
        ListNode cur = head;
        while (cur != null) {
            while (cur.next != null && cur.val == cur.next.val) {
                cur = cur.next; // while循环找到dup的最后一个节点
            }
            // 这里判断的是节点，不是某一节点的值，我是这么理解的
            if (pre.next == cur) { // 没有重复，向下移动指针
                pre = pre.next;
            } else { // 检测到重复 删除节点
                pre.next = cur.next;
            }
            cur = cur.next;
        }
        return h.next;
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
    public static ListNode reverseBetween2(ListNode head, int m, int n) {
        if(head == null) return null;
        ListNode dummy = new ListNode(0); // create a dummy node to mark the head of this list
        dummy.next = head;
        ListNode pre = dummy; // make a pointer pre as a marker for the node before reversing
        for(int i = 0; i<m-1; i++) pre = pre.next;

        ListNode start = pre.next; // a pointer to the beginning of a sub-list that will be reversed
        ListNode then = start.next; // a pointer to a node that will be reversed

        // 1 - 2 -3 - 4 - 5 ; m=2; n =4 ---> pre = 1, start = 2, then = 3
        // dummy-> 1 -> 2 -> 3 -> 4 -> 5

        for(int i=0; i<n-m; i++)
        {
            start.next = then.next;
            then.next = pre.next;
            pre.next = then;
            then = start.next;
        }

        // first reversing : dummy->1 - 3 - 2 - 4 - 5; pre = 1, start = 2, then = 4
        // second reversing: dummy->1 - 4 - 3 - 2 - 5; pre = 1, start = 2, then = 5 (finish)

        return dummy.next;

    }

    /**翻转链表 带起始条件的*/
    public static ListNode reverseBetween(ListNode head, int m, int n) {
        if (head == null || head.next == null) {
            return head;
        }
        ListNode dummy = new ListNode(0);
        dummy.next = head;
        ListNode pre = null;
        ListNode cur = dummy;
        int count = 0;
        while(cur.next!=null){
            pre = cur;
            cur=cur.next;
            count++;
            if(count==m){
                break;
            }
        }

        // 下面这个for循环就是就地翻转链表的逻辑
        ListNode post = cur.next;
        cur.next=null;
        for(int i=m;i<n;i++){
            ListNode tmp = post.next;
            post.next = cur;
            cur = post;
            post=tmp;
        }

        // cur 就是翻转后的
        // post就是最后剩下的
        // 这里注意顺序 这里的pre是翻转开始点和前一个
        pre.next.next=post;
        pre.next=cur;

        return dummy.next;
    }
    public ListNode reverseBetween3(ListNode head, int m, int n) {
        if (head == null || head.next == null) {
            return head;
        }

        if (m >= n) {
            return head;
        }

        ListNode dummy = new ListNode(0);
        dummy.next = head;

        ListNode pre = dummy;

        //1. get the pre node before m.
        for (int i = m; i > 1; i--) {
            pre = pre.next;
        }

        // record the tail of the reversed link.
        ListNode reverseTail = pre.next;
        pre.next = null;

        // reverse the link.
        ListNode cur = reverseTail;

        for (int i = n - m + 1; i > 0; i--) {
            if (i == 1) {
                // 这里是翻转段后的第一个元素 .
                reverseTail.next = cur.next;
            }

            ListNode tmp = cur.next;

            cur.next = pre.next;
            pre.next = cur;

            cur = tmp;
        }

        return dummy.next;
    }

    /**
     * 链表反转  利用三个指针  pre  q  next
     * 第一步 先把头结点分出来，然后把剩下的结点给q 进入循环
     * 循环中，q.next 给 next ，把 q指向pre pre和q结点都向后移动
     * 这里一定要理解，pre节点是反着来的，并不是常见的从前往后，应该是从后往前，
     * 把q.next指向pre，然后把q赋给pre（也就是pre节点前进一步），相当于 null->1->2这种顺序增加元素
     */
    public static ListNode reverseList5(ListNode head) {
        if (head == null) return head;
        ListNode pre = head, q = head.next;
        pre.next = null;
        while (q != null) {
            ListNode next = q.next;
            q.next = pre;
            pre = q;
            q = next;
        }
        return pre;
    }

    /**
     * 翻转链表 就地迭代法  没怎么看明白
     * <p>
     * 其实这个和reverseList5一样的，没有上个好理解，相当于把newHead当做pre，head当做q了
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
            if (val >= 10) {
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

    // 递归打印
    public static void printlnNodeRec(ListNode list) {
        if (list != null) {
            System.out.println(list.val + "->");
            ListNode node = list.next;
            printlnNodeRec(node);
        }
    }

    /**
     * 两个链表相加
     */
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
