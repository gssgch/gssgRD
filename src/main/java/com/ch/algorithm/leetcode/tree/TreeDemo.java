package main.java.com.ch.algorithm.leetcode.tree;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/** 二叉树的练习
 * Created by ch on 2017/3/30.
 * Email: 824203453@qq.com
 */
public class TreeDemo {
    public static void main(String[] args) {
        TreeNode root = createTree();
        printlnList(inorderTraversal(root));
//        printlnList(inorderTraversal2(root));

    }

    public static List<TreeNode> generateTrees(int n){
        return genTrees(1,n);
    }
    /**
     * leetcode大神思路
     * 我首先注意到1..n是节点1到n的任何BST的按顺序遍历。
     * 所以如果我选择第i个节点作为我的根，左子树将包含元素1到（i-1），右子树将包含元素（i + 1）到n。
     * 我使用递归调用来获取左右子树的所有可能的树，并以所有可能的方式将其与根结合起来。
     * */
    public static List<TreeNode> genTrees(int start, int end){
        List<TreeNode> list = new ArrayList<TreeNode>();
        if(start>end){
//            list.add(null);
            return list;
        }
        if(start==end){
            list.add(new TreeNode(start));
            return list;
        }
        List<TreeNode> left,right;
        for (int i = start; i <=end ; i++) {
            left = genTrees(start,i-1);
            right = genTrees(i+1,end);

            for (TreeNode lnode:left) {
                for (TreeNode rnode:right) {
                    TreeNode root = new TreeNode(i);
                    root.left = lnode;
                    root.right = rnode;
                    list.add(root);
                }
            }
        }
        return list;
    }


    /** 	Binary Tree Inorder Traversal
     * 二叉树中序遍历 非递归
     * 从根到最左节点,弹出并add到list中 无右子树，tree为null;stack不为null
     * 弹出父节点，到右子树，如果有，就压栈，到左子树
     * 继续遍历下去
     */
    public static List<Integer> inorderTraversal2(TreeNode root) {
        List<Integer> list = new ArrayList();
        Stack<TreeNode> stack = new Stack();
        TreeNode tree = root;
        while (tree != null && !stack.empty()) {
            while (tree != null) {
                stack.push(tree);
                tree = tree.left;
            }
            tree = stack.pop();
            list.add(tree.val);
            tree = tree.right;
        }
        return list;
    }


    /**
     * 二叉树中序遍历 递归
     */
    public static List<Integer> inorderTraversal(TreeNode root) {
        List<Integer> list = new LinkedList();
        inOrder(root, list);
        return list;
    }

    public static void inOrder(TreeNode root, List list) {
        if (root != null) {
            inOrder(root.left, list);
            list.add(root.val);
            inOrder(root.right, list);
        }
    }

    /**打印list*/
    private static void printlnList(List<Integer> list) {
        for (int i : list) {
            System.out.print(i + " ");
        }
    }
    /**
     * 创建测试二叉树
     */
    public static TreeNode createTree() {
        TreeNode root = new TreeNode(6);
        TreeNode n1 = new TreeNode(1);
        TreeNode n2 = new TreeNode(2);
        TreeNode n3 = new TreeNode(3);
        TreeNode n4 = new TreeNode(4);
//            TreeNode n5 = new TreeNode(8);
        root.left = n2;
        n2.left = n3;
        n2.right = n1;
        root.right = n4;
        return root;
    }
}
