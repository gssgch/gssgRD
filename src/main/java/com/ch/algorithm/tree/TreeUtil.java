package main.java.com.ch.algorithm.tree;

import java.util.Stack;

/**
 * Created by ch on 2017/3/9.
 * 参考 ：http://www.cnblogs.com/qinggege/p/5212215.html
 */

public class TreeUtil {

    /**
     * 前序递归
     */
    public static void preOrder(Tree tree) {
        if (tree != null) {
            System.out.print(tree.getTreeValue() + "\t");
            preOrder(tree.getLeftTree());
            preOrder(tree.getRightTree());
        }
    }

    /**
     * 前序非递归栈
     */
    public static void preOrderStack(Tree tree) {
        Stack<Tree> stack = new Stack<Tree>();
        if (tree != null) {
            stack.push(tree); // 压栈
            while (!stack.isEmpty()) {
                tree = stack.pop(); // 弹栈并赋值
                System.out.print(tree.getTreeValue() + "\t");
                // 前序，先压右，再压左   出栈顺序为左右
                if (tree.getRightTree() != null) {
                    stack.push(tree.getRightTree());
                }
                if (tree.getLeftTree() != null) {
                    stack.push(tree.getLeftTree());
                }
            }
        }

    }

    public static void inOrder(Tree tree) {
        if (tree != null) {
            preOrder(tree.getLeftTree());
            System.out.print(tree.getTreeValue() + "\t");
            preOrder(tree.getRightTree());
        }
    }

    public static void inOrderStack(Tree tree) {
        Stack<Tree> stack = new Stack<Tree>();
        while (tree != null) {
            while (tree != null) {
                if (tree.getRightTree() != null) {
                    stack.push(tree.getRightTree());
//                    System.out.println("push"+tree.getRightTree().getTreeValue());
                }
                stack.push(tree);
//                System.out.println("push"+tree.getTreeValue());
                tree = tree.getLeftTree();
            }
            // 外1轮
            // 内1轮  push c  push a  tree =b
            // 内2轮  push e  push b  tree =d
            // 内3轮  push d  // 退出内while循环
            // 外2轮 tree 为e
            // 内1轮  push f  push e
            // 外3轮 tree 为F
            // 内1轮 push f push G
            // 外4轮 tree 为c
            // 内1轮 push c


            // 外1轮 弹栈D
            // 外2轮 弹栈e
            // 外3轮 弹栈g
            // 外4轮 弹栈c
            tree = stack.pop();
            while (!stack.empty() && tree.getRightTree() == null) {
                System.out.print(tree.getTreeValue());
                //外1轮 输出d
                //外3轮 输出g  弹出f 输出f
                tree = stack.pop();
            }
            System.out.println(tree.getTreeValue());
            // 外1轮 输出b
            // 外2轮 输出e
            // 外3轮 输出a
            // 外4轮 输出c
            if (!stack.empty()) {
                tree = stack.pop();
                // 外1轮：栈不为空，弹出E，tree为E了，外层循环继续，
                // 外2轮 弹出f tree为f
                // 外3轮 弹出c tree为c
            } else {
                tree = null;
            }
        }
    }



    public static void main(String[] args) {



        Tree root = new Tree("A");
        Tree treeb = new Tree("B");
        Tree treec = new Tree("C");
        Tree treed = new Tree("D");
        Tree treee = new Tree("E");
        Tree treef = new Tree("F");
        Tree treeg = new Tree("G");
        root.setLeftTree(treeb);
        root.setRightTree(treec);
        treeb.setLeftTree(treed);
        treeb.setRightTree(treee);
        treee.setRightTree(treef);
        treef.setLeftTree(treeg);
        System.out.println("前序递归：");
//        preOrder(root);
        System.out.println("\n前序非递归：");
//        preOrderStack(root);
        System.out.println("\n中序非递归：");
        inOrderStack(root);

    }
}
