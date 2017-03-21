package main.java.com.ch.algorithm.tree;

/**
 * Created by ch on 2017/3/9.
 * 二叉树遍历
 */
public class Tree {
    private Tree leftTree;
    private Tree rightTree;
    private Object treeValue;

/*    public static String preOrderTravResult="先序遍历结果：";
    public static String inOrderTravResult="中序遍历结果：";
    public static String postOrderTravResult="后序遍历结果：";
    public static String levelOrderTravResult="层次遍历结果：";*/
    public Tree(Object treeValue){
        this.treeValue=treeValue;
    }
/*    public String toString(){
        return treeValue+",";
    }*/

    public void setLeftTree(Tree leftTree) {
        this.leftTree = leftTree;
    }

    public void setRightTree(Tree rightTree) {
        this.rightTree = rightTree;
    }

    public Tree getLeftTree(){
        return leftTree;
    }
    public Tree getRightTree(){
        return rightTree;
    }

    public Object getTreeValue() {
        return treeValue;
    }

    public void setTreeValue(Object treeValue) {
        this.treeValue = treeValue;
    }
}
