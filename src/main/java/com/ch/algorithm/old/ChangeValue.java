package main.java.com.ch.algorithm.old;

//不使用第三方变量，交换两个参数的值
public class ChangeValue {

    public static void main(String[] args) {
        method3();
    }

    //方法1：算数运算  ，把a,b当作数轴上的两点
    public static void method() {
        int a, b;
        a = 10;
        b = 15;
        a = b - a;//a=5,b=15;//求出a,b两点间的距离
        b = b - a;//a=5,b=10;//求出a到原点的距离，保存在b中
        a = b + a;//a=15,b=10;//求出b到原点的距离，保存在a中
        System.out.println(a + "\t" + b);
    }

    //方法2：指针地址操作
    public static void method2() {
    }

    //方法3：位运算
    /*
    由异或运算的特点决定的，通过异或运算能够使数据中的某些位翻转，
    其他位不变，即意味着任意一个数与任意一个给定的值连续异或两次，值不变
    即：a^b^b=a,将a=a^b代入b=a^b则得b=a^b^b=a,同理得到a=b^a^a=b;轻松完成交换
     * */
    public static void method3() {
        int a = 10, b = 12;//a=1010^b=1100;
        a = a ^ b;//a=0110^b=1100;
        b = a ^ b;//a=0110^b=1010;
        a = a ^ b;//a=1100=12;b=1010;
        System.out.println(a + "\t" + b);
    }
}
