package main.java.com.ch.algorithm.old;

//快速排序
public class QuickSort {

    /*
     //这个方法有问题，遇见100以上的数就排序错误
    public void quickSort(String [] strDate,int left ,int right){
    	String middle,tempDate;
    	int i,j;
    	i=left;
    	j=right;
    	middle=strDate[(i+j)/2];
    	do{
    		System.out.println(strDate[0].compareTo(strDate[1]));
    		while(strDate[i].compareTo(middle)<0 && i<right){
    			i++;//找出左边比中间值大的数
    		}
    		while(strDate[j].compareTo(middle)>0 && j>left){
    			j--;//找出右边比中间值小的数
    		}
    		if(i<=j){//将左边大的数和右边小的数进行替换
    			tempDate =strDate[i];
    			strDate[i]=strDate[j];
    			strDate[j]=tempDate;
    			i++;
    			j--;
    		}
    	}while(i<=j);//当两者交错时停止
    	if(i<right){
    		quickSort(strDate,i,right);
    	}
    	if(j>left){
    		quickSort(strDate,left,j);
    	}
    }
    */
    public static void quickSort(int a[], int start, int end) {
        int i, j;
        i = start;
        j = end;
        if ((a == null) || (a.length == 0)) {
            return;
        }
        while (i < j) {
            while ((i < j) && (a[i] <= a[j])) {
                j--; //以数组start 下标的数据为key，右侧扫描
            }
            if (i < j) {//右侧扫描，找出第一个比key小的数，交换位置
                int temp = a[i];
                a[i] = a[j];
                a[j] = temp;
            }
            while ((i < j) && (a[i] < a[j])) {
                i++;//左侧扫描(此时a[j]中存储着key值)
            }
            if (i < j) {//找出一个比key大的数，交换位置
                int temp = a[i];
                a[i] = a[j];
                a[j] = temp;
            }
            //			quickSort(a,0,i-1);
            //			quickSort(a,i+1,end);//可以写在这个地方(循环里面)，也可以写在下面(循环外面)
        }
        if ((i - start) > 1) {
            quickSort(a, 0, i - 1);//递归调用，把key前面的完成排序
        }
        if ((end - j) > 1) {
            quickSort(a, j + 1, end);//递归调用，把key后面的完成排序
        }

    }

    //和上面的方法一样滴
    //	public static void quickSort(int a[],int s,int e){
    //		if(s<e){
    //			//Swap(a[s],a[(s+r)/2]);//将中间的数和第一个数交换
    //			int i=s,j=e,x=a[s];
    //			while(i<j){
    //				while(i<j&& a[j]>=x){ //从右到左找出第一个小于x的数
    //					j--;
    //				}
    //				if(i<j){
    //					a[i++]=a[j];
    //				}
    //				while(i<j&& a[i]<x){  //从左到右找第一个大于或等于x的数
    //					i++;
    //				}
    //				if(i<j){
    //					a[j--]=a[i];
    //				}
    //			}
    //			a[i]=x;
    //			quickSort(a,s,i-1);//递归调用
    //			quickSort(a,i+1,e);
    //		}
    //	}

    public static void main(String[] args) {
        //字符串数组
        //		String [] strVoid =new String []{"121","145","33","0","77","99","15","45"};
        //		QuickSort sort =new QuickSort();
        //		sort.quickSort(strVoid,0,strVoid.length-1);
        //		for(int i=0;i<strVoid.length;i++){
        //			System.out.println(strVoid[i]+"");

        //int数组

        int[] strVoid = new int[] { 11, 55, 33, 10, 177, 111, 215, 45 };
        quickSort(strVoid, 0, strVoid.length - 1);
        for (int i = 0; i < strVoid.length; i++) {
            System.out.println(strVoid[i] + "");
        }
    }
}
