package main.java.com.ch.algorithm;

/**
 * Created by ch on 2016/12/20.
 * sort Demo
 */
public class SortDemo {

    static int[] temp = new int[20];

    public static void main(String[] args) {
        int[] arr = new int[]{55, 33, 10, 11, 177, 111, 215, 45, 12,12};
        System.out.println("before sort-----:");
        printValue(arr);


//        BubbleSort0(arr, arr.length);
//        BubbleSort1(arr, arr.length);
//        BubbleSort2(arr, arr.length);
//        BubbleSort3(arr, arr.length);

//        quickSort(arr, 0, arr.length-1);
//        quickSort2(arr, 0, arr.length - 1);

//        selectSort(arr,arr.length);
//        selectSort2(arr, arr.length);

//        InsertSort(arr, arr.length);
//        InsertSort2(arr, arr.length);
//        BinaryInsertSort(arr, arr.length);

//        ShellSort(arr,arr.length);
//        ShellSort1(arr,arr.length);
//        ShellSort2(arr, arr.length);


//        mergeSort(arr, 0, arr.length - 1);

        lsdRadixSort(arr,0,arr.length-1,3);

        System.out.println("\nafter sort-----:");
        printValue(arr);
    }

    private static void printValue(int[] arr) {
        for (int n : arr) {
            System.out.print(n + "\t");
        }
        System.out.println();
    }

    /**
     * BubbleSort 冒泡排序
     * 循环遍历，每一次比较相邻两位
     *
     * @param arr
     */
    private static void BubbleSort0(int[] arr, int len) {
        for (int i = 0; i < len - 1; i++) { // 轮次
            for (int j = 0; j < len - 1 - i; j++) { // swap
                if (arr[j] > arr[j + 1]) {
                    swap(arr, j, j + 1);
                }
                System.out.print("第" + (i + 1) + "次排序,第" + (j + 1) + "次交换：");
                printValue(arr);
                System.out.println();
            }
            /*System.out.print("第"+(i+1)+"次排序：");
            printValue(arr);
            System.out.println();*/
        }
    }

    // 加数据交换判断标志
    private static void BubbleSort1(int[] arr, int len) {
        for (int i = 0; i < len - 1; i++) { // 轮次
            boolean swap = false;
            for (int j = 0; j < len - 1 - i; j++) { // swap
                if (arr[j] > arr[j + 1]) {
                    swap(arr, j, j + 1);
                    swap = true;
                }
            }
            // 一轮下来木有交换，则已经排好序了
            if (!swap) {
                break;
            }
        }
    }

    // 记录数据最后交换位置
    private static void BubbleSort2(int[] arr, int len) {
        int i = len - 1; // begin 最后位置不变
        while (i > 0) {
            int pos = 0;  // 每趟开始时，无数据交换
            for (int j = 0; j < i; j++) {
                if (arr[j] > arr[j + 1]) {
                    pos = j;
                    swap(arr, j, j + 1);
                }
            }
            i = pos;
        }
    }

    // 双向冒泡
    private static void BubbleSort3(int[] arr, int len) {
        int start = 0;
        int end = len - 1;
        while (start < end) {
            for (int j = start; j < end; j++) { // 正向冒泡，找max
                if (arr[j] > arr[j + 1]) {
                    swap(arr, j, j + 1);
                }
            }
            --end;
            for (int j = end; j > start; j--) {
                if (arr[j] < arr[j - 1]) {
                    swap(arr, j, j - 1);
                }
            }
            ++start;
        }
    }

    // 快速排序  交换
    private static void quickSort(int[] arr, int start, int end) {
        int i = start;
        int j = end;
        int key = arr[i];
        while (i < j) {
            while (i < j && arr[j] >= key) {
                j--;
            }
            if (i < j) {
                swap(arr, i, j);
            }
            while (i < j && arr[i] <= key) {
                i++;
            }
            printValue(arr);
            if (i < j) {
                swap(arr, i, j);
            }
            printValue(arr);
        }
        // 到这里i == j了，也就是key的位置
        if (i - start > 1) { // 保证key前可以交换,key不在前两位
            quickSort(arr, start, i - 1);
        }
        if (end - j > 1) // // 保证key前可以交换，key不在后两位
            quickSort(arr, j + 1, end);
    }

    // 快速排序 挖坑填数 + 分治法
    public static void quickSort2(int arr[], int start, int end) {
        if (start < end) {
            int i = start;
            int j = end;
            int key = arr[i];
            while (i < j) {
                while (i < j && arr[j] >= key) { //从右到左找出第一个小于key的数
                    j--;
                }
                if (i < j) {
                    arr[i++] = arr[j];
                }
                while (i < j && arr[i] < key) {  //从左到右找第一个大于或等于key的数
                    i++;
                }
                if (i < j) {
                    arr[j--] = arr[i];
                }
            }
            arr[i] = key;
            quickSort(arr, start, i - 1);//递归调用
            quickSort(arr, i + 1, end);
        }
    }

    // 选择排序
    private static void selectSort(int[] arr, int len) {
        for (int i = 0; i <= len - 1; i++) {
            int min = i;
            for (int j = i + 1; j <= len - 1; j++) {
                if (arr[j] < arr[min]) {
                    min = j; // 记下第i小的数，比较后，得到最小
                }
            }
            if (min != i) { // 如果第i小的数不在i位置上，交换
                swap(arr, min, i);
            }
        }
    }

    //二元选择排序
    private static void selectSort2(int[] arr, int len) {
        for (int i = 0; i <= len / 2; i++) {
            int min = i;
            int max = i;
            for (int j = i + 1; j < len - i; j++) {
                // 因为一次选了两个，那么j的范围是len-i
                if (arr[j] < arr[min]) {
                    min = j;
                }
                if (arr[j] > arr[max]) {
                    max = j;
                }
            }
            if (min != i) {  // 替换最小值
                swap(arr, min, i);
            }
            if (max != len - 1 - i) { // 替换最大值
                swap(arr, max, len - 1 - i);
            }
        }
    }

    // 归并排序
    private static void mergeSort(int[] arr, int start, int end) {
        if (start >= end) {
            return;
        }
        int mid = (start + end) / 2;
        mergeSort(arr, start, mid);
        mergeSort(arr, mid + 1, end);
        merge(arr, start, mid, end);
    }

    // 归并后，排序的逻辑
    private static void merge(int[] arr, int start, int mid, int end) {
        int i = start;
        int j = mid + 1;
        int size = 0;
        for (; (i <= mid) && j <= end; size++) {
            if (arr[i] < arr[j]) {
                temp[size] = arr[i++];
            } else {
                temp[size] = arr[j++];
            }
        }
        while (i <= mid) {
            temp[size++] = arr[i++];
        }
        while (j <= end) {
            temp[size++] = arr[j++];
        }
        for (i = 0; i < size; i++) {
            arr[start + i] = temp[i];
        }
    }

    // 直接插入排序
    private static void InsertSort(int[] arr, int len) {
        for (int i = 1; i <= len - 1; i++) {
            if (arr[i] < arr[i - 1]) { // 需要插入，否则continue
                int temp = arr[i];
                for (int j = i - 1; j >= 0; j--) {
                    // 逆向遍历，有大于temp的，就要后移，腾出temp的位置
                    if (arr[j] > temp) { // 后移
                        arr[j + 1] = arr[j];
                        arr[j] = temp;
                    }
                }
            }
        }
    }

    // 二分插入排序
    private static void BinaryInsertSort(int[] arr, int len) {
        for (int i = 1; i <= len - 1; i++) {
            int temp = arr[i];
            int low = 0;
            int high = i - 1;
            // 使用二分法查出数据应该插入的位置 low
            while (low <= high) {
                int mid = (low + high) / 2;
                if (arr[i] < arr[mid]) {
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            }
            // low 到j的数据都后移
            for (int j = i - 1; j >= low; j--) {
                arr[j + 1] = arr[j];
            }
            arr[low] = temp;
        }
    }

    // 希尔排序
    private static void ShellSort(int[] arr, int len) {
        int dt = (len - 1) / 2; // 初始步长
        for (; dt >= 1; dt = dt / 2) { // 步长递减
            for (int i = 0; i <= dt; i++) {
                // 选定一组之后，进行比较并交换
                for (int j = i + dt; j <= len - 1; j += dt) {
                    if (arr[j] < arr[j - dt]) {
                        // 如果某一位比前一位大，则要递归和前面所有的位比较
                        int temp = arr[j];
                        int k = j - dt;
                        while (k >= 0 && arr[k] > temp) {
                            arr[k + dt] = arr[k];
                            k -= dt;
                        }
                        arr[k + dt] = temp;
                    }
                }
            }
        }
    }

    // 希尔排序 改变开始位置
    private static void ShellSort1(int[] arr, int len) {
        int dt = (len - 1) / 2;
        for (; dt >= 1; dt = dt / 2) {
            for (int j = dt; j <= len - 1; j++) { // 从dt位开始
                if (arr[j] < arr[j - dt]) {
                    //每个元素与自己组内的数据进行直接插入排序
                    int temp = arr[j];
                    int k = j - dt;
                    while (k >= 0 && arr[k] > temp) {
                        arr[k + dt] = arr[k];
                        k -= dt;
                    }
                    arr[k + dt] = temp;
                }
            }
        }
    }

    // 希尔排序
    private static void ShellSort2(int[] arr, int len) {
        int dt = (len - 1) / 2;
        for (; dt >= 1; dt = dt / 2) {
            for (int i = dt; i <= len - 1; i++) { // 从dt位开始
                for (int j = i - dt; j >= 0 && arr[j + dt] < arr[j]; j -= dt) {
                    swap(arr, j, j + dt);
                }
            }
        }
    }

    // 直接插入排序
    private static void InsertSort2(int[] arr, int len) {
        for (int i = 1; i <= len - 1; i++) {
            for (int j = i - 1; j >= 0 && arr[j + 1] < arr[j]; j--) {
                swap(arr, j, j + 1);
            }
        }
    }

    // lsd桶排序
    private static void lsdRadixSort(int[] arr, int start, int end, int digit) {
        final int radix = 10;
        int i = 0, j = 0;
        int[] count = new int[radix]; // 存放各个桶的数据统计个数
        int[] bucket = new int[end - start + 1];
        // 从低位到高位
        for (int d = 1; d <= digit; d++) {
            // 初始化桶数据
            for (i = 0; i < radix; i++) {
                count[i] = 0;
            }
            // 统计各个桶要装入的数据个数
            for (i = start; i <= end; i++) {
                j = getDigit(arr[i], d); // 返回1，则放入1号桶
                count[j]++;
            }
            // count[i]表示第i个桶的右边界索引
            for (i = 1; i < radix; i++) {
                count[i] = count[i] + count[i - 1];
            }
            // 将数据依次装入桶中
            // 这里要从右向左扫描，保证排序稳定性
            for (i = end; i >= start; i--) {
                j = getDigit(arr[i], d); // 求出关键码的第k位的数字， 例如：576的第3位是5
                System.out.println(arr[i]+"-"+d+"-"+j+"-"+count[j]);
                bucket[count[j] - 1] = arr[i]; // 放入对应的桶中，count[j]-1是第j个桶的右边界索引
                count[j]--; // 对应桶的装入数据索引减一
            }
            // 将已分配好的桶中数据再倒出来，此时已是对应当前位数有序的表
            for (i = start, j = 0; i <= end; i++, j++) {
                arr[i] = bucket[j];
            }
        }
    }

    /**
     * 获取数num d位上的数字
     * eg:123 1位-> 3
     * 各位：1 十位 2 百位 3
     *
     * @param num
     * @param d
     * @return
     */
    private static int getDigit(int num, int d) {
        int a[] = {1, 1, 10, 100};
        return num / a[d] % 10;
    }


    private static void swap(int[] arr, int i, int j) {
        arr[i] = arr[i] ^ arr[j];
        arr[j] = arr[i] ^ arr[j];
        arr[i] = arr[i] ^ arr[j];
    }

}
