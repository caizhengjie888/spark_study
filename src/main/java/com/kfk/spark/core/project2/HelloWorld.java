package com.kfk.spark.core.project2;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/1
 * @time : 5:12 下午
 */
public class HelloWorld {
    public static void main(String[] args) {
        int c = Integer.parseInt("11");
        int a = Integer.parseInt("11");
        Integer d = Integer.valueOf("11");
        Integer t = Integer.parseInt("11");

        int i = 128;
        Integer i5 = 128;
        Integer i2 = 128;
        Integer i3 = new Integer(128);
        Integer i4 = Integer.valueOf(4);

        int as = 32;
        int xc = 32;
        System.out.println(as == xc);

        System.out.println(i2 == i5);


        System.out.println(a == c  );
        System.out.println();
        System.out.println(d.equals(c));
    }
}
