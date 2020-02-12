package com.dtl.util;

import java.util.ArrayList;
import java.util.Random;

public class RandomUtil {
		
	public static ArrayList<String> getDiffNO(int n){
        // 生成 [0-n) 个不重复的随机数
        // list 用来保存这些随机数
        ArrayList<String> list = new ArrayList<String>();
        Random rand = new Random();
        boolean[] bool = new boolean[n];
        int num = 0;
        for (int i = 0; i < n; i++) {
            do {
                // 如果产生的数相同继续循环
                num = rand.nextInt(n);
            } while (bool[num]);
            bool[num] = true;
            list.add(String.valueOf(num));
        }
        return list;
    }

}
