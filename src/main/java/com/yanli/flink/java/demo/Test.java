package com.yanli.flink.java.demo;

import java.util.HashMap;
import java.util.Map;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: Test
 * @date 2021/9/2 4:23 下午
 */
public class Test {
    public static int lengthOfLongestSubstring(String s) {
        int len = s.length();
        int result =0;
        Map<Character,Integer> map = new HashMap<>();
        for(int i=0,j=0;j<len;j++){
            if(map.containsKey(s.charAt(j))){
                i = Math.max(map.get(s.charAt(j)),i);
            }
            result = Math.max(result,j-i+1);
            map.put(s.charAt(j),j+1);
        }
        return result;
    }

    public static void main(String[] args) {
        int length = lengthOfLongestSubstring("abba");
        System.out.println(length);
    }
}
