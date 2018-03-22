package com.demo.cn.recommend;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MovieYearRegx {

    private static String moduleType=".*\\(([1-9][0-9][0-9][0-9])\\).*";

    public static void main(String []args){
        System.out.println(movieYear("goldAage(2017)"));
    }

    public static int movieYear(String yearName){
        int getYear=2000;
        Pattern pattern=Pattern.compile(moduleType);
        Matcher matcher=pattern.matcher(yearName);
        while (matcher.find()){
             getYear=Integer.parseInt(matcher.group(1));
        }

        return getYear;
    }
}
