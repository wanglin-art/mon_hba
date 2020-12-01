package org.monba.utils;

import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

public class Test {
    public static void main(String[] args) {

//
//        JSONObject jsonObject = new JSONObject();
//        //为空判断
//        boolean AIsNull = jsonObject.isNull("A");
//        String A;
//        if(AIsNull){
//            A = null;    //列名为：dailyBudgetAmount
//        }else{
//            A = jsonObject.getString("A");
//        }
//
//       // for循环拼接
//        JSONArray B = jsonObject.getJSONArray("B");
//        JSONArray jsonArrayX = new JSONArray();
//        for (Object o : B) {
//            jsonArrayX.put(o);
//        }
//        String str = jsonArrayX.toString(); //列名为：str

        //有可能为null
        //如果不为null里面的值要进行拼接
//        boolean HIsNull = jsonObject.isNull("H");
//        String I;       //列名为：I
//        if (HIsNull) {
//            I = null;
//        } else {
//            JSONArray JJSONArray = jsonObject.getJSONArray("J");
//            JSONArray jsonArrayK = new JSONArray();
//            for (Object o : JJSONArray) {
//                jsonArrayK.put(o);
//            }
//            I = jsonArrayK.toString();
//        }

//        Long ts = 1589241600L;
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
//        String format = simpleDateFormat.format(ts*1000);
////        System.out.println(format);
//
//
//        ArrayList<String> strings = new ArrayList<>();
//        strings.add("faf");
//        strings.add("faf");
//        strings.add("faf");
//        strings.add("faf");
//        strings.add("faf");
//
//        String text = "hello_word";
//        String[] s = text.split("_");
//        System.out.println(s[1]);
//
//        long lo =(long) 123131.123;
//        System.out.println(lo);

        long ad_id =12123;
        String kw ="fsafs";

        String sql = "select kw_id from meta_keyword where ad_id = "+ad_id+" and text = " +"\'"+kw+"\'";
        System.out.println(sql);
    }
}
