package com.fiberhome.locksdb.jetty;

import java.util.ArrayList;  
import java.util.HashMap;  
import java.util.LinkedList;
import java.util.List;  
import java.util.Map;  
  
import com.alibaba.fastjson.JSON;  
import com.alibaba.fastjson.TypeReference;  
import com.fiberhome.locksdb.client.Pair;


public class TestFastJson {  
    static void method3(){  
        System.out.println("List<String>转锟斤拷示锟斤拷始----------");  
        List<String> list = new ArrayList<String>();  
        list.add("fastjson1");  
        list.add("fastjson2");  
        list.add("fastjson3");  
        String jsonString = JSON.toJSONString(list);  
        System.out.println("json锟街凤拷:"+jsonString);  
          
        //锟斤拷锟斤拷json锟街凤拷  
        List<String> list2 = JSON.parseObject(jsonString,new TypeReference<List<String>>(){});   
        System.out.println(list2.get(0)+"::"+list2.get(1)+"::"+list2.get(2));  
        System.out.println("List<String>转锟斤拷示锟斤拷锟斤拷锟�---------");  
  
    }  
    
    static void method5(){  
        System.out.println(" List<Map<String,Object>>转锟斤拷示锟斤拷始 ----------");  
        Map<String,Object> map = new HashMap<String,Object>();  
        map.put("key1", "value1");  
        map.put("key2", "value2");  
        Map<String,Object> map2 = new HashMap<String,Object>();  
        map2.put("key1", 1);  
        map2.put("key2", 2);  
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();  
        list.add(map);  
        list.add(map2);  
        String jsonString = JSON.toJSONString(list);  
        System.out.println("json锟街凤拷:"+jsonString);  

        List<Map<String,Object>> list2 = JSON.parseObject(jsonString,new TypeReference<List<Map<String,Object>>>(){});  
          
        System.out.println("map锟斤拷key1值"+list2.get(0).get("key1"));  
        System.out.println("map锟斤拷key2值"+list2.get(0).get("key2"));  
        System.out.println("ma2p锟斤拷key1值"+list2.get(1).get("key1"));  
        System.out.println("map2锟斤拷key2值"+list2.get(1).get("key2"));  
    } 
    
    
    
    
    
    static String method4(LinkedList<Pair<String, String>> list) {   
    	List<Map<String,String>> listTemp = new ArrayList<Map<String,String>>(list.size()); 
    	Map<String,String> map = new HashMap<String,String>();
    	for(Pair<String, String> pair : list)
    		map.put(pair.key, pair.value);
    	listTemp.add(map);
        String jsonString = JSON.toJSONString(listTemp);  
        return jsonString; 
    }  
    
    public static void main(String[] args) {  
//        method1();  
//        method2();  
//        method3();  
//        method5();  
    } 
    
    
    
      
}  
