package com.pride.core;


import com.pride.core.Tools.Tools;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Repository
public class CommonDAO {

    @PersistenceContext
    private EntityManager em;

    private final ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

    public Page createQuery(String qlString,Map<String,Object> parameterMap,Pageable p) {
        Query query = em.createQuery(qlString);
        setQueryParameter(parameterMap, query);
        Future<Integer> future = getTotalByHql(qlString,parameterMap);
        query.setMaxResults(p.getPageSize());
        query.setFirstResult(p.getPageNumber()*p.getPageSize());
        List<Object> list = query.getResultList();
        List content = getQueryResult(qlString, list);
        int total = 0;
        try {
            total = future.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new PageImpl(content,p,total);
    }

    public Page createNativeQuery(String qlString,Map<String,Object> parameterMap,Pageable p){
        Query query = em.createNativeQuery(qlString);
        setQueryParameter(parameterMap, query);
        Future<Integer> future = getTotalBySql(qlString, parameterMap);
        query.setMaxResults(p.getPageSize());
        query.setFirstResult(p.getPageNumber()*p.getPageSize());
        List<Object> list = query.getResultList();
        List content = getQueryResult(qlString, list);
        int total = 0;
        try {
            total = future.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new PageImpl(content,p,total);
    }

    private Future<Integer> getTotalBySql(String qlString, Map<String, Object> parameterMap) {
        return cachedThreadPool.submit(() -> {
                Query totalQuery = em.createNativeQuery(qlString);
                setQueryParameter(parameterMap, totalQuery);
                return totalQuery.getResultList().size();
            });
    }

    private Future<Integer> getTotalByHql(String qlString, Map<String, Object> parameterMap) {
        return cachedThreadPool.submit(() -> {
            Query totalQuery = em.createQuery(qlString);
            setQueryParameter(parameterMap, totalQuery);
            return totalQuery.getResultList().size();
        });
    }

    public List<Map> createNativeQuery(String qlString,Map<String,Object> parameterMap){
        Query query = em.createNativeQuery(qlString);
        setQueryParameter(parameterMap, query);
        List<Object> list = query.getResultList();
        List<Map> content = getQueryResult(qlString, list);
        return content;
    }

    public List createNativeQuery(String qlString,Map<String,Object> parameterMap,Class<?> targetClass){
        Query query = em.createNativeQuery(qlString);
        setQueryParameter(parameterMap, query);
        List<Object> list = query.getResultList();
        List<Map> content = getQueryResult(qlString, list);
        List _content = new ArrayList(content.size());
        content.forEach(m->{
            try {
                _content.add(Tools.mapToBean(m,targetClass));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        return _content;
    }

    public List createQuery(String qlString,Map<String,Object> parameterMap){
        Query query = em.createQuery(qlString);
        setQueryParameter(parameterMap, query);
        List<Object> list = query.getResultList();
        List content = getQueryResult(qlString, list);
        return content;
    }

    private List<Map> getQueryResult(String qlString, List<Object> list) {
        List<Map> content = new ArrayList(list.size());
        List<String> returnAttrList = new ArrayList<>();
        Pattern pattern =Pattern.compile("select\\s+(.*?)\\s+from\\s+", Pattern.CASE_INSENSITIVE);
        Matcher matcher=pattern.matcher(qlString.trim());
        if(matcher.find()){
            String matcherStr = replace(matcher.group(1));
            for(String str : matcherStr.split(",")){
                String[] arr = str.trim().split("\\s+");
             returnAttrList.add(arr[arr.length-1]);
            }
        }
        for(Object object:list){
            Map map = new LinkedHashMap();
            if(object.getClass().isArray()){
                Object[] objects = (Object[]) object;
                for(int i=0;i<returnAttrList.size();i++){
                    map.put(returnAttrList.get(i),objects[i]);
                }
            }else{
                for(int i=0;i<returnAttrList.size();i++){
                    map.put(returnAttrList.get(i),object);
                }
            }
            content.add(map);
        }
        return content;
    }

    // 识别括号并将括号内容替换的函数
    private  String replace(String str){
        int head = str.indexOf('('); // 标记第一个使用左括号的位置
        if (head == -1)
            ; // 如果str中不存在括号，什么也不做，直接跑到函数底端返回初值str
        else {
            int next = head + 1; // 从head+1起检查每个字符
            int count = 1; // 记录括号情况
            do {
                if (str.charAt(next) == '(')
                    count++;
                else if (str.charAt(next) == ')')
                    count--;
                next++; // 更新即将读取的下一个字符的位置
                if (count == 0) // 已经找到匹配的括号
                {
                    String temp = str.substring(head, next); // 将两括号之间的内容及括号提取到temp中
                    str = str.replace(temp, ""); // 用空内容替换，复制给str
                    head = str.indexOf('('); // 找寻下一个左括号
                    next = head + 1; // 标记下一个左括号后的字符位置
                    count = 1; // count的值还原成1
                }
            } while (head != -1); // 如果在该段落中找不到左括号了，就终止循环
        }
        return str; // 返回更新后的str
    }

    private void setQueryParameter(Map<String, Object> parameterMap, Query query) {
        if(parameterMap!=null){
            Iterator<Map.Entry<String, Object>> it = parameterMap.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry<String, Object> me = it.next();
                query.setParameter( me.getKey(),me.getValue());
            }
        }
    }

}
