package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 *  可以处理多种nested属性，形成多级嵌套的Map[String,Object]对象:
 *  1. user.name, user.email  => 'user': {'name':..., 'email':...}
 *  2. user.address.city, user.address.tel => 'user':{ 'address':{ 'city':...,'tel':... }}
 *  3. user. = `{'name':..., 'email':...}` => 'user': {'name':..., 'email':...}
 *  4. address[city], address[zip] => address:[ {'city':..,'zip':..},{'city':..,'zip':..}]
 *  任何以下划线开头字段不会被处理到json value中
 */
public class JsonParser {
    protected int ID_FIELD=-1;
    protected List<String> columns;
    protected int columnNumber;

    public final String SKIPPED_COLUMN_PREFIX="_";
    final String NESTED_SPLITTER=".";

    public JsonParser(int idFieldIdx, List<String> columnNames){
        ID_FIELD = idFieldIdx;
        columns = columnNames;
        columnNumber = columnNames.size();
    }

    //无需跨行处理
    public Record tryFetchWholeGroupOnReader(RecordReceiver recordReceiver, List<Record> writeBuffer){
        return null;
    }
    /**
     * 逐行处理多级属性名{a.b.c}， 单行转为Map
     */
    public List<Map<String,Object>> getJsonBatch(List<Record> records, BiConsumer<Record,Map<String,Object>> callback) {
        List<Map<String, Object>> batch = new ArrayList<>(records.size());
        for(Record r : records){
            Map<String, Object> row = getNestedDoc(r);//单行记录转Map

            callback.accept(r,row);
            batch.add(row);
        }
        return batch;
    }

    protected Map<String,Object> getNestedDoc(Record r){
        Map<String,Object> root = new HashMap<String,Object>();
        for(int i=0;i<columnNumber;i++){
            String colName = columns.get(i);
            if(!colName.startsWith(SKIPPED_COLUMN_PREFIX))//所有下划线开头的字段都忽略
                appendNestedProp(root,colName,r.getColumn(i));
        }
        return root;
    }
    //字段值为JSON String
    private void addNestedPropByJsonString(Map<String,Object> root,String nestedPropName,String val) {
        //以JSON串完整替换到对象属性
        root.put(nestedPropName, JSON.parseObject(val));
    }
    //以'.'号分隔的字段名， 要转为多层嵌套对象
    protected void appendNestedProp(Map<String,Object> root, String props, Column val){
        if(props.indexOf(NESTED_SPLITTER)>0){
            String[] nested = StringUtils.split(props, NESTED_SPLITTER, 2);//第一个分隔符
            if(props.endsWith(NESTED_SPLITTER)) {
                //值为JSON，如 user. => {name:'',email:''...}
                if(nested.length>1) {
                    //多级字段， 如user.address. ==> [user].[address].{...}
                    Map<String,Object> child = (Map<String,Object>)root.get(nested[0]);
                    if(child==null){
                        child=new HashMap<String,Object>();
                        root.put(nested[0], child);
                    }
                    appendNestedProp(child,nested[1],val);
                }else
                    addNestedPropByJsonString(root,nested[0],val.getRawData()==null?null:val.asString());
            }else {
                //多个字段， 如user.name, user.email ...
                Map<String,Object> child = (Map<String,Object>)root.get(nested[0]);
                if(child==null){
                    child=new HashMap<String,Object>();
                    root.put(nested[0], child);
                }

                appendNestedProp(child,nested[1],val);
            }
        }
        else{
            setColumValue(root,props,val);
        }
    }

    protected void setColumValue(Map<String,Object> obj,String props,Column val)
    {
        if(val instanceof DateColumn)//日期型
            obj.put(props,
                    null == val.getRawData()?null:
                            DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT//2009-03-20T22:07:01+08:00
                                    .format(val.asDate())
            );
        else
            obj.put(props, val.getRawData());
    }
}
