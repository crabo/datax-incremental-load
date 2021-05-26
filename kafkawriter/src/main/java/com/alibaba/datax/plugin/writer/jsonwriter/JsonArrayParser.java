package com.alibaba.datax.plugin.writer.jsonwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class JsonArrayParser extends JsonParser {
    final String ARRAY_SPLITTER="[";

    public JsonArrayParser(int idFieldIdx, List<String> columnNames){
        super(idFieldIdx, columnNames);
    }
    /**
     * 读取buffer最后一行id，持续判断reader直到id不同为止。
     * 1. reader数据必须按 id_field 进行排序，确保同一分组连续分布
     * 2. 获得当前buffer数据后， 继续向后读取， 直到 id_field不同，则分组结束
     * 3. a)返回record，该记录从reader取出，并且不属于最后一个分组。外层应将它加入下一batch
     *    b)返回‘伪’TerminateRecord，通知外层此reader已结束
     *    c)返回null， 分组无需处理
     */
    @Override
    public Record tryFetchWholeGroupOnReader(RecordReceiver recordReceiver, List<Record> writeBuffer){
        if(!writeBuffer.isEmpty())
        {
            Record prev = writeBuffer.get(writeBuffer.size()-1);
            String prevId= prev.getColumn(this.ID_FIELD).asString();
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                if(prevId.equals(record.getColumn(this.ID_FIELD).asString())){
                    writeBuffer.add(record);
                }else{//diff group
                    return record;
                }
            }
            if(record==null)
                return TerminateRecord.get();
        }
        return null;
    }

    /**
     * 将连续的分组记录，区分为普通行、分组合并行
     * 按columns提供的属性名、值 构造为 Map对象
     */
    @Override
    public List<Map<String,Object>> getJsonBatch(List<Record> records, BiConsumer<Record,Map<String,Object>> callback){
        List<Map<String,Object>> batch = new ArrayList<>(records.size());

        Record[] array = records.toArray(new Record[0]);
        Map<String,List<Record>> groups = mergeGroupRecords(array);
        for(Record r : array){
            if(r!=null) {//为null的记录具有多行分组， 已移动到groups中
                Map<String,Object> row = getNestedDoc(r);//单行记录转Map

                callback.accept(r,row);
                batch.add(row);
            }
        }

        for(Map.Entry<String, List<Record>> group : groups.entrySet()){
            Record r = group.getValue().get(0);//多行记录
            Map<String,Object> row = getNestedDoc(r);
            mergeChildArrayToRoot(row,group.getValue());//多行合并同一Map

            callback.accept(r,row);
            batch.add(row);
        }
        return batch;
    }

    @Override
    protected Map<String,Object> getNestedDoc(Record r){
        Map<String,Object> root = new HashMap<String,Object>();

        for(int i=0;i<this.columnNumber;i++){
            String colName = this.columns.get(i);
            if(!colName.startsWith(this.SKIPPED_COLUMN_PREFIX))//所有下划线开头的字段都忽略
            {
                if(colName.indexOf(ARRAY_SPLITTER)<0)
                    appendNestedProp(root,colName,r.getColumn(i));
                else
                    appendNestedArrayProp(root,colName,r.getColumn(i));
            }
        }
        return root;
    }

    /**
     * root已通过第一条记录构造Map结构，
     * 后续只需要将数组[] 属性追加到root内； 非数组[]属性忽略。
     */
    void mergeChildArrayToRoot(Map<String,Object> root, List<Record> records){
        for(int k=1;k<records.size();k++)//跳过第一条
        {
            Record r = records.get(k);

            for(int i=0;i<this.columnNumber;i++){
                String colName = this.columns.get(i);
                if(colName.indexOf(ARRAY_SPLITTER)>0){
                    appendNestedArrayProp(root,colName,r.getColumn(i));
                }
            }
        }
    }
    /**
     * 对数组的属性使用单独的赋值方式
     */
    void appendNestedArrayProp(Map<String,Object> root, String propStr, Column val){
        if(propStr.indexOf(NESTED_SPLITTER)>0){
            String[] nested = StringUtils.split(propStr, NESTED_SPLITTER, 2);//第一个分隔符
            Map<String,Object> child = (Map<String,Object>)root.get(nested[0]);
            if(child==null){
                child=new HashMap<String,Object>();
                root.put(nested[0], child);
            }

            appendNestedArrayProp(child,nested[1],val);
        }
        else{//order[itemID]
            String[] array = StringUtils.split(propStr, ARRAY_SPLITTER);
            List<Map<String,Object>> list = (List<Map<String,Object>>)root.get(array[0]);
            if(list==null){
                list=new ArrayList<Map<String,Object>>();//构建一个初始数组
                root.put(array[0], list);
            }

            String column=array[1].replace("]", "");
            Map<String,Object> child =null;
            if(list.isEmpty() || list.get(list.size()-1).containsKey(column))//列名重复，则是新的一条记录
            {
                child=new HashMap<String,Object>();
                list.add(child);
            }else
                child=list.get(list.size()-1);//旧的一条记录

            super.setColumValue(child, column ,val);
        }
    }

    /**
     * 将数组内同组的记录移动到Map<>中， 原数组值置为null
     * @param records: 无分组的记录
     * @return 返回： 存在分组的记录
     */
    private Map<String,List<Record>> mergeGroupRecords(Record[] records){
        Map<String,List<Record>> groups = new HashMap<String,List<Record>>();
        if(records.length>1){
            String prevId=records[0].getColumn(ID_FIELD).asString();
            for(int i=1;i<records.length;i++)
            {
                String id = records[i].getColumn(ID_FIELD).asString();
                if(id.equals(prevId)){
                    List<Record> g = groups.get(id);
                    if(g==null){
                        g=new ArrayList<Record>();
                        g.add(records[i-1]);//prev one
                        records[i-1]=null;

                        groups.put(id, g);//move record to Map<>
                    }
                    g.add(records[i]);//curent
                    records[i]=null;//drop this
                }else
                    prevId=id;
            }
        }
        return groups;
    }
}
