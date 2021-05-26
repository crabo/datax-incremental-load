package com.alibaba.datax.plugin.writer.jsonwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiConsumer;

/**
 "parameter":{
     "column": ["_id","...", ...] //必须包含 _id等es关键Meta，以下划线开头。
     "server": ["ip:9000","ip2:9000"], //Es servers
     "topicIndex":1, //动态替换topic的 %%
     "topic": "t08-%%", //写入目标index名。
     "topicOfMonth": 6, //如果topicIndex字段为Date类型，则每N个月合并到同一index, 格式为：yy0d

     "batchSize": 2048, //向kafka一次性写入的数据量。
     "refreshSec": 5 //最迟5s刷到写入（批次不足时）
     "keyIndex": 0, //多行合并
     "parseArray": true, //多行按Key合并？
 },
 */
public class EsWriter extends Writer {
    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }
        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }

            return writerSplitConfigs;
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(EsWriter.Task.class);
        private Configuration writerSliceConfig = null;
        private List<String> columns;
        private int IdFieldIndex = -1; //用于行合并的id列：tradeID
        private int TopicFieldIndex = -1; //写入的‘动态’主题列
        private String writeMode; //写入方式：index/update

        private int batchSize = 2048;
        private JsonParser jsonParser = null;
        String INDEX_TOPIC;
        private int INDEX_TOPIC_OF_MONTH=6;
        RestClient producer=null;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.columns = writerSliceConfig.getList("column", String.class);
            this.writeMode = writerSliceConfig.getString("writeMode", "update");//index 或  update
            this.IdFieldIndex = writerSliceConfig.getInt("keyIndex",-1);
            this.TopicFieldIndex = writerSliceConfig.getInt("topicIndex",-1);
            this.batchSize = writerSliceConfig.getInt("batchSize",2048);
            this.INDEX_TOPIC_OF_MONTH = writerSliceConfig.getInt("topicOfMonth",6);
            this.INDEX_TOPIC = writerSliceConfig.getString("topic",null);
            if(this.INDEX_TOPIC==null){
                throw new IllegalArgumentException("写入的目标索引名'Topic'必须指定！");
            }

            this.jsonParser = writerSliceConfig.getBool("parseArray",false)?
                    new JsonArrayParser(this.IdFieldIndex,this.columns) :
                    new JsonParser(this.IdFieldIndex,this.columns);

            List<String> servers = writerSliceConfig.getList("server", String.class);
            String authBasic = writerSliceConfig.getString("auth",null);

            HttpHost[] hosts = servers.stream().map(x->HttpHost.create(x)).toArray(HttpHost[]::new);
            RestClientBuilder builder = RestClient.builder(hosts);
            if(authBasic!=null && !authBasic.isEmpty()) {
                //auth格式为  用户名：密码
                byte[] credentials = Base64.encodeBase64(authBasic.getBytes(StandardCharsets.UTF_8));
                builder.setDefaultHeaders(new Header[]{ new BasicHeader(
                    "Authorization", "Basic "
                        + new String(credentials, StandardCharsets.UTF_8))
                    });
            }
            producer = builder.build();
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize+50);//合并末尾的同一分组
            while ((record = recordReceiver.getFromReader()) != null) {
                if(record.getColumnNumber() == 0)//TerminateRecord
                    break;
                //bugfix: _id列不允许为空!!
                if(IdFieldIndex>-1 && record.getColumn(IdFieldIndex).getByteSize()==0) {
                    LOG.warn("- drop 'Null' _id in record: {}",record);
                    continue;
                }
                writeBuffer.add(record);

                if (writeBuffer.size() >= batchSize) {
                    record = jsonParser.tryFetchWholeGroupOnReader(recordReceiver,writeBuffer);
                    //write batch
                    flush(writeBuffer);

                    if(record != null) {//下一batch的记录被fetch取出
                        if(record instanceof TerminateRecord){//reader已终止，直接退出（再次read会被阻塞）。
                            break;
                        }else {
                            writeBuffer.add(record);//新分组记录，flush()后需加回
                        }
                    }
                }
            }

            if(!writeBuffer.isEmpty()){
                flush(writeBuffer);
            }
        }

        /**
         POST _bulk
         { "index" : {"_id" : "1", "_index" : "test" } }
         { "field1" : "value1" }
         { "update" : {"_id" : "1", "_index" : "test"} }
         { "doc" : {"field2" : "value2"} }  <-- 结构不同
         */
        void flush(List<Record> writeBuffer){
            Map<String,String> postEntities = new HashMap<>();
            jsonParser.getJsonBatch(writeBuffer, new BiConsumer<Record, Map<String, Object>>() {
                @Override
                public void accept(Record record, Map<String, Object> row) {
                    String topic = calcDynamicIndexName(record);
                    String postEntry = ElasticRestHelper.recordToBulkEntity(record,row
                                    ,writeMode,columns,topic);
                    postEntities.put(
                         topic + row.get("_id").toString(),
                            postEntry);
                }
            });

            try {
                ElasticRestHelper.postRequest(this.producer, postEntities);
                writeBuffer.clear();//必须清空
            } catch (Exception e) {
                LOG.warn("'_bulk' post error",e);
                throw new RuntimeException("'_bulk' post error with"+e.getMessage());
            }
        }

        @Override
        public void destroy() {
            if(this.producer !=null){
                try {
                    this.producer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private String calcDynamicIndexName(Record record){
            if(this.TopicFieldIndex > -1){
                Column calcField = record.getColumn(this.TopicFieldIndex);
                if(calcField instanceof DateColumn){//日期类型：按月合并index，如：每6个月一索引
                    Date calcVal = calcField.asDate();
                    Calendar calc = Calendar.getInstance();

                    calc.setTime(calcVal);
                    int y = calc.get(Calendar.YEAR);//2021
                    int m=calc.get(Calendar.MONTH);//start form 0-11
                    int shard = (int) (Math.ceil(m/this.INDEX_TOPIC_OF_MONTH)+1);
                    return this.INDEX_TOPIC.replace("%%",//2101
                            String.format("%d%02d",y-2000, shard)
                    );
                }else{
                    return this.INDEX_TOPIC.replace("%%",calcField.asString());
                }
            }else
                return this.INDEX_TOPIC;
        }
    }
}
