package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

/**
 "parameter":{
     "server": "ip:9092", //Kafka的server地址。
     "topicIndex":1, //动态替换topic的 %%
     "topic": "t08-%%", //Kafka的topic。
     "batchSize": 1024, //向kafka一次性写入的数据量。
     "refreshSec": 5 //最迟5s刷到写入（批次不足时）
     "keyIndex": 0, //多行合并
     "parseArray": true, //多行按Key合并？
 },
 */
public class KafkaWriter extends Writer {
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
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration writerSliceConfig = null;
        private List<String> columns;
        private int IdFieldIndex = -1; //用于行合并的id列：tradeID
        private int TopicFieldIndex = -1; //写入的‘动态’主题列

        private int batchSize = 1024;
        private JsonParser jsonParser = null;
        private Producer<String, String> producer = null;
        private String KAFKA_TOPIC=null;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.columns = writerSliceConfig.getList("column", String.class);
            this.IdFieldIndex = writerSliceConfig.getInt("keyIndex",-1);
            this.TopicFieldIndex = writerSliceConfig.getInt("topicIndex",-1);
            this.batchSize = writerSliceConfig.getInt("batchSize",1024);
            this.KAFKA_TOPIC = writerSliceConfig.getString("topic");

            Properties props = new Properties();
            props.put("bootstrap.servers", writerSliceConfig.getString("server"));//指定broker的地址清单
            props.put("acks", "1");
            props.put("retries", 1);//收到临时性错误时，重发消息的次数
            props.put("retries.backoff.ms",500);//重试间隔
            props.put("batch.size",10 * writerSliceConfig.getInt("batchSize",1638));//批次行数
            props.put("linger.ms", 1000 * writerSliceConfig.getInt("refreshSec",2));//生产者在发送消息前等待秒
            props.put("buffer.memory", 33554432);//设置生产者内缓存区域的大小，生产者用它缓冲要发送到服务器的消息
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");//必须是一个实现org.apache.kafka.common.serialization.Serializer接口的类，将key序列化成字节数组。注意：key.serializer必须被设置，即使消息中没有指定key
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");//value序列化成字节数组
            this.producer = new KafkaProducer(props);

            this.jsonParser = writerSliceConfig.getBool("parseArray",false)?
                  new JsonArrayParser(this.IdFieldIndex,this.columns) :
                  new JsonParser(this.IdFieldIndex,this.columns);
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize+50);//合并末尾的同一分组
            while ((record = recordReceiver.getFromReader()) != null) {
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

        void flush(List<Record> writeBuffer){
            List<Map<String,Object>> batch = jsonParser.getJsonBatch(writeBuffer, new BiConsumer<Record, Map<String, Object>>() {
                @Override
                public void accept(Record record, Map<String, Object> row) {
                    row.put("_topic_",
                        TopicFieldIndex > -1? //是否动态topic写入？
                            KAFKA_TOPIC.replace("%%",
                                record.getColumn(TopicFieldIndex).asString())
                            : KAFKA_TOPIC);
                }
            });

            for(Map<String,Object> row : batch){
                producer.send(new ProducerRecord(
                    row.get("_topic_").toString(),
                    null, //round-robin依次写入所有分区
                        JSON.toJSONString(row)
                    ));
            }

            writeBuffer.clear();//必须清空
        }

        @Override
        public void destroy() {
            if(this.producer !=null){
                this.producer.close();
            }
        }
    }
}
