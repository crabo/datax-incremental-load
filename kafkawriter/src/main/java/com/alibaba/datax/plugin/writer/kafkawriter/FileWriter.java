package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class FileWriter extends Writer {
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
        private static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.Task.class);
        private Configuration writerSliceConfig = null;
        private List<String> columns;
        private int IdFieldIndex = -1; //用于行合并的id列：tradeID
        private int TopicFieldIndex = -1; //写入的‘动态’主题列

        private int batchSize = 1024;
        private JsonParser jsonParser = null;
        int BUFFER_SIZE=1000000;//1MB
        String TOPIC;
        BufferedOutputStream producer=null;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.columns = writerSliceConfig.getList("column", String.class);
            this.IdFieldIndex = writerSliceConfig.getInt("keyIndex",-1);
            this.TopicFieldIndex = writerSliceConfig.getInt("topicIndex",-1);
            this.batchSize = writerSliceConfig.getInt("batchSize",1024);
            this.TOPIC = writerSliceConfig.getString("topic","output-json")+".txt";

            this.jsonParser = writerSliceConfig.getBool("parseArray",false)?
                    new JsonArrayParser(this.IdFieldIndex,this.columns) :
                    new JsonParser(this.IdFieldIndex,this.columns);

            try {
                producer = new BufferedOutputStream(
                        new FileOutputStream(this.TOPIC)
                        //new GZIPOutputStream(new FileOutputStream("es_data.json.gz"))
                        ,BUFFER_SIZE);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
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
                                    TOPIC.replace("%%",
                                            record.getColumn(TopicFieldIndex).asString())
                                    : TOPIC);
                }
            });

            try {
                for(Map<String,Object> row : batch){
                    producer.write(
                        JSON.toJSONString(row)
                            .getBytes("utf-8")
                    );
                    producer.write("\n".getBytes("utf-8"));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            writeBuffer.clear();//必须清空
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
    }
}
