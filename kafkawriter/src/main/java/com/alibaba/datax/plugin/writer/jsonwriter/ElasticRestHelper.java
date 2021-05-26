package com.alibaba.datax.plugin.writer.jsonwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class ElasticRestHelper {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticRestHelper.class);

    /**
     * _bulk 提交全部行，并自动提取失败，进行长时delay并重试
     * @param postEntities [index+id, bulkJson]
     */
    public static void postRequest(RestClient conn, Map<String,String> postEntities) throws Exception {
        int retries=0;
        Set<String> fails=null;
        do{
            HttpEntity entity = getPostEntity(postEntities,fails);
            retries ++;
            try {
                fails = doPost(conn,entity);
                if(fails==null || fails.isEmpty())
                    break;//SUCESS
                else {
                    Thread.sleep(3000*retries);
                }
            } catch(IllegalStateException ie){
                String body=EntityUtils.toString(entity, StandardCharsets.UTF_8);
                if(body.length()>1000) body= body.substring(0,1000);
                LOG.error("ES client '_bulk' post with IllegalException:\n{}",body);
                throw ie;
            }
            catch(RuntimeException e){
                if(e.getCause()!=null && e.getCause() instanceof TimeoutException){
                    Thread.sleep(10000*retries);
                    LOG.warn("ES client timeout-error, {}# retrying '{}' docs",retries,postEntities.size());
                }else
                    throw e;
            }catch(ConnectException ce){
                LOG.error("ES client connection refused!!");
                Thread.sleep(30000);
                retries=26;
            }
            catch(IOException ex){
                if(retries>20){//NETWORK ERROR?
                    LOG.warn("ES client IO-error too often, set a smaller 'batchSize' !!");
                    Thread.sleep(2^(retries-20)*1000);
                }
                Thread.sleep(10000*retries);
                LOG.warn("ElasticSearch IO-error on request {}# retrying '{}' docs.\n {}",retries,postEntities.size(),ex);
            }

        }while(retries<28);

        if(fails!=null || !fails.isEmpty()){
            throw new IllegalArgumentException("Too many retries, stop retrying.");
        }
    }
    private static HttpEntity getPostEntity(Map<String,String> postEntities,Set<String> fails){
        if(fails!=null && !fails.isEmpty()){
            int prevCount = postEntities.size();
            Iterator<Map.Entry<String,String>> rows = postEntities.entrySet().iterator();
            while(rows.hasNext()){
                Map.Entry<String,String> kv = rows.next();
                if(! fails.contains(kv.getKey()) )//未包含在fail，则为成功提交。 无需继续重试
                    rows.remove();
            }
            LOG.info("ES client retrying '{}' docs with skipped '{}' success docs",
                    fails.size(), prevCount - fails.size());
        }

        StringBuilder data = new StringBuilder();
        for(String row : postEntities.values()){
            data.append(data); //均已换行
        }
        //return getGzipEntity(data);
        return new NStringEntity(data.toString(), ContentType.APPLICATION_JSON);
    }

    /**
     *
     * @return null： 提交全部成功， Set[]：返回失败的'_index+_id'
     */
    private static Set<String> doPost(RestClient conn, HttpEntity entity)throws IOException {
        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setEntity(entity);

        Response resp = conn.performRequest(bulkRequest);
        return extractFailedInBulk(resp);
    }

    /**
 {
     "took": 486,
     "errors": true,
     "items": [
     {
         "update": {
             "_index": "index1",
             "_type" : "_doc",
             "_id": "5",
             "status": 404,
             "error": {
                 "type": "document_missing_exception",
                 "reason": "[_doc][5]: document missing",
                 "index_uuid": "aAsFqTI0Tc2W0LCWgPNrOA",
                 "shard": "0",
                 "index": "index1"
             }
        }
     },
     * Map<_id,_index>
     */
    private static Set<String> extractFailedInBulk(Response resp) throws ParseException, IOException{
        String result = EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);

        if(resp.getStatusLine().getStatusCode()>HTTP_STATUS_OK
                || result.indexOf("\"errors\":true")>0)
        {
            Set<String> failedBatch = new HashSet<>();
            JSONObject obj = JSON.parseObject(result);
            JSONArray items = obj.getJSONArray("items");
            if(items!=null){
                for(int i=0;i<items.size();i++){
                    JSONObject op = items.getJSONObject(i);
                    obj = op.containsKey("index")?
                            op.getJSONObject("index")
                            :op.getJSONObject("update");

                    if(obj.getIntValue("status")>HTTP_STATUS_OK){
                        if(failedBatch.isEmpty()) {//记录首个错误
                            LOG.info("ElasticSearch '_bulk' post failed: {}",
                                    op.toJSONString());
                        }
                        if(op.containsKey("update") && obj.getString("_id").isEmpty()) {
                            LOG.error("'_id' is null in '_bulk' update! {} ",op);
                        }else
                            failedBatch.add(obj.getString("_index")+obj.getString("_id"));
                    }
                }
            }

            if(failedBatch.isEmpty())
                LOG.warn("ES client '_bulk' post failed with no items return, errors: \n{}",
                        result
                );

            return failedBatch;
        }
        return null;
    }
    static int HTTP_STATUS_OK=201;

    /**-----------------------_bulk message--------------------------------*/
    public static String recordToBulkEntity(Record record,Map<String, Object> row,
                      String writeMode,List<String> columns,String topic){
        StringBuilder sb = new StringBuilder();
        //action&meta
        sb.append("{\"").append(writeMode).append("\":");
        Map<String, Object> meta = createMeta(record,columns);
        meta.put("_index", topic);
        if("update".equals(writeMode)) {
            meta.put("retry_on_conflict", 2);
        }
        sb.append(JSON.toJSONString(meta));
        sb.append("}\n");

        //data
        if("update".equals(writeMode)){
            sb.append("{\"doc\":");
            sb.append(JSON.toJSONString(row));
            sb.append(",\"doc_as_upsert\":true}");
        }else{
            sb.append(JSON.toJSONString(row));
        }
        sb.append("\n");

        return sb.toString();
    }
    private static Map<String,Object> createMeta(Record record,List<String> columns){
        Map<String,Object> meta=new HashMap<>();

        for(int i=0;i< columns.size();i++){
            String colName = columns.get(i);
            if(!colName.startsWith("_"))//连续的下划线字段，将一一进入meta
                break;
            else{
                if(colName.startsWith("_-"))//忽略该字段，meta、data均排除
                    continue;;
                if(colName.startsWith("_+"))//进入meta，但仅保留+以后的字符
                    colName=colName.substring(2);//非下划线的metaName

                meta.put(colName,record.getColumn(i).getRawData());
            }
        }

        return meta;
    }
}
