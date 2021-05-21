package com.alibaba.datax.core.job.scheduler.batchmode;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;

public class BatchArgsReader {
    private static String BATCH_SETTING_PATH = "job.batchSetting.";
    private static String JOB_CONTENT_PATH = "job.content[0].";
    private Configuration jsonRoot;//初始化json原始副本
    String batchTsKey;//当前jdbc任务组id
    private List<Object> batchNames;
    List<Object[]> batchArgs;
    public BatchArgsReader(Configuration cfg,String tsKey){
        if(tsKey==null) throw new IllegalArgumentException("任务批次 ‘-DtsKey=...’ 必须在启动命令行配置！");
        jsonRoot = cfg;
        batchTsKey = tsKey;

        batchNames = cfg.getList(BATCH_SETTING_PATH+"names");
        batchArgs = getBatchArgs(tsKey);
        if(batchArgs.isEmpty()) throw new IllegalArgumentException("‘jdbc.batchArgs’批量参数为空，请检查配置！");
        if(batchArgs.get(0).length!=batchNames.size())
            throw new IllegalArgumentException("‘jdbc.batchArgs’替换参数名与参数个数不一致，请重新配置！");
    }

    public int getChannelNum(){
        return jsonRoot.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL);
    }
    public int getSleepIntervalMS(){
        return 1000 * jsonRoot.getInt(BATCH_SETTING_PATH+"ts_interval_sec",0);
    }

    private List<Object[]> getBatchArgs(String tsKey){
        ArrayList<Object[]> li;
        if(jsonRoot.getMap(BATCH_SETTING_PATH+"jdbc")!=null){
            li = querySql(jsonRoot.getString(BATCH_SETTING_PATH+"jdbc.batchArgs"),tsKey);
        }else{
            li = new ArrayList<>();
            List<Object> args = jsonRoot.getList(BATCH_SETTING_PATH+"args");
            if(args!=null){
                for(Object row : args){
                    li.add((Object[]) row);
                }
            }
        }
        return li;
    }

    void applyBatchArgs(Configuration cfg, Object[] args){
        for(int i=0;i<batchNames.size();i++){
            //$r,$w 作为parameter路径缩写
            //reader.parameter.connection[]数组已向上合并到parameter内。
            //如： $r.jdbcUrl
            String path = JOB_CONTENT_PATH+batchNames.get(i).toString()
                        .replace("$r.","reader.parameter.")
                        .replace("$w.","writer.parameter.");
            cfg.set(path,args[i]);
            //int iKey = path.lastIndexOf(".");
            //cfg.getConfiguration(path.substring(0,iKey)).set(path.substring(iKey+1),args[i]);
        }
    }

    Object[] queryTsValues(){
        List<Object[]> rs = querySql(jsonRoot.getString(BATCH_SETTING_PATH+"jdbc.querySql"),batchTsKey);
        Object[] tsValues = rs.get(0);
        if(tsValues[0]==null) tsValues[0]=TS_FORMAT.format(new Date());
        //重新设置tsEnd为计算后取值
        tsValues[1] = calcBatchEndTs(jsonRoot,tsValues);
        CURRENT_TS_VALUES = tsValues;
        return tsValues;
    }
    public Object[] CURRENT_TS_VALUES;
    void applyTsToReader(Configuration cfg,Object[] tsValues){
        //parameter.connection[0].querySql 内配置项已被上移
        String jsonPath = JOB_CONTENT_PATH+"reader.parameter.querySql";
        String sql = cfg.getString(jsonPath);
        cfg.set(jsonPath,
                parepareBatchEndSql(cfg,sql)
                    .replace("$ts_start",tsValues[0].toString())
                    .replace("$ts_end",tsValues[1].toString())
            );
    }
    void updateJobStatus(String tsEnd,String error){
        if(error==null || error.isEmpty()){
            updateSql(jsonRoot.getString(BATCH_SETTING_PATH+"jdbc.updateSql"),batchTsKey,
                    tsEnd);
        }else{
            updateSql(jsonRoot.getString(BATCH_SETTING_PATH+"jdbc.updateErr"),batchTsKey,
                    error.length()>=250 ? error.substring(0,240): error);
        }
    }

    private static SimpleDateFormat TS_FORMAT=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private String calcBatchEndTs(Configuration cfg,Object[] tsValues){
        String tsStart = tsValues[0].toString();
        int minBatch = cfg.getInt(BATCH_SETTING_PATH+"ts_batch_mins", 0);
        if(tsStart.indexOf(":")>0){
            Date tsNext = null;
            Date tsEnd = null;
            try {
                tsNext = new Date(
                    TS_FORMAT.parse(tsStart).getTime() + minBatch*60000
                );
                tsEnd = TS_FORMAT.parse(tsValues[1].toString());
            } catch (ParseException e) {
                e.printStackTrace();
            }
            //下一批次时间 小于 截止时间，则使用批次时间
            return tsNext.before(tsEnd)?TS_FORMAT.format(tsNext):TS_FORMAT.format(tsEnd);
        }else{//ts_start,ts_end 也可以为id字段，按minBatch的数值间隔调度
            Long tsNext = Long.parseLong(tsStart) + minBatch;
            Long tsEnd = Long.parseLong(tsValues[1].toString());
            return tsNext<tsEnd ? tsNext.toString() : tsEnd.toString();
        }
    }

    private static java.util.regex.Pattern REGEX_TS_START = java.util.regex.Pattern.compile("(\\w*\\.+\\w*)\\W*\\$ts_start");
    private String parepareBatchEndSql(Configuration cfg,String sql) {
        if (cfg.getInt(BATCH_SETTING_PATH+"ts_batch_mins", 0) > 0 //不存在$ts_end???
                && sql.indexOf("$ts_end") < 0 && sql.indexOf("$ts_start") > 0) {

            //sql offset字段必须带有别名： a.syncTime>'$ts_start'
            Matcher m = REGEX_TS_START.matcher(sql);
            if (m.find()) {
                String criteria = " and " + m.group(1) + " <= '$ts_end' ";

                sql = sql.replace("'$ts_start'", "'$ts_start'" + criteria);
            }
        }
        return sql;
    }

    private Connection getConn() throws SQLException {
        String jdbcUrl = jsonRoot.getString(BATCH_SETTING_PATH+"jdbc.jdbcUrl");
        String uid = jsonRoot.getString(BATCH_SETTING_PATH+"jdbc.username");
        String pwd = jsonRoot.getString(BATCH_SETTING_PATH+"jdbc.password");
        try {Class.forName("com.mysql.jdbc.Driver");}
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(jdbcUrl, uid, pwd);
    }
    private ArrayList<Object[]> querySql(String sql,String tsKey){
        ArrayList<Object[]> li = new ArrayList<>();
        Connection conn=null;
        try {
            conn = getConn();
            ResultSet rs =conn.createStatement()
                    .executeQuery(sql.replace("$ts_key", tsKey));

            int colCount = rs.getMetaData().getColumnCount();
            while(rs.next()) {
                Object[] row = new Object[colCount];
                li.add(row);
                for(int i=1;i<=colCount;i++){
                    row[i-1] = rs.getObject(i);
                }
            }
        }catch(SQLException e){
            e.printStackTrace();
        }finally{
            if(conn!=null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }
        return li;
    }
    private Boolean updateSql(String sql,String tsKey,String tsValue){
        Connection conn=null;
        int affected=0;
        try {
            conn = getConn();
            affected =conn.createStatement().executeUpdate(sql
                    .replace("$ts_key", tsKey)
                    .replace("$ts_value", tsValue));
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
            if(conn!=null)
                try {
                    conn.close();
                } catch (Exception e) {
                }
        }
        return affected > 0;
    }
}
