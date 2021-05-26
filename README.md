# datax-incremental-load
Improve alibaba/datax 3.0 for "continurous"  running in a incremental mode

------
#### 1. 新增特性
- [x] 修改为多个读写任务配置: 依次运行job.content[0], job.content[1],job.content[2] ...
- [x] 增加批量运行模式，读取批次参数，逐个动态替换reader/writer parameter， 实现同一个表名 **“跨库”、“跨租户”** 运行。 如table1的同步任务自动切换为： rds1.table1->writer, rds2.table1->writer, rds3.table1->writer ...
- [x] 增加时间戳“增量模式”，根据sql配置，在单个时间戳范围[ts_start,ts_end]内运行 以上所有多任务、多批次。 完成后更新**时间戳** ,适当休眠后继续读取sql时间戳循环运行。
- [x] 实现进程不退出的准“实时增量读取” ,动态适时休眠后按新时间戳调度。


#### 2. 新增job.batchSetting配置项
```json5
{"job":
    {
    "setting":{...},
    "content":[{"jobgroup1"},{"jobgroup2"}], //增强为多组任务调度
    
    "batchSetting": {
    
        "ts_batch_mins":3600,  //每批次ts_start,ts_end最大间隔（避免rds拉取卡死）
        "ts_interval_sec":30,  //所有批次结束后，是否继续休眠后运行？
    // 设置每个批次需要替换的 task reader/writer参数名 $r为reader.paramter路径简写
     "names":["$r.username","$w.jdbcUrl"],
    // 1. 手动设置批次参数
     "args":[
        ["root","jdbc:mysql://127.0.0.1:4000/test"]
     ],

     // 2. 按jdbc连接自动设置批次参数
     "jdbc":{
         // 按外部传入的$ts_key参数，获取当前批次参数
         "batchArgs":"select uid, jdbc from datax_task where groupid='$ts_key' ",
         // 获取任务时间戳ts_start,服务端当前时间ts_end
         "querySql":"select ts_offset as ts_start, now() as ts_end from datax_task where groupid='$ts_key' limit 1",
         // 成功任务更新时间戳，失败任务更新任务状态
         "updateSql":"update datax_task set ts_offset=$ts_value,ts_err='',end_time=now() where groupid='$ts_key' ",
         "updateErr":"update datax_task set ts_error=$ts_value,end_time=now() where groupid='$ts_key' ",
         "username": "root",
         "password": "*",
         "jdbcUrl": "jdbc:mysql://127.0.0.1:3306/test?useSSL=false"
     }
 }
```
#### 3. 编译与运行  
1）. 合并当前代码到datax core模块 （仅新增）  
2）. 修改com.alibaba.datax.core.job.JobContainer 为 protected initStandaloneScheduler()， 从而允许BatchJobContainer的子类重载。  
3）. 修改com.alibaba.datax.core.Engine 59行的container = new BatchJobContainer(allConf); 将JobContainer实例替换为BatchJobContainer。  
4） 运行： java -cp... -Ddatax.home=./datax -DtsKey=_batch_id_for_sql_ -mode standalone -job job/test.json  
