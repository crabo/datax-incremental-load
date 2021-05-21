package com.alibaba.datax.core.job.scheduler.batchmode;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.taskgroup.runner.TaskGroupContainerRunner;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.fastjson.JSONArray;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 检测job.batchSetting配置项
 "batchSetting": {
     "ts_batch_mins":3600,  --每批次ts_start,ts_end最大间隔（避免rds拉取卡死）
     "ts_interval_sec":30,  --所有批次结束后，是否继续休眠后运行？
    -- 设置每个批次需要替换的 task reader/writer参数名
     "names":["reader.username","writer.connection.jdbcUrl"],
    -- 1. 手动设置批次参数
     "args":[
        ["root","jdbc:mysql://127.0.0.1:4000/test"]
     ],

     -- 2. 按jdbc连接自动设置批次参数
     "jdbc":{
         -- 按外部传入的$ts_key参数，获取当前批次参数
         "batchArgs":"select uid, jdbc from datax_task where groupid='$ts_key' ",
         -- 获取任务时间戳ts_start,服务端当前时间ts_end
         "querySql":"select ts_offset as ts_start, now() as ts_end from datax_task where groupid='$ts_key' limit 1",
         -- 成功任务更新时间戳，失败任务更新任务状态
         "updateSql":"update datax_task set ts_offset=$ts_value,ts_err='',end_time=now() where groupid='$ts_key' ",
         "updateErr":"update datax_task set ts_error=$ts_value,end_time=now() where groupid='$ts_key' ",
         "username": "root",
         "password": "*",
         "jdbcUrl": ["jdbc:mysql://127.0.0.1:3306/test?useSSL=false"
     }
 }
 */
public class BatchScheduler extends AbstractScheduler {
    private ExecutorService taskGroupContainerExecutorService;
    private BatchArgsReader batchArgsReader;
    public BatchScheduler(AbstractContainerCommunicator containerCommunicator,BatchArgsReader argsReader) {
        super(containerCommunicator);
        batchArgsReader = argsReader;
    }

    @Override
    protected boolean isJobKilling(Long jobId) {
        return false;
    }

    @Override
    public void startAllTaskGroup(List<Configuration> configurations) {
        this.taskGroupContainerExecutorService =
                new ThreadPoolExecutor(batchArgsReader.getChannelNum(), batchArgsReader.getChannelNum(),
                        1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(batchArgsReader.getChannelNum()*4),
                        new ThreadPoolExecutor.CallerRunsPolicy());

        //2. 所有子任务均按当前offset范围执行
        int iBatch=0;
        for(Object[] args : batchArgsReader.batchArgs) {
            for (Configuration taskGroupCfg : configurations) {
                Configuration batchCfg = taskGroupCfg.clone();//新增副本
                batchCfg.set("job.content[0].taskId",iBatch);
                batchArgsReader.applyBatchArgs(batchCfg,args);
                batchArgsReader.applyTsToReader(batchCfg,batchArgsReader.CURRENT_TS_VALUES);

                TaskGroupContainerRunner runner = newTaskGroupContainerRunner(batchCfg);
                this.taskGroupContainerExecutorService.execute(runner);
            }
            iBatch++;
        }

        this.taskGroupContainerExecutorService.shutdown();
    }

    @Override
    public void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable) {
        this.taskGroupContainerExecutorService.shutdownNow();
        batchArgsReader.updateJobStatus(null,throwable.getMessage());
        throw DataXException.asDataXException(
                FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
    }


    @Override
    public void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks) {
        //通过进程退出返回码标示状态
        this.taskGroupContainerExecutorService.shutdownNow();
        batchArgsReader.updateJobStatus(null,"Jobs got killed");
        throw DataXException.asDataXException(FrameworkErrorCode.KILLED_EXIT_VALUE,
                "job killed status");
    }


    private TaskGroupContainerRunner newTaskGroupContainerRunner(
            Configuration configuration) {
        TaskGroupContainer taskGroupContainer = new TaskGroupContainer(configuration);

        return new TaskGroupContainerRunner(taskGroupContainer);
    }
}
