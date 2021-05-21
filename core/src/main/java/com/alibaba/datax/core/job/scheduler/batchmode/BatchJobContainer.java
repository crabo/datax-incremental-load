package com.alibaba.datax.core.job.scheduler.batchmode;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.job.scheduler.processinner.StandAloneScheduler;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.communicator.job.StandAloneJobContainerCommunicator;
import com.alibaba.datax.core.util.container.CoreConstant;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class BatchJobContainer extends JobContainer {
    private Configuration rootCfg;
    AbstractContainerCommunicator containerCommunicator;
    AbstractScheduler scheduler;

    private BatchArgsReader batchArgsReader;
    public BatchJobContainer(Configuration configuration) {
        super(configuration);
        rootCfg = configuration;

        containerCommunicator = new StandAloneJobContainerCommunicator(rootCfg);
        batchArgsReader =  new BatchArgsReader(rootCfg,System.getProperty("tsKey"));
        scheduler = new BatchScheduler(containerCommunicator,batchArgsReader);
    }

    @Override
    public void start() {
        int sleelpMS = batchArgsReader.getSleepIntervalMS();
        List<Object> taskGroups = rootCfg.getList(CoreConstant.DATAX_JOB_CONTENT);

        if(sleelpMS > 1000){
            while (true){
                Long start = System.currentTimeMillis();

                runOneBatch(taskGroups); // <----------

                Long ellapsed = System.currentTimeMillis()-start;
                if(sleelpMS>ellapsed){
                    try {
                        TimeUnit.MILLISECONDS.sleep(sleelpMS-ellapsed);
                    } catch (InterruptedException e) {break;}
                }
            }
        } else {
            runOneBatch(taskGroups);
        }
    }

    private void runOneBatch(List<Object> taskGroups){
        if(taskGroups.size() >1 ){
            //1. minBatch重新计算当前批次offset范围（防止单次拉取数据过多）
            Object[] tsValues = batchArgsReader.queryTsValues();

            for(int i=0;i<taskGroups.size();i++){
                List<Object> runGroup = new ArrayList<>();
                runGroup.add(taskGroups.get(i));//依次取出group，由底层jobContainer执行
                super.configuration.set(CoreConstant.DATAX_JOB_CONTENT,runGroup);
                super.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID,i);
                super.start();
            }

            //3. 本批次执行完成，更新offset
            batchArgsReader.updateJobStatus(tsValues[1].toString(),null);
        }else{
            super.start();
        }
    }

    @Override
    protected AbstractScheduler initStandaloneScheduler(Configuration configuration) {
        //AbstractContainerCommunicator containerCommunicator = new StandAloneJobContainerCommunicator(configuration);
        super.setContainerCommunicator(this.containerCommunicator);

        if(configuration.get("job.batchSetting")==null)
            return new StandAloneScheduler(containerCommunicator);
        else {
            return this.scheduler;
        }
    }
}
