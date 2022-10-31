package hikversion.flink.job;

import hikversion.flink.functions.*;
import hikversion.flink.functions.gatewaylogparser.GatewayLogParserFunction;
import hikversion.utils.FileUtils;
import hikversion.utils.StringDfUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkRun {
    static Logger log = LoggerFactory.getLogger(FlinkRun.class);

    public static void main(String[] args) throws Exception {
        String jobName = " flink_default_job_name";
        if (args.length == 2) {
            jobName = args[1];
        }
        log.info("get jobName:{}", jobName);
        List<String> list = FileUtils.readFile(args[0]);


        //获取flink 参数配置
        Properties properties = FileUtils.getProperties("flink.properties");

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        //join优化 设置state 保存时间
        TableConfig tConfig = bsTableEnv.getConfig();
        tConfig.setIdleStateRetentionTime(Time.minutes(10), Time.minutes(30));


        //设置重启策略

        bsEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.of(30, TimeUnit.SECONDS)
        ));
        //设置checkpoint 和 state
        String property = properties.getProperty("checkpoints.dir");
        if (!StringUtils.isBlank(property)){
            bsEnv.setStateBackend(new FsStateBackend(property));
            CheckpointConfig checkpointConfig = bsEnv.getCheckpointConfig();
            checkpointConfig.setCheckpointInterval(1000*60*10);
            checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
            //ck失败容忍次数
            checkpointConfig.setTolerableCheckpointFailureNumber(3);
            // Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
            bsEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        }

        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        bsTableEnv.createTemporaryFunction("unix_time_convert",new UnixTimeConvert());
        bsTableEnv.createTemporaryFunction("data_format_local",new DateFormat());
        bsTableEnv.createTemporaryFunction("get_unix_time", new GetUnixTime());
        bsTableEnv.createTemporaryFunction("get_unix_date",new GetDate());
        bsTableEnv.createTemporaryFunction("time_to_step_index",new TimeToStepIndex());
        bsTableEnv.createTemporaryFunction("get_num_value",new GetNumValue());
        bsTableEnv.createTemporaryFunction("hkx_hash",new MixHash());
        bsTableEnv.createTemporaryFunction("metro_hash64",new FlinkMetroHasher());
        bsTableEnv.createTemporaryFunction("get_list_value",new GetListValue());
        bsTableEnv.createTemporaryFunction("get_json_value",new GetJsonValue());
        bsTableEnv.createTemporaryFunction("gateway_log_parser", (UserDefinedFunction)new GatewayLogParserFunction());
        //udaf注册
        bsTableEnv.registerFunction("get_intersection_count",new IntersectionCount());

        //匹配 BEGIN STATEMENT SET;
         String regx1 = "(BEGIN)\\s*(STATEMENT )\\s*(SET)\\s*";
        StatementSet statementSet = null;
        boolean isBatchSql = false;

        for (String partSql : list) {
            if (!StringUtils.isBlank(partSql)) {
                if (StringDfUtils.isMatch(partSql,regx1) && statementSet == null){
                    statementSet = bsTableEnv.createStatementSet();
                    isBatchSql = true;
                    log.info("开始执行批量插入任务");
                    continue;
                }
                //批量执行结束
                if (StringUtils.isBlank(partSql.toLowerCase().replaceAll("end","")) && isBatchSql && partSql.toLowerCase().contains("end")){
                    System.out.println("结束===============");
                    statementSet.execute();
                    break;
                }
                if (isBatchSql){
                    log.info("执行批量sql：{}",partSql);
                    statementSet.addInsertSql(partSql);
                }else {
                    log.info("执行sql " + partSql);
                    if (partSql.toLowerCase().contains("create view") || partSql.toLowerCase().contains("insert into") || partSql.toLowerCase().contains("create table")){
                        bsTableEnv.executeSql(partSql);
                    }
                    else {
                        log.info("》》》》》》》开始查询数据 》》》》》》》》》" );
                        Table table = bsTableEnv.sqlQuery(partSql);
                        bsTableEnv.toRetractStream(table, Row.class).print("测试数据");
                        bsEnv.execute("测试job");
                    }
                }

            }
        }



    }

}
