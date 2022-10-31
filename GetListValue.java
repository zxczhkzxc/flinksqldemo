package hikversion.flink.functions;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName GetListValue
 * @Description
 * @Author jiangchao10
 * @Date 2020/12/3 10:13
 **/
public class GetListValue  extends ScalarFunction {

    Logger log = LoggerFactory.getLogger(GetListValue.class);

    public int eval(String str) {
        try {
            JSONArray jsonArray = JSONObject.parseArray(str);
            return jsonArray.size();
        }catch (Exception e){
            log.error("UDF input data error,please check data:{}",str);

          return 0;
        }


    }

    public String eval(String str,int idx) {
        try {
            JSONArray jsonArray = JSONObject.parseArray(str);
            if (jsonArray.size() < idx){
                return null;
            }
            return jsonArray.getString(idx-1);
        }catch (Exception e){
            log.error("UDF input data error,please check data,{}",str);
            e.printStackTrace();
        }

        return null;

    }

}
