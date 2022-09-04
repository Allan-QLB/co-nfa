package standalone;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.streaming.api.windowing.time.Time;
import standalone.source.InputSource;

import java.util.HashMap;
import java.util.List;

public class Demo1 {
    public static void main(String[] args) throws BackendBuildingException, InterruptedException {
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return "a".equals(jsonObject.get("name"));
                    }
                }).followedBy("second").where(
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObject) throws Exception {
                                return "b".equals(jsonObject.get("name"));
                            }
                        }
                ).within(Time.seconds(1));

        StandaloneRunner<Object> runner = StandaloneRunner.create(pattern, match -> {
            List<JSONObject> first = match.get("first");
            List<JSONObject> second = match.get("second");
            return (first.get(0).get("id") + "," + second.get(0).get("id"));
        });

        InputSource inputSource = new InputSource();
        inputSource.run(runner);
        Thread.sleep(100);
        inputSource.send(1000, new HashMap<String, Object>(){{
            put("id", 1);
            put("name", "a");
        }});
        inputSource.send(1999, new HashMap<String, Object>(){{
            put("id", 2);
            put("name", "b");
        }});
    }
}
