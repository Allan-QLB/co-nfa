package standalone;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import standalone.source.InputSource;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class Demo1 {
    public static void main(String[] args) throws Exception {
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
                ).within(Time.seconds(200));

        InputSource inputSource = new InputSource();
        final StandaloneRunner<String> stringStandaloneRunner = StandaloneRunner.create(pattern, match -> {
            List<JSONObject> first = match.get("first");
            List<JSONObject> second = match.get("second");
            return (first.get(0).get("id") + "," + second.get(0).get("id"));
        }, inputSource);
        stringStandaloneRunner.start();

        inputSource.send(1000, new HashMap<String, Object>(){{
            put("id", 1);
            put("name", "a");
        }});

        for (int i = 0; i < 100000; i++) {

            inputSource.send(1000 + i, new HashMap<String, Object>(){{
                put("id", 3);
                put("name", UUID.randomUUID().toString());
            }});
        }

        inputSource.send(120000, new HashMap<String, Object>(){{
            put("id", 2);
            put("name", "b");
        }});


        //Thread.sleep(40000);
        //stringStandaloneRunner.shutdown();
    }
}
