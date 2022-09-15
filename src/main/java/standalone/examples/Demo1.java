package standalone.examples;

import cep.nfa.compiler.NFACompiler;
import cep.nfa.util.NfaUtil;
import com.alibaba.fastjson.JSONObject;
import cep.pattern.Pattern;
import cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import standalone.StandaloneRunner;
import standalone.source.InputSource;

import java.util.HashMap;
import java.util.List;

public class Demo1 {
    public static void main(String[] args) throws Exception {
        Pattern<JSONObject, JSONObject> or1 = Pattern.<JSONObject>begin("second")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return "b".equals(jsonObject.get("name"));
                    }
                });

        Pattern<JSONObject, JSONObject> or2 = Pattern.<JSONObject>begin("second")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return "c".equals(jsonObject.get("name"));
                    }
                });

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return "a".equals(jsonObject.get("name"));
                    }
                })
//                .followedBy(or1).within(Time.seconds(500));
                .followedByOr("or1", or1, or2)
                .within(Time.seconds(500));

        NfaUtil.drawGraphic(NFACompiler.compileFactory(Pattern.beginByOr("xx", or1, or2, pattern), false).createNFA());

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


        inputSource.send(220000, new HashMap<String, Object>(){{
            put("id", 2);
            put("name", "c");
        }});

        inputSource.send(220001, new HashMap<String, Object>(){{
            put("id", 3);
            put("name", "b");
        }});

        System.out.println("finish");

    }
}
