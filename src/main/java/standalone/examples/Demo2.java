package standalone.examples;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import cep.nfa.aftermatch.AfterMatchSkipStrategy;
import cep.pattern.Pattern;
import cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import standalone.StandaloneRunner;
import standalone.source.InputSource;

import java.util.List;

public class Demo2 {
    public static void main(String[] args) throws Exception {
        final Pattern<JSONObject, JSONObject> begin = Pattern.<JSONObject>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<JSONObject>() {
                           @Override
                           public boolean filter(JSONObject event) throws Exception {
                               return  new JSONPath("$.tag").eval(event).equals("regulator")
                                       && new JSONPath("$.event.action").eval(event).equals("on");
                           }
                       }
                );

        final Pattern<JSONObject, JSONObject> alt1 = Pattern.<JSONObject>begin("second").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject event) throws Exception {
                return new JSONPath("$.tag").eval(event).equals("regulator")
                        &&  new JSONPath("$.event.action").eval(event).equals("off");
            }
        });

        final Pattern<JSONObject, JSONObject> alt2 = Pattern.<JSONObject>begin("second").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject event) throws Exception {
                return new JSONPath("$.tag").eval(event).equals("roomKey") && new JSONPath("$.event.action").eval(event).equals("removed");
            }
        });

        final Pattern<JSONObject, JSONObject> pattern = begin.followedByOr("or", alt1, alt2).within(Time.seconds(10));

        InputSource inputSource = new InputSource();
        final StandaloneRunner<String> stringStandaloneRunner = StandaloneRunner.create(pattern, match -> {
            List<JSONObject> first = match.get("first");
            List<JSONObject> second = match.get("second");
            final JSONPath jsonPath = new JSONPath("$.event.id");
            return String.join(",",
                    jsonPath.eval(first.get(0)).toString(),
                    jsonPath.eval(first.get(first.size() - 1)).toString(),
                    jsonPath.eval(second.get(0)).toString());
        }, inputSource);
        stringStandaloneRunner.start();

        for (int i = 0; i < 2000; i++) {
            //System.out.println(i);
            inputSource.send(1000 + i, new TagEvent("regulator", new Regulator("a" + i, 10, 5, "on")));
        }


//        inputSource.send(1005L,new TagEvent("regulator", new Regulator("a1", 10, 5, "on")));
//        inputSource.send(1007L,new TagEvent("regulator", new Regulator("a1_1", 10, 5, "on")));
//        inputSource.send(1008L,new TagEvent("regulator", new Regulator("a1_2", 10, 5, "on")));
        inputSource.send(11_000L,new TagEvent("roomKey", new RoomKey("b1",  10, 5, "removed")));
//
//        inputSource.send(11_004L,new TagEvent("regulator", new Regulator("a2_0", 10, 5, "on")));
//        inputSource.send(11_005L, new TagEvent("regulator", new Regulator("a2", 10, 5, "on")));
//        inputSource.send(11_006L,new TagEvent("regulator", new Regulator("a3", 10, 5, "on")));
//        inputSource.send(11_007L,new TagEvent("regulator", new Regulator("a4", 10, 5, "on")));
//        inputSource.send(11009L,new TagEvent("regulator", new Regulator("b2",  10, 5, "off")));

    }
}

class TagEvent {
    private final String tag;
    private final Object event;

    public TagEvent(String tag, Object event) {
        this.tag = tag;
        this.event = event;
    }

    public Object getEvent() {
        return event;
    }

    public String getTag() {
        return tag;
    }


}

class Regulator {
    private final String id;
    private final long deviceId;
    private final int roomNo;
    private final String action;

    public Regulator(String id, long deviceId, int roomNo, String action) {
        this.id = id;
        this.deviceId = deviceId;
        this.roomNo = roomNo;
        this.action = action;
    }

    public String getId() {
        return id;
    }

    public long getDeviceId() {
        return deviceId;
    }

    public int getRoomNo() {
        return roomNo;
    }

    public String getAction() {
        return action;
    }

    @Override
    public String toString() {
        return "Regulator{" +
                "id='" + id + '\'' +
                ", deviceId=" + deviceId +
                ", roomNo=" + roomNo +
                ", action='" + action + '\'' +
                '}';
    }
}

class RoomKey {
    private final String id;
    private final long deviceId;
    private final int roomNo;
    private final String action;

    public RoomKey(String id, long deviceId, int roomNo, String action) {
        this.id = id;
        this.deviceId = deviceId;
        this.roomNo = roomNo;
        this.action = action;
    }

    public String getId() {
        return id;
    }

    public long getDeviceId() {
        return deviceId;
    }

    public int getRoomNo() {
        return roomNo;
    }

    public String getAction() {
        return action;
    }

    @Override
    public String toString() {
        return "RoomKey{" +
                "id='" + id + '\'' +
                ", deviceId=" + deviceId +
                ", roomNo=" + roomNo +
                ", action='" + action + '\'' +
                '}';
    }
}
