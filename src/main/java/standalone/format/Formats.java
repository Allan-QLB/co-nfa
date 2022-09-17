package standalone.format;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class Formats {
    public static final Format<JSONObject> JSON_FORMAT = data -> (JSONObject) JSON.toJSON(data);
}
