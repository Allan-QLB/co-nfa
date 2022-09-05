package standalone;

import org.yaml.snakeyaml.Yaml;

import java.io.StringReader;
import java.util.Map;

public class YamlUtil {
    public static String dump(String root, Map<String, Object> map) {
        final Yaml yaml = new Yaml();
        return yaml.dump(map.get(root));
    }

    public static <T> T loadAs(String source, Class<T> clazz) {
        final Yaml yaml = new Yaml();
        return yaml.loadAs(new StringReader(source), clazz);
    }
}
