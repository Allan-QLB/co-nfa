package standalone;

import cn.hutool.core.collection.CollectionUtil;
import org.yaml.snakeyaml.Yaml;
import standalone.exception.ConfigurationException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

public class ConfigUtil {
    private static final String CONFIG_FILE = "config.yaml";

    public static Map<String, Object> loadConfigAsMap() throws IOException {
        return loadConfigAsMap(CONFIG_FILE);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> loadConfigAsMap(String fileName) throws IOException {
        final InputStream configStream = ClassLoader.getSystemClassLoader().getResourceAsStream(fileName);
        if (configStream == null) {
            throw new ConfigurationException("Config file " + fileName + " not found in classpath");
        }
        final Yaml yaml = new Yaml();
        try (InputStream inputStream = configStream) {
            final Map<String, Object> configMap = yaml.loadAs(inputStream, Map.class);
            if (CollectionUtil.isEmpty(configMap)) {
                return Collections.emptyMap();
            } else {
                return configMap;
            }
        }
    }
}
