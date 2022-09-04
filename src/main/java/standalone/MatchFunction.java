package standalone;

import java.util.List;
import java.util.Map;

public interface MatchFunction<IN, OUT> {
    OUT processMatch(final Map<String, List<IN>> match);
}
