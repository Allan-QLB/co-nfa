package standalone.pattern;

import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class OrPattern<T, F extends T> extends Pattern<T, F> {

    private final List<GroupPattern<T, ? extends T>> patterns = new ArrayList<>();

    public OrPattern(String name, Pattern<T, ? extends T> previous, @Nonnull List<Pattern<T, ? extends T>> innerPatterns,
                     Quantifier.ConsumingStrategy consumingStrategy, AfterMatchSkipStrategy afterMatchSkipStrategy) {
        super(name, previous, consumingStrategy, afterMatchSkipStrategy);
        for (Pattern<T, ? extends T> innerPattern : innerPatterns) {
            patterns.add(new GroupPattern<>(previous, innerPattern, consumingStrategy,
                    previous.getAfterMatchSkipStrategy()));
        }
    }

    public List<GroupPattern<T, ? extends T>> getPatterns() {
        return patterns;
    }
}
