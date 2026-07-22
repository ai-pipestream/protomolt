package ai.pipestream.proto.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the text rule language used by {@link ProtoFieldMapper}.
 */
public final class TextRuleParser {

    private static final Pattern ASSIGN_PATTERN = Pattern.compile("^\\s*([^=+\\s]+)\\s*=\\s*(.+)\\s*$");
    private static final Pattern APPEND_PATTERN = Pattern.compile("^\\s*([^+\\s]+)\\s*\\+=\\s*(.+)\\s*$");
    private static final Pattern CLEAR_PATTERN = Pattern.compile("^\\s*-\\s*(\\S+)\\s*$");

    public List<TextMappingRule> parse(List<String> ruleStrings) throws MappingException {
        List<TextMappingRule> rules = new ArrayList<>();
        for (String ruleString : ruleStrings) {
            if (ruleString == null || ruleString.trim().isEmpty()) {
                continue;
            }
            Matcher assignMatcher = ASSIGN_PATTERN.matcher(ruleString);
            if (assignMatcher.matches()) {
                rules.add(new TextMappingRule(assignMatcher.group(1), assignMatcher.group(2).trim(),
                        TextMappingRule.Operation.ASSIGN, ruleString));
                continue;
            }
            Matcher appendMatcher = APPEND_PATTERN.matcher(ruleString);
            if (appendMatcher.matches()) {
                rules.add(new TextMappingRule(appendMatcher.group(1), appendMatcher.group(2).trim(),
                        TextMappingRule.Operation.APPEND, ruleString));
                continue;
            }
            Matcher clearMatcher = CLEAR_PATTERN.matcher(ruleString);
            if (clearMatcher.matches()) {
                rules.add(new TextMappingRule(clearMatcher.group(1), null,
                        TextMappingRule.Operation.CLEAR, ruleString));
                continue;
            }
            throw new MappingException("Invalid rule syntax", ruleString);
        }
        return rules;
    }
}
