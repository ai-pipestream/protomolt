package ai.protomolt.proto.mapper;

import java.util.ArrayList;
import java.util.List;

/** Parses the text rule language used by {@link ProtoFieldMapper}. */
public final class TextRuleParser {

    public List<TextMappingRule> parse(List<String> ruleStrings) throws MappingException {
        List<TextMappingRule> rules = new ArrayList<>();
        for (String text : ruleStrings) {
            if (text == null || text.trim().isEmpty()) continue;
            TextMappingRule rule = assignment(text, false);
            if (rule == null) rule = assignment(text, true);
            if (rule == null) rule = clear(text);
            if (rule == null) throw new MappingException("Invalid rule syntax", text);
            rules.add(rule);
        }
        return rules;
    }

    private static TextMappingRule assignment(String text, boolean append) {
        int start = whitespaceEnd(text, 0);
        int end = start;
        while (end < text.length() && !space(text.charAt(end)) && text.charAt(end) != '+'
                && (append || text.charAt(end) != '=')) end++;
        if (end == start) return null;
        int operator = whitespaceEnd(text, end);
        String token = append ? "+=" : "=";
        if (!text.startsWith(token, operator)) return null;
        int rhs = operator + token.length();
        // The old greedy whitespace prefix can backtrack to leave a whitespace-only
        // (but nonempty) dot match. Dot excludes Java's default line terminators.
        int content = whitespaceEnd(text, rhs);
        if (content == text.length()) {
            boolean hasDot = false;
            for (int i = rhs; i < text.length(); i++) hasDot |= !lineTerminator(text.charAt(i));
            if (!hasDot) return null;
        } else {
            int i = content;
            while (i < text.length() && !lineTerminator(text.charAt(i))) i++;
            if (i == content || whitespaceEnd(text, i) != text.length()) return null;
        }
        return new TextMappingRule(text.substring(start, end), text.substring(rhs).trim(),
                append ? TextMappingRule.Operation.APPEND : TextMappingRule.Operation.ASSIGN, text);
    }

    private static TextMappingRule clear(String text) {
        int start = whitespaceEnd(text, 0);
        if (start == text.length() || text.charAt(start) != '-') return null;
        start = whitespaceEnd(text, start + 1);
        int end = start;
        while (end < text.length() && !space(text.charAt(end))) end++;
        if (end == start || whitespaceEnd(text, end) != text.length()) return null;
        return new TextMappingRule(text.substring(start, end), null, TextMappingRule.Operation.CLEAR, text);
    }

    private static int whitespaceEnd(String text, int start) {
        while (start < text.length() && space(text.charAt(start))) start++;
        return start;
    }

    private static boolean space(char c) {
        return c == ' ' || (c >= '\t' && c <= '\r');
    }

    private static boolean lineTerminator(char c) {
        return c == '\n' || c == '\r' || c == '\u0085' || c == '\u2028' || c == '\u2029';
    }
}
