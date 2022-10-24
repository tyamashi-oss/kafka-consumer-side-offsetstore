/*
 *     Copyright org.tyamashi authors.
 *     License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package org.tyamashi.kafka.offsetstore.internals;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OffsetStoreUtils {
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([^}]*)\\}");
    public static String replaceVariable(String template, Map<String, String> variables) {
        Matcher matcher = VARIABLE_PATTERN.matcher(template);
        if(!matcher.find()) {
            return template;
        }

        StringBuffer sb = new StringBuffer();
        do {
            matcher.appendReplacement(sb,
                    Matcher.quoteReplacement(variables.getOrDefault(matcher.group(1), matcher.group(0))));
        } while(matcher.find());

        matcher.appendTail(sb);
        return sb.toString();
    }
}
