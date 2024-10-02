package de.komoot.photon.opensearch;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class PostCodeNormalizer {

    private static final HashMap<String, RegexBasedNormalizer> normalizers;

    static
    {
        normalizers = new HashMap<>();
        normalizers.put("GR", new RegexBasedNormalizer("([0-9]{3}) ?([0-9]{2})", "\\1 \\2"));
        normalizers.put("PL", new RegexBasedNormalizer("([0-9]{2})-?([0-9]{3})", "\\1-\\2"));
        normalizers.put("SE", new RegexBasedNormalizer("([0-9]{3}) ?([0-9]{2})", "\\1 \\2"));
        normalizers.put("SK", new RegexBasedNormalizer("([0-9]{3}) ?([0-9]{2})", "\\1 \\2"));
        normalizers.put("IR", new RegexBasedNormalizer("([0-9]{5}) ?([0-9]{5})", "\\1 \\2"));
    }

    public static String normalize(String iso2, String postcode) {
        var normalizer = normalizers.get(iso2.toUpperCase());
        return normalizer != null ? normalizer.normalize(postcode) : postcode;
    }

    private static class RegexBasedNormalizer {
        private final Pattern pattern;
        private final String[] separators;

        public RegexBasedNormalizer(String pattern, String output) {
            this.pattern = Pattern.compile(pattern);
            this.separators = getSeparators(output);
        }

        private static String[] getSeparators(String output) {
            var result = new ArrayList<String>();

            int pos = 0;
            int lastEnd = 0;
            while (pos < output.length()) {
                var character = output.charAt(pos);

                if (character == '\\') {
                    result.add(output.substring(lastEnd, pos));
                    var groupIndex = new StringBuilder();
                    ++pos;
                    while (pos < output.length() && Character.isDigit(output.charAt(pos))) {
                        groupIndex.append(output.charAt(pos));
                        ++pos;
                    }

                    if(Integer.parseInt(groupIndex.toString()) != result.size()) throw new IllegalArgumentException("wrong group index");

                    lastEnd = pos;
                }
                else {
                    ++pos;
                }
            }

            result.add(output.substring(lastEnd));

            return result.toArray(new String[result.size()]);
        }

        public String normalize(String postcode)
        {
            var matcher = pattern.matcher(postcode);
            if(matcher.matches() && matcher.groupCount() + 1 == separators.length) {
                var result = new StringBuilder(separators[0]);

                for(int i = 1; i <= matcher.groupCount(); ++i) {
                    result.append(matcher.group(i));
                    result.append(separators[i]);
                }

                return result.toString();
            }

            return postcode;
        }
    }
}
