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
        normalizers.put("LV", new RegexBasedNormalizer("(LV-)?([0-9]{4})", "LV-\\2"));
    }

    public static String normalize(String iso2, String postcode) {
        var normalizer = normalizers.get(iso2.toUpperCase());
        return normalizer != null ? normalizer.normalize(postcode) : postcode;
    }

    private static class RegexBasedNormalizer {
        private final Pattern pattern;
        private final String[] separators;
        private final int[] groupIndices;

        public RegexBasedNormalizer(String pattern, String output) {
            this.pattern = Pattern.compile(pattern);
            var groupIndices = new ArrayList<Integer>();
            this.separators = getSeparators(output, groupIndices);
            this.groupIndices = new int[groupIndices.size()];
            for(int i = 0; i< groupIndices.size(); ++i) {
                this.groupIndices[i] = groupIndices.get(i);
            }
        }

        private static String[] getSeparators(String output, ArrayList<Integer> groupIndices) {
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

                    var parsedGroupIndex = Integer.parseInt(groupIndex.toString());
                    if(parsedGroupIndex < result.size()) throw new IllegalArgumentException("wrong group index");

                    groupIndices.add(parsedGroupIndex);
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
            if(matcher.matches()) {
                var result = new StringBuilder(separators[0]);

                for(int i = 0; i < groupIndices.length; ++i) {
                    result.append(matcher.group(groupIndices[i]));
                    result.append(separators[i + 1]);
                }

                return result.toString();
            }

            return postcode;
        }
    }
}
