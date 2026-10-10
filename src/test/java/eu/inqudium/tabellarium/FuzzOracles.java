package eu.inqudium.tabellarium;

/**
 * Oracle helpers shared by the fuzz targets. Each one re-states a rule of
 * the library in plain Java, so an invariant compares the library against
 * an independent spelling of the same rule.
 */
final class FuzzOracles {

    private FuzzOracles() {
    }

    /**
     * Kotlin's isBlank(), which the library uses: a char is whitespace per
     * Character.isWhitespace OR isSpaceChar - NBSP counts, unlike in Java's
     * String.isBlank(). The oracle must speak the library's dialect.
     */
    static boolean kotlinBlank(String s) {
        return s.codePoints().allMatch(c -> Character.isWhitespace(c) || Character.isSpaceChar(c));
    }
}
