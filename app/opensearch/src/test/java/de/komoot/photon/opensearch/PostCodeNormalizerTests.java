package de.komoot.photon.opensearch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PostCodeNormalizerTests {

    @Test
    public void doesNotNormalizePostCodesWithPrefix() {
        String postCodeWithPrefix = "x12345";
        Assertions.assertEquals(postCodeWithPrefix, PostCodeNormalizer.normalize("GR", postCodeWithPrefix));
    }

    @Test
    public void doesNotNormalizePostCodesWithPostfix() {
        String postCodeWithPostfix = "12345x";
        Assertions.assertEquals(postCodeWithPostfix, PostCodeNormalizer.normalize("GR", postCodeWithPostfix));
    }

    @Test
    public void normalizesGreekPostCodes() {
        Assertions.assertEquals("123 45", PostCodeNormalizer.normalize("GR", "12345"));
        Assertions.assertEquals("543 21", PostCodeNormalizer.normalize("GR", "543 21"));
    }

    @Test
    public void normalizesPolishPostCodes() {
        Assertions.assertEquals("12-345", PostCodeNormalizer.normalize("PL", "12345"));
        Assertions.assertEquals("54-321", PostCodeNormalizer.normalize("PL", "54-321"));
    }
}
