package dev.prita.redrive.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HmacSignerTest {

    private final HmacSigner signer = new HmacSigner();

    @Test
    void signatureIsDeterministicForSameInputs() {
        var a = signer.sign("whsec_test", 1700000000L, "{\"x\":1}");
        var b = signer.sign("whsec_test", 1700000000L, "{\"x\":1}");
        assertThat(a).isEqualTo(b).startsWith("sha256=");
    }

    @Test
    void signatureChangesWithBody() {
        var a = signer.sign("whsec_test", 1700000000L, "{\"x\":1}");
        var b = signer.sign("whsec_test", 1700000000L, "{\"x\":2}");
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void signatureChangesWithTimestamp_replayProtection() {
        var a = signer.sign("whsec_test", 1700000000L, "{\"x\":1}");
        var b = signer.sign("whsec_test", 1700000001L, "{\"x\":1}");
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void signatureChangesWithSecret() {
        var a = signer.sign("whsec_one", 1700000000L, "{\"x\":1}");
        var b = signer.sign("whsec_two", 1700000000L, "{\"x\":1}");
        assertThat(a).isNotEqualTo(b);
    }
}
