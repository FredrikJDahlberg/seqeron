package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agrona.SemanticVersion;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Which logs a node starts on (doc/snapshot.md §10.2). */
class LogVersionTest {
    @Test
    @DisplayName("a log of this build's major, or of Aeron's default major, is accepted; any other is not")
    void onlyThisMajorAndTheDefaultAreAccepted() {
        assertTrue(LogVersion.VALIDATOR.isVersionCompatible(LogVersion.APP_VERSION, LogVersion.APP_VERSION));
        assertTrue(LogVersion.VALIDATOR.isVersionCompatible(LogVersion.APP_VERSION, SemanticVersion.compose(1, 7, 3)));
        assertTrue(LogVersion.VALIDATOR.isVersionCompatible(LogVersion.APP_VERSION, SemanticVersion.compose(0, 0, 1)),
                   "a log from before there was a version");
        assertFalse(LogVersion.VALIDATOR.isVersionCompatible(LogVersion.APP_VERSION, SemanticVersion.compose(2, 0, 0)));
    }

    @Test
    @DisplayName("the version this build writes is of its own major")
    void theWrittenVersionIsOfItsOwnMajor() {
        org.junit.jupiter.api.Assertions.assertEquals(LogVersion.LOG_VERSION,
                                                      SemanticVersion.major(LogVersion.APP_VERSION));
    }
}
