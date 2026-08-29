# FetchContent PATCH_COMMAND for Aeron — corrects the declared OUTPUTs of the C archive codec
# generation. Run with the Aeron source tree as the working directory.
#
# Aeron's aeron-archive/src/main/c/CMakeLists.txt declares
#     ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/*.h   ->  <build>/generated/aeron_c_archive_client/
# but the command it runs is
#     :aeron-archive:generateCCodecs -Dcodec.target.dir=${ARCHIVE_C_CODEC_TARGET_DIR}
# with SBE namespace aeron.archive.client, so the 58 headers actually land in
#     <build>/generated/c/aeron_archive_client/
# which is also where the C sources #include them from (#include "c/aeron_archive_client/...", against
# include dir ${ARCHIVE_CODEC_TARGET_DIR}). Both path segments in the OUTPUT list are wrong.
#
# Generation and compilation are fine — only the declaration is wrong, which is why this has been
# invisible. The cost is that the declared outputs never appear, so the edge is never satisfied and
# ninja re-runs the whole --no-daemon Gradle codec generation on EVERY build (~4 s), which also puts
# a Gradle invocation in the path of every incremental build.
#
# Still wrong on 1.51.0, 1.52.0 and master as of 2026-08, so a version bump does not fix it.

set(CODEC_CMAKE "aeron-archive/src/main/c/CMakeLists.txt")

if (NOT EXISTS "${CODEC_CMAKE}")
    message(FATAL_ERROR "FixAeronCodecOutputs: ${CODEC_CMAKE} not found — run with the Aeron source "
                        "tree as the working directory.")
endif ()

file(READ "${CODEC_CMAKE}" CODEC_TEXT)

# Already patched (a re-populate that reused a patched tree): nothing to do.
if (CODEC_TEXT MATCHES "\\\${ARCHIVE_C_CODEC_TARGET_DIR}/aeron_archive_client/")
    return()
endif ()

string(REPLACE "\${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/"
               "\${ARCHIVE_C_CODEC_TARGET_DIR}/aeron_archive_client/"
               PATCHED_TEXT "${CODEC_TEXT}")

# Fail loudly rather than silently reverting to a Gradle run on every build: if a future Aeron bump
# fixes or restructures this, the pattern disappears and configure stops here so the patch can go.
if (PATCHED_TEXT STREQUAL CODEC_TEXT)
    message(FATAL_ERROR
        "FixAeronCodecOutputs: expected OUTPUT path pattern not found in ${CODEC_CMAKE}. Aeron may "
        "have fixed this upstream — verify, then drop PATCH_COMMAND and this script from "
        "CMakeLists.txt.")
endif ()

file(WRITE "${CODEC_CMAKE}" "${PATCHED_TEXT}")
message(STATUS "Patched Aeron C archive codec OUTPUT paths to the directory generateCCodecs writes")
