/* pulzZ C API example — demonstrates the lifecycle without requiring a
 * running server: create a client, attempt a connect, observe the error
 * path, and clean up.
 *
 * Build:
 *   cc -I ffi/include ffi/examples/c_example.c \
 *      -L target/debug -lpulzz -o c_example
 * Run:
 *   LD_LIBRARY_PATH=target/debug ./c_example
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pulzz.h"

static const char *result_name(PulzzResult r) {
    switch (r) {
    case Ok: return "OK";
    case InvalidArg: return "INVALID_ARG";
    case InvalidState: return "INVALID_STATE";
    case ConnectionFailed: return "CONNECTION_FAILED";
    case HandshakeFailed: return "HANDSHAKE_FAILED";
    case CompressionFailed: return "COMPRESSION_FAILED";
    case Timeout: return "TIMEOUT";
    case BufferTooSmall: return "BUFFER_TOO_SMALL";
    case EndOfStream: return "END_OF_STREAM";
    case Internal: return "INTERNAL";
    default: return "?";
    }
}

int main(void) {
    printf("pulzZ C ABI example\n");
    printf("  abi version   = 0x%x\n", pulzz_abi_version());
    printf("  version str   = %s\n", pulzz_version_string());

    /* Build a default config and create a client handle. */
    PulzzConfig cfg = {
        .carrier = WebSocket,
        .security = PqSimpleV1,
        .batch_size = 0,
        .zstd_level = 3,
        .timeout_ms = 1000,
    };

    PulzzClientHandle client = NULL;
    PulzzResult r = pulzz_client_new(&cfg, &client);
    if (r != Ok) {
        fprintf(stderr, "pulzz_client_new failed: %s (%s)\n",
                result_name(r), pulzz_last_error() ? pulzz_last_error() : "(no error)");
        return 1;
    }
    printf("  client_new   = %s\n", result_name(r));

    /* Try to connect to a closed port — we expect a failure here. */
    r = pulzz_client_connect(client, "ws://127.0.0.1:1");
    printf("  client_connect (to closed port) = %s\n", result_name(r));
    if (r != Ok) {
        printf("    last_error = %s\n",
               pulzz_last_error() ? pulzz_last_error() : "(null)");
    }

    /* Build a batch (lifecycle only — not sent). */
    PulzzBatchHandle batch = NULL;
    r = pulzz_batch_new(&batch);
    printf("  batch_new    = %s\n", result_name(r));

    const char *payload1 = "{\"user\":\"alice\"}";
    PulzzSlice slice1 = {
        .ptr = (const uint8_t *)payload1,
        .len = strlen(payload1),
    };
    r = pulzz_batch_add(batch, 1, 1, slice1);
    printf("  batch_add    = %s\n", result_name(r));

    /* send_batch will fail with InvalidState because we're not connected,
     * but the call exercises the full panic-safe path. */
    r = pulzz_client_send_batch(client, batch);
    printf("  send_batch   = %s (expected InvalidState)\n", result_name(r));

    /* Cleanup. */
    pulzz_client_free(client);
    printf("  client freed\n");

    printf("pulzZ C ABI example complete\n");
    return 0;
}
