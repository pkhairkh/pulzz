/* pulzZ C quickstart — cross-language example.

   Build:
     cargo build -p pulzz-ffi
     cc examples/c_quickstart.c -I ffi/include -L target/debug -lpulzz -o target/debug/c_quickstart
   Run:
     LD_LIBRARY_PATH=target/debug ./target/debug/c_quickstart
*/

#include <stdio.h>
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
    printf("pulzZ C quickstart\n");
    printf("  abi version = 0x%x\n", pulzz_abi_version());
    printf("  version str = %s\n", pulzz_version_string());

    PulzzConfig cfg = {
        .carrier = WebSocket,
        .security = PqSimpleV1,
        .batch_size = 50,
        .zstd_level = 3,
        .timeout_ms = 1000,
    };

    PulzzClientHandle client = NULL;
    PulzzResult r = pulzz_client_new(&cfg, &client);
    if (r != Ok) {
        fprintf(stderr, "client_new failed: %s (%s)\n",
                result_name(r), pulzz_last_error() ? pulzz_last_error() : "(no error)");
        return 1;
    }
    printf("  client_new   = %s\n", result_name(r));

    r = pulzz_client_connect(client, "ws://127.0.0.1:1");
    printf("  client_connect (to closed port) = %s\n", result_name(r));
    if (r != Ok) {
        printf("    last_error = %s\n",
               pulzz_last_error() ? pulzz_last_error() : "(null)");
    }

    PulzzBatchHandle batch = NULL;
    r = pulzz_batch_new(&batch);
    printf("  batch_new    = %s\n", result_name(r));

    const char *p1 = "{\"user\":\"alice\"}";
    PulzzSlice s1 = { .ptr = (const uint8_t *)p1, .len = strlen(p1) };
    pulzz_batch_add(batch, 1, 1, s1);

    r = pulzz_client_send_batch(client, batch);
    printf("  send_batch   = %s (expected InvalidState)\n", result_name(r));

    pulzz_client_free(client);
    printf("  client freed\n");

    printf("pulzZ C quickstart complete\n");
    return 0;
}
