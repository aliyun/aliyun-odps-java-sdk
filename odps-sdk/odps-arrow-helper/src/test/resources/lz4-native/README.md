# Synthetic dependent-block fixtures

These are standard LZ4 frames produced with the official LZ4 CLI 1.9.4 (`-B4 -BD`). They contain no customer data.

- `minimal-linked.lz4`: 65,537 `x` bytes. The second block makes block dependence explicit in the frame descriptor.
- `history-linked.lz4`: a fixed pseudorandom 49,152-byte dictionary repeated to 1 MiB, requiring history across 64 KiB blocks. Decoded SHA-256: `f8d7c323f6da44578c07e42d6c14e2020c7f862cddf66e9cca69e8e7d774df2b`.

Frame SHA-256:

- minimal: `66636a702f926f49694188bcbee7a11d345972d5e962a21e53883f78fc9f33f5`
- history: `c5d8d405316abfd0096e079879ce8e6b04b29099d91a4b6279649e2a820103e5`
