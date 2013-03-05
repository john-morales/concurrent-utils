pigz4j
====

Threaded implementation of gzip stream based on [pigz][pigz].

**High-level features:**

* Write to an [OutputStream][outputstream] interface.
* Configurable buffer sizes, compression level, and thread pooling.
* Fails fast on any I/O issues on underlying stream or compression
* For compressing larger content, shown to scale linearly with # threads. (Benchmarks  coming soon.)

Limitations
-----------

* Compression ratio lags pigz - current version of pigz4j doesn't implement the compression dictionary priming between blocks.

Usage
-----

Accepting all internal defaults:

```
// * Buffer sizes of 128 kB
// * Default `Deflater` compression level
// * Re-usable fixed thread pool of size = # processors.
final PigzOutputStream out = new PigzOutputStream(new ByteArrayOutputStream());
// write bytes to the stream however you like
out.write(someBytesToCompress);
out.close();
```

See [PigzOutputStreamTest][pigzoutputstreamtest] unit test for more examples.

[pigz]: http://zlib.net/pigz/
[outputstream]: http://docs.oracle.com/javase/6/docs/api/java/io/OutputStream.html
[pigzoutputstreamtest]: https://github.com/john-morales/pigz4j/blob/master/src/test/java/com/pigz4j/io/stream/PigzOutputStreamTest.java
