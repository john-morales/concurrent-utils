pigz4j
====

Threaded implementation of gzip stream based on [pigz][pigz].

**High-level features:**

* Write to an [OutputStream][outputstream] interface.
* Configurable buffer sizes, compression level, and thread pooling.
* Fails fast on any I/O issues on underlying stream or compression.
* Speed up compression of larger content close to linearly with # threads.
* Like pigz, the last 1/4 of previous block used to prime dictionary for better compression.

Limitations
-----------

* Moderate amount of overhead. JRE's single-threaded performance is better.

Quick Start Usage
-----------------

Accepting all internal defaults:

```java
// * Buffer sizes of 128 kB
// * Default `Deflater` compression level
// * Re-usable fixed thread pool of size = # processors.
final PigzOutputStream out = new PigzOutputStream(new ByteArrayOutputStream());
// write bytes to the stream however you like
out.write(someBytesToCompress);
out.close();
```

Benchmarks
----------

Benchmarks performed on a 24-core KnownHost VPS-2 instance.
See [PigzPerformanceTest][pigzperformancetest] for the test case, which can be run with:

```
$ mvn clean test -DincludePerfTests=true
```

**Random input pattern designed for poor compression:**
![alt text][random_chart_small]

**Sequential input pattern designed to compress well:**
![alt text][sequential_chart_small]

Compression ratios in both cases are within ~0.1%. (JRE producing the slightly
better compression. The last 1/4 of previous block method helps offset some of
the disadvantage, but still isn't as good using a single dictionary for the
entire input.)

See [PigzOutputStreamTest][pigzoutputstreamtest] unit test for more examples.

[pigz]: http://zlib.net/pigz/
[outputstream]: http://docs.oracle.com/javase/7/docs/api/java/io/OutputStream.html
[pigzoutputstreamtest]: https://github.com/john-morales/pigz4j/blob/master/src/test/java/com/pigz4j/io/stream/PigzOutputStreamTest.java
[pigzperformancetest]: https://github.com/john-morales/pigz4j/blob/master/src/test/java/com/pigz4j/io/stream/PigzOutputStreamTest.java
[sequential_chart_small]: https://raw.github.com/john-morales/pigz4j/master/doc/img/20130523_sequential_1280.png "Sequential Data Pattern - Compressing 64MB"
[random_chart_small]: https://raw.github.com/john-morales/pigz4j/master/doc/img/20130523_random_1280.png "Random Data Pattern - Compressing 64MB"
