concurrent-utils
====

Threaded Java implementation of GZIP output stream for high-performance compression.

**High-level features:**

* Write to an [OutputStream][outputstream] interface.
* Configurable buffer sizes, compression level, and thread pooling.
* Fails fast on any I/O issues on underlying stream or compression.
* Speeds up compression of larger content close to linearly with # threads.
* Implements [pigz][pigz] technique of priming compression dictionary with last 1/4 of previous block for better compression.

Quick Start Usage
-----------------

Accepting all internal defaults:

```java
// * Buffer sizes of 128 kB
// * Default `Deflater` compression level
// * Re-usable fixed thread pool of size = # processors.
final OutputStream out = new ConcurrentGZIPOutputStream(new ByteArrayOutputStream());
// write bytes to the stream however you like
out.write(someBytesToCompress);
out.close();
```

Benchmarks
----------

Benchmarks performed on a 24-core KnownHost VPS-2 instance with JDK 7u21.
See [ConcurrentGZIPPerformanceTest][concurrentgzipperformancetest] for the test case, which can be run with:

```
$ mvn clean test -DincludePerfTests=true
```

**Random input pattern designed for poor compression:**
![alt text][random_chart_small]

**Sequential input pattern designed to compress well:**
![alt text][sequential_chart_small]

Compression ratios in both cases are within ~0.1%. (JRE producing the slightly
better compression. The last 1/4 of previous block method helps offset some of
the disadvantage, but still isn't as good as using a single dictionary for the
entire input.)

See [ConcurrentGZIPOutputStreamTest][concurrentgzipoutputstreamtest] unit test for more examples.

Limitations
-----------

* Moderate amount of overhead. JRE's single-threaded performance is better.

[pigz]: http://zlib.net/pigz/
[outputstream]: http://docs.oracle.com/javase/7/docs/api/java/io/OutputStream.html
[concurrentgzipoutputstreamtest]: https://github.com/john-morales/concurrent-utils/blob/master/src/test/java/com/jmo/concurrent/utils/ConcurrentGZIPOutputStreamTest.java
[concurrentgzipperformancetest]: https://github.com/john-morales/concurrent-utils/blob/master/src/test/java/com/jmo/concurrent/utils/ConcurrentGZIPPerformanceTest.java
[random_chart_small]: https://raw.github.com/john-morales/concurrent-utils/master/doc/img/20130523_random_1280.png "Random Data Pattern - Compressing 64MB"
[sequential_chart_small]: https://raw.github.com/john-morales/concurrent-utils/master/doc/img/20130523_sequential_1280.png "Sequential Data Pattern - Compressing 64MB"
