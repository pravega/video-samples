# Video Chunking

The Pravega Event API is limited to 8 MiB events.
To allow storage of objects larger than 8 MiB, such as video frames and images,
objects must be split into chunks of 8 MiB or less.

The logical object that is generally stored for video is the video frame.
In this example, the [VideoFrame](flinkprocessor/src/main/java/io/pravega/example/videoprocessor/VideoFrame.java)
class stores a video frame.
It has a timestamp and a byte array containing a PNG-encoded image.
The size of the image is limited only by the JVM which limits arrays to 2 GiB.

Note: If you are storing a video **transport stream**, such as Real-time Transport Protocol,
HTTP Live Streaming, or MPEG Transport Stream (broadcast video), these are already packetized into
small packets of 188 or 1500 bytes. These can be stored in Pravega as-is. However, due to the complexity
involved in decoding such transport streams, it is often easier to store individual video frames
as ordinary images using the methods described in this project.

## Writing Video to Pravega

Before a `VideoFrame` instance can be written to Pravega, it is split into
one or more [ChunkedVideoFrame](flinkprocessor/src/main/java/io/pravega/example/videoprocessor/ChunkedVideoFrame.java)
instances.
A `ChunkedVideoFrame` is a subclass of `VideoFrame` and it adds two 16-bit integers,
`ChunkIndex` and `FinalChunkIndex`.
`ChunkIndex` is a 0-based counter for this `ChunkedVideoFrame` within the `VideoFrame`.
`FinalChunkIndex` is the value of the last `ChunkIndex` for this `VideoFrame` and is equal to the number
of chunks minus 1.
For example, a 1.5 MiB image can be split into three 0.5 MiB chunks with {ChunkIndex, FinalChunkIndex} pairs of
{0,2}, {1,2}, {2,2}.

As implemented in `VideoFrameChunker`, the first chunk will contain the first 0.5 MB of the image,
the second chunk will contain the second 0.5 MB of the image,
and so on. The last chunk may be smaller than the other chunks.

`ChunkedVideoFrame` instances are then serialized into JSON using `ChunkedVideoFrameSerializationSchema` and
it is this JSON that is written to the Pravega stream.
JSON is widely supported, simple to use, and easy to inspect.
However, because it requires base-64 encoding for byte arrays, it has a 33% storage overhead
compared to more efficient encodings such as Avro and Protobuf.

If a non-transactional Pravega writer were to fail while writing chunks of video, this could result in only some
of the chunks being written. Although this can easily be handled by the reassembly process, this could cause high
memory usage for the state of the reassembly process. To avoid this, Pravega transactions can be used to keep
chunks for the same image within a single transaction.

For an example of a Flink video writer job, see
[VideoDataGeneratorJob](flinkprocessor/src/main/java/io/pravega/example/videoprocessor/VideoDataGeneratorJob.java).

## Reading Video from Pravega

To read video frames from Pravega, the Pravega reader first reads the JSON-encoded `ChunkedVideoFrame`
and deserializes it.

Next, a series of Flink operations is performed.
```java
DataStream<VideoFrame> videoFrames = chunkedVideoFrames
    .keyBy("camera")
    .window(new ChunkedVideoFrameWindowAssigner())
    .process(new ChunkedVideoFrameReassembler());
```

The `keyBy` function enables parallel execution of different cameras.
As new events are read from Pravega from multiple Flink tasks in parallel, events
from the same camera will be grouped together and handled by the same task.

The `window` function groups `ChunkedVideoFrame` instances by camera and timestamp.
It also defines a default trigger function that watches for
`ChunkedVideoFrame` instances where `ChunkIndex` equals `FinalChunkIndex`.
When this occurs, it returns
`FIRE_AND_PURGE` which tells it to call the `process` function and then it purges the
video frame from the state.

Flink watermarks are used during reassembly to purge the state of video frames with missing chunks.
If a non-transactional Pravega writer wrote only 2 of 3 chunks, these chunks would be purged from the
Flink state after 1 second, thus freeing memory.

The `ChunkedVideoFrameReassembler` process function concatenates the byte arrays from all `ChunkedVideoFrame` instances
and outputs `VideoFrame` instances.
It checks for missing chunks and out-of-order chunks.
It also validates that the SHA-1 hash of the data matches the hash calculated when it was written to Pravega.
Note that this check can be removed for high-throughput applications as Pravega and Flink
have additional layers of data consistency checks.

For an example of a Flink video reader job, see
[VideoReaderJob](flinkprocessor/src/main/java/io/pravega/example/videoprocessor/VideoReaderJob.java).

For a complete job that reads video from Pravega, processes it, and writes the processed video,
see [MultiVideoGridJob](flinkprocessor/src/main/java/io/pravega/example/videoprocessor/MultiVideoGridJob.java).

# Limitations

As of Pravega 0.5.0, the watermarks feature ((Issue 3344)[https://github.com/pravega/pravega/issues/3344])
is not yet available. This means a Flink application that requires watermarks,
for instance, one that uses event time windows, must assign watermarks using
`assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor(1000))`.
This generally works well when reading the most recent events at the tail of the stream.
However, it generally fails to produce accurate watermarks when performing non-tail reads
such as when reprocessing historical events or restarting from old checkpoints.

Either of the following methods may provide a workaround for this limitation.

- Set minNumSegments to 1 and disable segment scaling.
  With only a single segment, `BoundedOutOfOrdernessTimestampExtractor` will work as expected.
  This will limit stream throughput.
  
- Use a Flink batch job for any historical processing.
  Configure Flink streaming jobs to begin at the tail of the input Pravega stream(s).

# Memory Usage of the Flink Video Reader

A Flink task must store in memory all `ChunkedVideoFrame` instances which it has read until the final chunk for that
frame has been read or a session timeout occurs. A single Flink task that is reading from multiple Pravega segments
may receive chunks from interleaved frames, this requiring multiple partial frames to be buffered. To avoid out-of-memory
errors, each Flink task should have enough memory to buffer its share of Pravega segments.
For example, if you have 12 Pravega segments and 3 Flink reader tasks, you should account for 4 frames to be buffered
for each Flink reader task.

If you are using a non-transactional writer, you should also account for additional frames to be buffered.
If interruptions are rare, a single additional frame should be sufficient.
