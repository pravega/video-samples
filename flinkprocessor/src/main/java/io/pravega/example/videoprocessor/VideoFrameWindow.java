package io.pravega.example.videoprocessor;

import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Objects;

/**
 * A Window that represents a single VideoFrame.
 * Frames in a windows have matching camera, ssrc, and timestamp.
 */
public class VideoFrameWindow extends Window {
    // Unique ID for this video stream.
    // This is not required in the window if we keyBy camera.
    // However, to make this class usable for other situations (keyBy camera group),
    // we include it in this class.
    private final int camera;
    // Random source identifier used to avoid corruption if multiple sources use the same camera and timestamp.
    // See https://tools.ietf.org/html/rfc3550.
    private final int ssrc;
    // Event time of this frame. We use Timestamp to have nanosecond precision for high-speed cameras.
    private final Timestamp timestamp;

    public VideoFrameWindow(int camera, int ssrc, Timestamp timestamp) {
        this.camera = camera;
        this.ssrc = ssrc;
        this.timestamp = timestamp;
    }

    public VideoFrameWindow(ChunkedVideoFrame frame) {
        this.camera = frame.camera;
        this.ssrc = frame.ssrc;
        this.timestamp = frame.timestamp;
    }

    public int getCamera() {
        return camera;
    }

    public int getSsrc() {
        return ssrc;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "VideoFrameWindow{" +
                "camera=" + camera +
                ", ssrc=" + ssrc +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VideoFrameWindow that = (VideoFrameWindow) o;
        return camera == that.camera &&
                ssrc == that.ssrc &&
                timestamp.equals(that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(camera, ssrc, timestamp);
    }

    /**
     * Gets the largest timestamp that still belongs to this window.
     *
     * @return The largest timestamp that still belongs to this window.
     */
    @Override
    public long maxTimestamp() {
        return timestamp.getTime();
    }

    // ------------------------------------------------------------------------
    // Serializer
    // ------------------------------------------------------------------------

    /**
     * The serializer used to write the VideoFrameWindow type.
     */
    public static class Serializer extends TypeSerializerSingleton<VideoFrameWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public VideoFrameWindow createInstance() {
            return null;
        }

        @Override
        public VideoFrameWindow copy(VideoFrameWindow from) {
            return from;
        }

        @Override
        public VideoFrameWindow copy(VideoFrameWindow from, VideoFrameWindow reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 4 + 4 + 8 + 4;
        }

        @Override
        public void serialize(VideoFrameWindow record, DataOutputView target) throws IOException {
            target.writeInt(record.camera);
            target.writeInt(record.ssrc);
            target.writeLong(record.timestamp.getTime());
            target.writeInt(record.timestamp.getNanos());
        }

        @Override
        public VideoFrameWindow deserialize(DataInputView source) throws IOException {
            int camera = source.readInt();
            int ssrc = source.readInt();
            Timestamp timestamp = new Timestamp(source.readLong());
            timestamp.setNanos(source.readInt());
            return new VideoFrameWindow(camera, ssrc, timestamp);
        }

        @Override
        public VideoFrameWindow deserialize(VideoFrameWindow reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeInt(source.readInt());
            target.writeInt(source.readInt());
            target.writeLong(source.readLong());
            target.writeInt(source.readInt());
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof VideoFrameWindow.Serializer;
        }
    }
}
