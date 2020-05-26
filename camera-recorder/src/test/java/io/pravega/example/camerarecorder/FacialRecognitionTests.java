//package io.pravega.example.camerarecorder;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import io.pravega.client.EventStreamClientFactory;
//import io.pravega.client.stream.EventStreamWriter;
//import io.pravega.client.stream.EventWriterConfig;
//import io.pravega.client.stream.impl.ByteBufferSerializer;
//import io.pravega.example.common.ChunkedVideoFrame;
//import io.pravega.example.common.PravegaUtil;
//import io.pravega.example.common.VideoFrame;
//import org.bytedeco.javacpp.BytePointer;
//import org.bytedeco.javacv.CanvasFrame;
//import org.bytedeco.javacv.Frame;
//import org.bytedeco.javacv.OpenCVFrameConverter;
//import org.bytedeco.opencv.global.opencv_imgcodecs;
//import org.bytedeco.opencv.global.opencv_videoio;
//import org.bytedeco.opencv.opencv_core.Mat;
//import org.bytedeco.opencv.opencv_videoio.VideoCapture;
//import org.junit.Ignore;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.nio.ByteBuffer;
//import java.nio.file.Files;
//import java.rmi.ConnectIOException;
//import java.sql.Timestamp;
//import java.util.Random;
//import java.util.concurrent.CompletableFuture;
//
//public class FacialRecognitionTests {
//    private static Logger log = LoggerFactory.getLogger(FacialRecognitionTests.class);
//
//    @Test
//    @Ignore
//    public void Test2() throws Exception {
//        // Initialize camera.
//        final int captureWidth = getConfig().getImageWidth();
//        final int captureHeight = getConfig().getImageHeight();
//        log.info("creating grabber");
//        final VideoCapture cap = new VideoCapture(getConfig().getCameraDeviceNumber());
//
//        if(!cap.open(getConfig().getCameraDeviceNumber())) {
//            throw new ConnectIOException("Cannot open the camera");
//        }
//
//        log.info("starting video capture");
//        cap.set(opencv_videoio.CAP_PROP_FPS, getConfig().getFramesPerSec());
//        cap.set(opencv_videoio.CAP_PROP_FRAME_WIDTH, captureWidth);
//        cap.set(opencv_videoio.CAP_PROP_FRAME_HEIGHT, captureHeight);
//
//        final double actualFramesPerSec = cap.get(opencv_videoio.CAP_PROP_FPS);
//        log.info("actual frame rate={}", actualFramesPerSec);
//        final boolean dropFrames = actualFramesPerSec > getConfig().getFramesPerSec();
//        long lastTimestamp = 0;
//
//        // Initialize capture preview window.
//        final CanvasFrame cFrame = new CanvasFrame("Capture Preview", CanvasFrame.getDefaultGamma() / 2.2);
//
//        // Create Pravega stream.
//        PravegaUtil.createStream(getConfig().getClientConfig(), getConfig().getOutputStreamConfig());
//
//        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(
//                getConfig().getOutputStreamConfig().getStream().getScope(),
//                getConfig().getClientConfig());
//             EventStreamWriter<ByteBuffer> pravegaWriter = clientFactory.createEventWriter(
//                     getConfig().getOutputStreamConfig().getStream().getStreamName(),
//                     new ByteBufferSerializer(),
//                     EventWriterConfig.builder().build())) {
//
//            ObjectMapper mapper = new ObjectMapper();
//            OpenCVFrameConverter.ToMat converterToMat = new OpenCVFrameConverter.ToMat();
//
//            int frameNumber = 0;
//            int ssrc = new Random().nextInt();
//            Frame capturedFrame;
//
//            Mat mat = new Mat();
//
////                OpenCVFrameConverter.ToIplImage converterToImage = new OpenCVFrameConverter.ToIplImage();
//            while (cap.read(mat)) {
//                capturedFrame = converterToMat.convert(mat);
//                long timestamp = System.currentTimeMillis();
//                // drop frames to adjust speed of camera recorder
////                    System.out.println("Current time diff is" + (timestamp - lastTimestamp));
//                if (dropFrames && timestamp - lastTimestamp < 1000 / getConfig().getFramesPerSec()) {
//                    log.debug("Dropping captured frame to maintain desired frames per second");
//                    continue;
//                }
//
//                log.info("frameNumber={}, timestamp={}, capturedFrame={}", frameNumber, timestamp, capturedFrame);
//
//                // Convert captured frame to JPEG.
//                BytePointer jpgBytePointer = new BytePointer();
//                opencv_imgcodecs.imencode(".jpg", mat,  jpgBytePointer);
//                log.info("jpgBytePointer={}", jpgBytePointer);
//                byte[] jpgByteArray = jpgBytePointer.getStringBytes();
//                if (false) {
//                    Files.write((new File(String.format("capture-%05d.jpg", frameNumber))).toPath(), jpgByteArray);
//                }
//
//                // Create VideoFrame. We assume that it fits in a single chunk (< 1 MB).
//                VideoFrame videoFrame = new VideoFrame();
//                videoFrame.camera = getConfig().getCamera();
//                videoFrame.ssrc = ssrc;
//                videoFrame.timestamp = new Timestamp(timestamp);
//                videoFrame.frameNumber = frameNumber;
//                videoFrame.data = jpgByteArray;
//                videoFrame.hash = videoFrame.calculateHash();
//                ChunkedVideoFrame chunkedVideoFrame = new ChunkedVideoFrame(videoFrame);
//                ByteBuffer jsonBytes = ByteBuffer.wrap(mapper.writeValueAsBytes(chunkedVideoFrame));
//
//                // Write to Pravega.
//                CompletableFuture<Void> future = pravegaWriter.writeEvent(Integer.toString(videoFrame.camera), jsonBytes);
//
//                // Show our frame in the preview window..
//                if (cFrame.isVisible()) {
//                    cFrame.showImage(capturedFrame);
//                }
//
//                // Make sure frame has been durably persisted to Pravega.
////                    future.get();
//
//                frameNumber++;
//                lastTimestamp = timestamp;
//    }
//
//
//
//}
