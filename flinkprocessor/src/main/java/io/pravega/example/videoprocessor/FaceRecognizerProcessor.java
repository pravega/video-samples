package io.pravega.example.videoprocessor;

import akka.event.NoLogging;
import io.pravega.example.common.Embedding;
import io.pravega.example.common.Transaction;
import io.pravega.example.common.VideoFrame;
import io.pravega.example.tensorflow.*;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.bytedeco.opencv.opencv_core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.bytedeco.opencv.global.opencv_imgcodecs.IMREAD_UNCHANGED;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imdecode;

public class FaceRecognizerProcessor
    extends KeyedBroadcastProcessFunction<String, VideoFrame, Transaction, VideoFrame> {

    private final Logger log = LoggerFactory.getLogger(FaceRecognizer.class);

    FaceRecognizer recognizer;

    // broadcast state descriptor
    MapStateDescriptor<String, Embedding> embeddingsDesc;

    @Override
    public void open(Configuration conf) throws Exception {
        // initialize keyed state
        embeddingsDesc =
                new MapStateDescriptor<String, Embedding>("embeddingBroadcastState", String.class, Embedding.class);
        recognizer = new FaceRecognizer();
        recognizer.warmup();

    }

    @Override
    public void close() {
        recognizer.close();
    }

    @Override
    public void processElement(VideoFrame frame, ReadOnlyContext ctx, Collector<VideoFrame> out) throws Exception {
        Iterable<Map.Entry<String, Embedding>> embeddigsIterable = ctx
                .getBroadcastState(this.embeddingsDesc)
                .immutableEntries();
        Iterator<Map.Entry<String, Embedding>> embeddingsIterator = embeddigsIterable.iterator();

        VideoFrame outputFrame = recognizer.recognizeFaces(frame, embeddingsIterator);
        out.collect(outputFrame);
    }

    @Override
    public void processBroadcastElement(Transaction transaction, Context ctx, Collector<VideoFrame> out) throws Exception {
        BroadcastState<String, Embedding> bcState = ctx.getBroadcastState(embeddingsDesc);

        log.info("trying to add element");

        if(transaction.transactionType.equals("add")) {
            List<BoundingBox> recognizedBoxes = recognizer.detectFaces(transaction.imageData);

            Mat imageMat = imdecode(new Mat(transaction.imageData), IMREAD_UNCHANGED);

            for(int i=0; i< recognizedBoxes.size(); i++) {
                BoundingBox currentFace = recognizedBoxes.get(i);
                byte[] croppedFace = recognizer.cropFace(currentFace, imageMat);
                float[] embeddingValues = recognizer.embeddFace(croppedFace);
                Embedding embedding = new Embedding(transaction.personId, embeddingValues, transaction.imageName, transaction.timestamp);

                log.info("add embedding: " + embedding);

                // add the new embedding to database
                bcState.put(transaction.personId, embedding);
            }
        } else if(transaction.transactionType.equals("delete")) {
            // remove the embeddings related to the person in database
            bcState.remove(transaction.personId);
        }
    }
}
