package io.pravega.example.videoprocessor;

import io.pravega.example.common.Embedding;
import io.pravega.example.common.Transaction;
import io.pravega.example.common.VideoFrame;
import io.pravega.example.tensorflow.BoundingBox;
import io.pravega.example.tensorflow.FaceRecognizer;
import io.pravega.example.tensorflow.ImageUtil;
import io.pravega.example.tensorflow.Recognition;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.bytedeco.opencv.opencv_core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.bytedeco.opencv.global.opencv_imgcodecs.IMREAD_UNCHANGED;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imdecode;

/*
 *   This class manages the embeddings database in state, and does facial recognition
 * */
public class FaceRecognizerProcessor
        extends KeyedBroadcastProcessFunction<String, VideoFrame, Transaction, VideoFrame> {

    private final Logger log = LoggerFactory.getLogger(FaceRecognizerProcessor.class);

    FaceRecognizer recognizer;

    // broadcast state descriptor
    MapStateDescriptor<String, Embedding> embeddingsDatabaseDesc; // mapped from personId to Embedding

//    ValueState<Set> badgeListState;

    @Override
    public void open(Configuration conf) throws Exception {
        // initialize keyed state
        embeddingsDatabaseDesc =
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
                .getBroadcastState(this.embeddingsDatabaseDesc)
                .immutableEntries();

        Iterator<Map.Entry<String, Embedding>> embeddingsIterator = embeddigsIterable.iterator();
        ImageUtil imageUtil = new ImageUtil();

        for(int i=0; i < frame.recognizedBoxes.size() && frame.lastBadges != null; i++) {
//        for(int i=0; i < frame.recognizedBoxes.size(); i++) {
            BoundingBox currFaceLocation = frame.recognizedBoxes.get(i);
            float[] currEmbedding = frame.embeddings.get(i);
            String match = recognizer.matchEmbedding(currEmbedding, embeddingsIterator, frame.lastBadges);

            Recognition recognition;
            if(!match.equals("Unknown")) {
                recognition = recognizer.getLabel(match, currFaceLocation);
            } else {
                recognition = recognizer.getLabel(match, currFaceLocation);
            }
            frame.data = imageUtil.labelFace(frame.data, recognition, !match.equals("Unknown"));
        }

        frame.hash = frame.calculateHash();
        out.collect(frame);
    }

    @Override
    public void processBroadcastElement(Transaction transaction, Context ctx, Collector<VideoFrame> out) throws Exception {
        BroadcastState<String, Embedding> bcState = ctx.getBroadcastState(embeddingsDatabaseDesc);

        log.info("trying to add element");

        if (transaction.transactionType.equals("add")) {
            List<BoundingBox> recognizedBoxes = recognizer.locateFaces(transaction.imageData);

            Mat imageMat = imdecode(new Mat(transaction.imageData), IMREAD_UNCHANGED);

            for (int i = 0; i < recognizedBoxes.size(); i++) {
                BoundingBox currentFace = recognizedBoxes.get(i);
                byte[] croppedFace = recognizer.cropFace(currentFace, imageMat);
                float[] embeddingValues = recognizer.embeddFace(croppedFace);
                Embedding embedding = new Embedding(transaction.personId, embeddingValues, transaction.imageName, transaction.timestamp);

                // add the new embedding to database
                bcState.put(transaction.personId, embedding);
            }
        } else if (transaction.transactionType.equals("delete")) {
            // remove the embeddings related to the person in database
            bcState.remove(transaction.personId);
        }
    }
}
