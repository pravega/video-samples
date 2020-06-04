package io.pravega.example.videoprocessor;

import io.pravega.example.common.Embedding;
import io.pravega.example.common.Transaction;
import io.pravega.example.common.VideoFrame;
import io.pravega.example.tensorflow.*;
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

    private static EmbeddingsComparator embeddingsComp;

    // broadcast state descriptor
    MapStateDescriptor<String, Embedding> embeddingsDesc;

    @Override
    public void open(Configuration conf) throws Exception {
        // initialize keyed state
        embeddingsDesc =
                new MapStateDescriptor<String, Embedding>("embeddingBroadcastState", String.class, Embedding.class);

        embeddingsComp = new EmbeddingsComparator();
    }

    @Override
    public void processElement(VideoFrame frame, ReadOnlyContext ctx, Collector<VideoFrame> out) throws Exception {
        Iterable<Map.Entry<String, Embedding>> embeddigsIterable = ctx
                .getBroadcastState(this.embeddingsDesc)
                .immutableEntries();
        Iterator<Map.Entry<String, Embedding>> embeddingsIterator = embeddigsIterable.iterator();
        ImageUtil imageUtil = new ImageUtil();

        for(int i=0; i < frame.recognizedBoxes.size(); i++) {
            BoundingBox currFaceLocation = frame.recognizedBoxes.get(i);
            float[] currEmbedding = frame.embeddingValues.get(i);
            String match = embeddingsComp.matchEmbedding(currEmbedding, embeddingsIterator);
            Recognition recognition = embeddingsComp.getLabel(match, currFaceLocation);
            frame.data = imageUtil.labelFace(frame.data, recognition);
        }

        frame.hash = frame.calculateHash();
        out.collect(frame);
    }

    @Override
    public void processBroadcastElement(Transaction transaction, Context ctx, Collector<VideoFrame> out) throws Exception {
        BroadcastState<String, Embedding> bcState = ctx.getBroadcastState(embeddingsDesc);

        if (transaction.transactionType.equals("add")) {
            for (int i = 0; i < transaction.embeddingValues.size(); i++) {
                float[] embeddingValues = transaction.embeddingValues.get(i);
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
