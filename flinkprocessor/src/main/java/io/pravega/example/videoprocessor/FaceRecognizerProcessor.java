package io.pravega.example.videoprocessor;

import io.pravega.example.common.Embedding;
import io.pravega.example.common.EmbeddingsTransaction;
import io.pravega.example.common.VideoFrame;
import io.pravega.example.tensorflow.BoundingBox;
import io.pravega.example.tensorflow.EmbeddingsComparator;
import io.pravega.example.tensorflow.ImageUtil;
import io.pravega.example.tensorflow.Recognition;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/*
 *   This class manages the faces of people known in state with incoming processed known face embeddings. Matches
 * embeddings of scanned faces to known faces for facial recognition upon a face being processed from camera. Upon
 * matching faces, creates video frames with labelled faces and location of faces identified.
 * */
public class FaceRecognizerProcessor
        extends KeyedBroadcastProcessFunction<String, VideoFrame, EmbeddingsTransaction, VideoFrame> {

    private static final Logger log = LoggerFactory.getLogger(FaceRecognizerProcessor.class);

    // broadcast state descriptor
    // personId -> set of embeddings
    MapStateDescriptor<String, Set<Embedding>> embeddingsDbDesc;

    @Override
    public void open(Configuration conf) {
        // initialize keyed state
        embeddingsDbDesc =
                new MapStateDescriptor<>("embeddingBroadcastState", BasicTypeInfo.STRING_TYPE_INFO,
                        (TypeInformation<Set<Embedding>>) TypeInformation.of(new TypeHint<Set<Embedding>>() {
                        }));
    }

    @Override
    public void processElement(VideoFrame frame, ReadOnlyContext ctx, Collector<VideoFrame> out) throws Exception {
        Iterable<Map.Entry<String, Set<Embedding>>> embeddigsIterable = ctx
                .getBroadcastState(this.embeddingsDbDesc)
                .immutableEntries();
        Iterator<Map.Entry<String, Set<Embedding>>> embeddingsIterator = embeddigsIterable.iterator();
        ImageUtil imageUtil = new ImageUtil();

        for (int i = 0; i < frame.recognizedBoxes.size() && frame.lastBadges != null; i++) {
            BoundingBox currFaceLocation = frame.recognizedBoxes.get(i);
            float[] currEmbedding = frame.embeddingValues.get(i);
            String match = EmbeddingsComparator.matchEmbedding(currEmbedding, embeddingsIterator);
            Recognition recognition;
            if(!match.equals("Unknown")) {
                recognition = EmbeddingsComparator.getLabel(match, currFaceLocation);
            } else {
                recognition = EmbeddingsComparator.getLabel(match, currFaceLocation);
            }
            frame.data = imageUtil.labelFace(frame.data, recognition);
        }
        frame.hash = frame.calculateHash();
        out.collect(frame);
    }

    @Override
    public void processBroadcastElement(EmbeddingsTransaction transaction, Context ctx, Collector<VideoFrame> out)
            throws Exception {
        BroadcastState<String, Set<Embedding>> bcState = ctx.getBroadcastState(embeddingsDbDesc);

        if (transaction.transactionType.equals("add")) {
            for (int i = 0; i < transaction.embeddingValues.size(); i++) {
                float[] embeddingValues = transaction.embeddingValues.get(i);
                Embedding embedding = new Embedding(transaction.personId, embeddingValues, transaction.imageName
                        , transaction.timestamp);

                Set<Embedding> embeddingSet = bcState.get(transaction.personId);
                if (embeddingSet == null) {
                    embeddingSet = new HashSet<>();
                }
                embeddingSet.add(embedding);

                // add the new embedding to database
                bcState.put(transaction.personId, embeddingSet);
            }
        } else if (transaction.transactionType.equals("delete")) {
            // remove the embeddings related to the person in database
            bcState.remove(transaction.personId);
        }
    }
}
