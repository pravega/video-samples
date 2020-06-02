package io.pravega.example.tensorflow;

import io.pravega.example.common.Embedding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public class EmbeddingsComparator {
    private final Logger log = LoggerFactory.getLogger(EmbeddingsComparator.class);

    private static final double THRESHOLD = 0.97;   // Threshold for matching embeddings


    /**
     * @param otherEmbedding     The current facial embedding found in the image
     * @param embeddingsDatabase The database of facial embeddings to compare to
     * @return The name of the person the embedding matches with in the embeddings database
     */
    public String matchEmbedding(float[] otherEmbedding, Iterator<Map.Entry<String, Embedding>> embeddingsDatabase) {
        String match = "Unknown";
        double minDiff = 1.0;

        while (embeddingsDatabase.hasNext()) {
            Map.Entry<String, Embedding> embeddingEntry = embeddingsDatabase.next();
            String personId = embeddingEntry.getKey();
            Embedding embedding = embeddingEntry.getValue();

            log.info("Current embedding considered is " + embedding.personId);

            double diff = compareEmbeddings(embedding.embeddingValue, otherEmbedding);
            log.info("distance with " + personId + " is " + diff);

            // Matches if within threshold
            if (diff < THRESHOLD && diff < minDiff) {
                match = personId;
            }
        }

        return match;
    }

    /**
     * @param origEmbedding  The data for a facial embedding
     * @param otherEmbedding The data for another facial embedding
     * @return The difference between the facial embeddings
     */
    public double compareEmbeddings(float[] origEmbedding, float[] otherEmbedding) {
        double sumDiffSq = 0;

        for (int i = 0; i < origEmbedding.length; i++) {
            sumDiffSq += Math.pow(origEmbedding[i] - otherEmbedding[i], 2);
        }

        return Math.sqrt(sumDiffSq);
    }

    /**
     *
     * @param badgeId identifier for the face
     * @param faceLocation location of the face
     * @return a label representing location and identifier for face
     */
    public Recognition getLabel(String badgeId, BoundingBox faceLocation) {
        Recognition recognition = new Recognition(1, badgeId, (float) 1,
                new BoxPosition((float) (faceLocation.getX()),
                        (float) (faceLocation.getY()),
                        (float) (faceLocation.getWidth()),
                        (float) (faceLocation.getHeight())));

        return recognition;
    }
}
