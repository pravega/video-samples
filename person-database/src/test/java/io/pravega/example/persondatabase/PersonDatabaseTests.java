package io.pravega.example.persondatabase;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.example.common.PravegaAppConfiguration.StreamConfig;
import io.pravega.example.common.Transaction;
import io.pravega.example.videoplayer.StreamCutBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.UUID;

public class PersonDatabaseTests {
    private static Logger log = LoggerFactory.getLogger(PersonDatabaseTests.class);

    @Test
    @Ignore
    // Read data from database stream.
    public void Test1() throws Exception {
        String scope = "examples";

        String streamName = "person-database-transaction";
        URI controllerURI = new URI("tcp://localhost:9090");
        boolean startAtTail = false;
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
        StreamConfig inputStreamConfig = new StreamConfig("examples", "INPUT_");


        StreamInfo streamInfo;
        try (StreamManager streamManager = StreamManager.create(clientConfig)) {
            streamInfo = streamManager.getStreamInfo(scope, streamName);
        }

        StreamCut startStreamCut = inputStreamConfig.getStartStreamCut();
        if (startStreamCut == StreamCut.UNBOUNDED && startAtTail) {
            startStreamCut = streamInfo.getTailStreamCut();
        }
        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(
                        Stream.of(scope, streamName),
                        startStreamCut,
                        inputStreamConfig.getEndStreamCut())
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        final long timeoutMs = 1000;
        final ObjectMapper mapper = new ObjectMapper();

        // Create a pravega reader and access the data
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
             EventStreamReader<ByteBuffer> reader = clientFactory.createReader("reader",
                     readerGroup,
                     new ByteBufferSerializer(),
                     ReaderConfig.builder().build())) {
            final StreamCutBuilder streamCutBuilder = new StreamCutBuilder(Stream.of(scope, streamName), startStreamCut);
            for (; ; ) {
                EventRead<ByteBuffer> event = reader.readNextEvent(timeoutMs);
                if (event.getEvent() != null) {
                    streamCutBuilder.addEvent(event.getPosition());
                    final StreamCut streamCutForNextEvent = streamCutBuilder.getStreamCut();
                    final Transaction transaction = mapper.readValue(event.getEvent().array(), Transaction.class);
                    log.info("Transaction={}", transaction);
                    log.info("streamCutForNextEvent={}", streamCutForNextEvent);
                    log.info("streamCutForNextEvent={}", streamCutForNextEvent.asText());
                    if (transaction.getImageData() != null) {
                        BufferedImage img = ImageIO.read(new ByteArrayInputStream(transaction.getImageData()));
                        File outputfile = new File("../images/temp/" + transaction.getImageName());
                        log.info(outputfile.getAbsolutePath());
                        outputfile.createNewFile();
                        ImageIO.write(img, "jpg", outputfile);
                    }
                }
            }
        }
    }
}
