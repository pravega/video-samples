package io.pravega.example.persondatabase;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.example.common.PravegaAppConfiguration;
import io.pravega.example.common.PravegaUtil;
import io.pravega.example.common.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

/*
    This class adds data to the embeddings database using input
 */

public class PersonDatabase implements Runnable {
    private static Logger log = LoggerFactory.getLogger(PersonDatabase.class);
    private final AppConfiguration config;

    public PersonDatabase(AppConfiguration config) {
        this.config = config;
    }

    public static void main(String... args) {
        AppConfiguration config = new AppConfiguration(args);
        log.info("config: {}", config);
        Runnable app = new PersonDatabase(config);
        app.run();
    }

    public AppConfiguration getConfig() {
        return config;
    }


    public void run() {
        if (PravegaAppConfiguration.isCreateScope()) {
            try (StreamManager streamManager = StreamManager.create(getConfig().getClientConfig())) {
                streamManager.createScope(getConfig().getDefaultScope());
            }
        }

        // Create Pravega stream.
        PravegaUtil.createStream(getConfig().getClientConfig(), getConfig().getOutputStreamConfig());

        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(
                getConfig().getOutputStreamConfig().getStream().getScope(),
                getConfig().getClientConfig());
             EventStreamWriter<ByteBuffer> pravegaWriter = clientFactory.createEventWriter(
                     getConfig().getOutputStreamConfig().getStream().getStreamName(),
                     new ByteBufferSerializer(),
                     EventWriterConfig.builder().build())) {

            ObjectMapper mapper = new ObjectMapper();

            // args
            String personId = getConfig().getpersonId();
            String transactionType = getConfig().gettransactionType();
            String imageName = "";

            Timestamp timestamp = new Timestamp(System.currentTimeMillis());

            // Decode the image.
            BufferedImage originalImage = null;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            byte[] imageData = null;
            if (transactionType.equals("add")) {
                File imageFile = new File(getConfig().getimagePath());
                imageName = imageFile.getName();
                originalImage = ImageIO.read(imageFile);
                ImageIO.write(originalImage, "jpg", baos);
                imageData = baos.toByteArray();
                log.info("imageName={}", imageName);
            }

            Transaction transaction = new Transaction(personId, imageName, imageData, transactionType, timestamp);
            log.info("transaction={}", transaction);

            ByteBuffer jsonBytes = ByteBuffer.wrap(mapper.writeValueAsBytes(transaction));

            // Write to pravega
            CompletableFuture<Void> future = pravegaWriter.writeEvent(personId, jsonBytes);

            future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
