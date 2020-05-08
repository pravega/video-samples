package io.pravega.example.persondatabase;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.ByteBufferSerializer;
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;

public class PersonDatabase implements Runnable {
    private static Logger log = LoggerFactory.getLogger(PersonDatabase.class);
    private final AppConfiguration config;
    static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public PersonDatabase(AppConfiguration config) {
        this.config = config;
    }

    public static void main(String... args) {
        AppConfiguration config = new AppConfiguration(args);
        log.info("config: {}", config);
        Runnable app = new PersonDatabase(config);
        app.run();
    }

    public AppConfiguration getConfig() { return config; }


    public void run() {
        if (getConfig().isCreateScope()) {
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

            // args: personId, imagePath, transactionType
            String personId = getConfig().getpersonId();
            File imageFile = new File(getConfig().getimagePath());
            String transactionType = getConfig().gettransactionType();
            String imageName = imageFile.getName();

            final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            final String utcTimestamp = sdf.format(new Date());
            // Decode the image.
            BufferedImage originalImage= null;
            ByteArrayOutputStream baos=new ByteArrayOutputStream();

            try {
                originalImage = ImageIO.read(imageFile);
                ImageIO.write(originalImage, "jpg", baos );
            } catch (IOException e) {
                e.printStackTrace();
            }
            byte[] imageData=baos.toByteArray();
            log.info("imageName={}", imageName);

            Transaction transaction = new Transaction(personId, imageName, imageData, transactionType, utcTimestamp);
            log.info("transaction={}", transaction);

            ByteBuffer jsonBytes = ByteBuffer.wrap(mapper.writeValueAsBytes(transaction));

            CompletableFuture<Void> future = pravegaWriter.writeEvent(personId, jsonBytes);

            future.get();

//            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//
//            transaction = mapper.readValue(jsonBytes.array(), Transaction.class);
//            log.info("transactionBuffer={}", transaction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
