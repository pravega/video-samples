package io.pravega.example.camerarecorder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class PersonDatabaseConnection implements Runnable {
    private static Logger log = LoggerFactory.getLogger(PersonDatabaseConnection.class);
    private final AppConfiguration config;
    static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public PersonDatabaseConnection(AppConfiguration config) {
        this.config = config;
    }

    public static void main(String... args) {
        AppConfiguration config = new AppConfiguration(args);
        log.info("config: {}", config);
        Runnable app = new PersonDatabaseConnection(config);
        app.run();
    }

    public AppConfiguration getConfig() { return config; }


    public void run() {
        // args: personId, personName, imagePath
        int personId = 1;
        String personName = "Thejas";
        File imageFile = new File("/home/vidyat/Desktop/video-samples/camera-recorder/src/main/resources/TJ_now.jpg");
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
        byte[] imageBytes=baos.toByteArray();
        log.info("imageName={}", imageName);

        log.info("personId={}, personName={}, imageName={}, utcTimestamp={}", personId, personName, imageName, utcTimestamp);
    }
}
