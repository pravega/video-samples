package io.pravega.example.persondatabase;

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
        // args: personId, personName, imagePath
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
        byte[] imageBytes=baos.toByteArray();
        log.info("imageName={}", imageName);

        log.info("personId={}, imageName={}, transactionType={}, utcTimestamp={}", personId, imageName, transactionType, utcTimestamp);
    }
}
