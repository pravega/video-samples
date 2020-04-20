package io.pravega.example.tensorflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.bytedeco.opencv.opencv_core.Mat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.bytedeco.opencv.global.opencv_imgcodecs.IMREAD_UNCHANGED;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imdecode;

public class Database {

    public Database() {

    }
    public static void main(String[] args) throws IOException, URISyntaxException {
        ObjectMapper mapper = new ObjectMapper();

        List<Person> database = new ArrayList<Person>();
        String[] images = {"/TJ_now.jpg", "/ben_afflek_input_2.jpg"};

        for(String image : images) {
            InputStream inputImageStream = Database.class.getResourceAsStream(image);

            byte[] data = IOUtils.toByteArray(inputImageStream);
            FaceRecognizer recognizer = FaceRecognizer.getInstance();

            List<BoundingBox> recognizedBoxes = recognizer.detectFaces(data);
            Mat imageMat = imdecode(new Mat(data), IMREAD_UNCHANGED);
            data = recognizer.cropFace(recognizedBoxes.get(0), imageMat);

            float[] origEmbedding = recognizer.embeddFace(data);
            Person person = new Person(1, "Thejas", origEmbedding);
            database.add(person);
        }


        mapper.writeValue(new File("./common/src/main/resources/database.json"), database);
    }

}
