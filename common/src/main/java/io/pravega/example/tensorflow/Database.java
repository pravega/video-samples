package io.pravega.example.tensorflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.bytedeco.opencv.opencv_core.Mat;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.bytedeco.opencv.global.opencv_imgcodecs.IMREAD_UNCHANGED;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imdecode;

public class Database {

    public List<Person> database = new ArrayList<>();

    public Database() {
    }
    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        Database db = new Database();

        Map<String, String> imagesOfPeople = new HashMap<>();

        imagesOfPeople.put("Thejas Vidyasagar", "/TJ_now.jpg");
        imagesOfPeople.put("Ben Afflek", "/ben_afflek_input_2.jpg");
        imagesOfPeople.put("Srikanth Satya", "/srikanth_satya.jpg");
        imagesOfPeople.put("Ashish Batwara", "/ashish_batwara.jpg");

        for(String name: imagesOfPeople.keySet()) {
            db.addPerson(name, imagesOfPeople.get(name));
        }


        mapper.writeValue(new File("./common/src/main/resources/database.json"), db.database);
    }

    public void addPerson(String name, String imageName) throws Exception {
        try {
            InputStream inputImageStream = Database.class.getResourceAsStream(imageName);

            byte[] data = IOUtils.toByteArray(inputImageStream);
            FaceRecognizer recognizer = new FaceRecognizer();

            List<BoundingBox> recognizedBoxes = recognizer.detectFaces(data);
            Mat imageMat = imdecode(new Mat(data), IMREAD_UNCHANGED);
            data = recognizer.cropFace(recognizedBoxes.get(0), imageMat);

            float[] origEmbedding = recognizer.embeddFace(data);
            Person person = new Person(1, name, origEmbedding);
            database.add(person);
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

}
