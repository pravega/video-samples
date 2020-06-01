package io.pravega.example.tensorflow;

import java.awt.image.BufferedImage;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

import javax.imageio.ImageIO;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.zxing.BarcodeFormat;
import com.google.zxing.BinaryBitmap;
import com.google.zxing.EncodeHintType;
import com.google.zxing.MultiFormatReader;
import com.google.zxing.MultiFormatWriter;
import com.google.zxing.NotFoundException;
import com.google.zxing.Result;
import com.google.zxing.WriterException;
import com.google.zxing.client.j2se.BufferedImageLuminanceSource;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.common.HybridBinarizer;
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel;
import org.apache.commons.io.IOUtils;

import static org.bytedeco.opencv.global.opencv_imgcodecs.imencode;

public class QRCode {

    public static void main(String[] args) throws WriterException, IOException,
            NotFoundException {

        // Example QRCode production
        JsonObject identification = new JsonObject();
        identification.addProperty("Id", "Thejas.Vidyasagar@dell.com");

        String qrCodeData = identification.toString();
        String filePath = "./common/src/main/resources/QRCode.png";
        InputStream imgFile = new FileInputStream(filePath);
        byte[] imageBytes = IOUtils.toByteArray(imgFile);

        String charset = "UTF-8"; // or "ISO-8859-1"
        Map hintMap = new HashMap();
        hintMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.L);

        createQRCode(qrCodeData, filePath, charset, 200, 200);
        System.out.println("QR Code image created successfully!");

        String readValue = readQRCode(imageBytes);
        System.out.println("Data read from QR Code: "
                + readValue);

        Gson g = new Gson();
        JsonObject json = g.fromJson(readValue, JsonObject.class);

        String id = json.get("Id").getAsString();

        System.out.println(id);
    }

    /**
     *
     * @param qrCodeData data that is needed to be converted into a QR code
     * @param filePath location where the QR code needs to be stored
     * @param charset which type of charset needs to be used for input data
//     * @param hintMap configuration of QR code generator
     * @param qrCodeheight height of the QR code
     * @param qrCodewidth width of the QR code
     * @throws WriterException
     * @throws IOException
     */
    public static void createQRCode(String qrCodeData, String filePath,
                                    String charset, int qrCodeheight, int qrCodewidth)
            throws WriterException, IOException {
        BitMatrix matrix = new MultiFormatWriter().encode(
                new String(qrCodeData.getBytes(charset), charset),
                BarcodeFormat.QR_CODE, qrCodewidth, qrCodeheight);
        MatrixToImageWriter.writeToFile(matrix, filePath.substring(filePath
                .lastIndexOf('.') + 1), new File(filePath));
    }

    public static String readQRCode(byte[] imageBytes)
            throws IOException, NotFoundException {
        BinaryBitmap binaryBitmap = new BinaryBitmap(new HybridBinarizer(
                new BufferedImageLuminanceSource(
                        ImageIO.read(new ByteArrayInputStream(imageBytes)))));
        Result qrCodeResult = new MultiFormatReader().decode(binaryBitmap);
//        Result qrCodeResult = new MultiFormatReader().decode(binaryBitmap, hintMap);
        return qrCodeResult.getText();
    }
}
