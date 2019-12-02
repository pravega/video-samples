/*
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.videoplayer;

import io.pravega.example.common.CommonParams;
import io.pravega.example.common.Utils;
import io.pravega.example.common.VideoFrame;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.connectors.flink.PravegaConfig;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacv.CanvasFrame;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.OpenCVFrameConverter;
import org.bytedeco.opencv.global.opencv_imgcodecs;
import org.bytedeco.opencv.opencv_core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.UUID;

/**
 * Reads video images a Pravega stream and displays them on the screen.
 */
public class PravegaReaderVideoPlayer implements Runnable {
    private static Logger log = LoggerFactory.getLogger(PravegaReaderVideoPlayer.class);

    private final AppConfiguration config;
    private static String scope = "video-demo";
    private static String streamName = "video-objects-detected";
    private static URI controllerURI = null;

    public static void main(String[] args) {
        AppConfiguration config = new AppConfiguration(args);
        log.info("config: {}", config);
        Runnable app = new PravegaReaderVideoPlayer(config);
        CommonParams.init(args);
//        scope = CommonParams.getParam(Constants.SCOPE);
//        streamName = CommonParams.getParam(Constants.STREAM_NAME);
//        controllerURI = URI.create(CommonParams.getParam(Constants.CONTROLLER_URI));

        scope = CommonParams.getScope();
        streamName = CommonParams.getStreamName();
        controllerURI = CommonParams.getControllerURI();


        log.info("#######################     SCOPE   ###################### " + scope);
        log.info("#######################     streamName   ###################### " + streamName);
        log.info("#######################     controllerURI   ###################### " + controllerURI);


        app.run();
    }

    public PravegaReaderVideoPlayer(AppConfiguration appConfiguration) {
        config = appConfiguration;
    }

    public AppConfiguration getConfig() {
        return config;
    }

    public void run() {
        try {

//            TFObjectDetector tfobj = new TFObjectDetector();

            // Initialize camera.
            final int captureWidth = 416; //o
            final int captureHeight =416; //o//o

//            String scope = "examples";
//            String streamName = "video-objects-detected";
//            URI controllerURI = URI.create(CommonParams.getParam(Constants.CONTROLLER_URI));

            // Create client config
            PravegaConfig pravegaConfig = null;

            pravegaConfig = PravegaConfig.fromDefaults()
                    .withControllerURI(controllerURI)
                    .withDefaultScope(scope)
                    .withHostnameValidation(false);

            log.info("==============  pravegaConfig  =============== " + pravegaConfig);

            // create the Pravega input stream (if necessary)
            Stream stream = Utils.createStream(
                    pravegaConfig,
                    streamName);

            ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
            StreamCut startStreamCut = StreamCut.UNBOUNDED;

            try (StreamManager streamManager = StreamManager.create(clientConfig)) {
                startStreamCut = streamManager.getStreamInfo(stream.getScope(), stream.getStreamName()).getTailStreamCut();
            }

           /* StreamInfo streamInfo;
            try (StreamManager streamManager = StreamManager.create(clientConfig)) {
                streamInfo = streamManager.getStreamInfo(scope, streamName);
            }*/
            final long timeoutMs = 1000;
            ObjectMapper mapper = new ObjectMapper();
            log.info("gamma={}", CanvasFrame.getDefaultGamma());
            final CanvasFrame cFrame = new CanvasFrame("Playback from Pravega", 1.0);
            OpenCVFrameConverter.ToMat converter = new OpenCVFrameConverter.ToMat();
            final String readerGroup = UUID.randomUUID().toString().replace("-", "");
            final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(
                            stream,
                            startStreamCut,
                            StreamCut.UNBOUNDED)
                    .build();
            try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig)) {
                readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
            }
            try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
                 EventStreamReader<VideoFrame> reader = clientFactory.createReader("reader",
                         readerGroup,
                         new io.pravega.client.stream.impl.JavaSerializer(),
                         ReaderConfig.builder().build())) {
                int count = 0;

//                TFObjectDetector tfobj = new TFObjectDetector();

                for (;;) {
                    EventRead<VideoFrame> event = reader.readNextEvent(timeoutMs);
                    if (event.getEvent() != null) {
                        VideoFrame videoFrame = event.getEvent();;
                        if (videoFrame.camera == getConfig().getCamera()) {
                            count++;
                            log.info("============== @@@@@  RESULT VIDEO   @@@@@@@@=============== "+videoFrame.frameNumber  );
//                            videoFrame.data = ObjectDetector.getInstance().detectObject( videoFrame.data);
//                            videoFrame.data = TFObjectDetector.getInstance().detect(videoFrame.data);
//                            videoFrame.recognitions = TFObjectDetector.getInstance().printRecognitions();

//                            videoFrame.validateHash();

                            System.out.println(videoFrame.recognitions.toString());

                            Mat pngMat = new Mat(new BytePointer(videoFrame.data));
                            Mat mat = opencv_imgcodecs.imdecode(pngMat, opencv_imgcodecs.IMREAD_UNCHANGED);
                            Frame frame = converter.convert(mat);
                            cFrame.setName("Playback from Pravega");
                            cFrame.setSize(captureWidth, captureHeight);
                            cFrame.showImage(frame);
                            //cFrame.repaint();
                            count++;
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}