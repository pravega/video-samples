package io.pravega.example.htmlvideoplayer;

import io.pravega.example.common.VideoFrame;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

@Path("data")
public class DataHandler {
    private static final Logger log = LoggerFactory.getLogger(DataHandler.class);

    @GET
    @Produces({"image/png"})
    public byte[] get(@Context Request request) throws Exception {
        try {
            VideoFrame videoFrame = VideoPlayer.getLatestVideoFrame(0);
            return videoFrame.data;
        }
        catch (Exception e) {
            log.error(e.toString());
            throw e;
        }
    }
}
