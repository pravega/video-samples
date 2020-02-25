package io.pravega.example.htmlvideoplayer;

import io.pravega.example.common.VideoFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;

@Path("data")
public class DataHandler {
    private static final Logger log = LoggerFactory.getLogger(DataHandler.class);

    @GET
    @Produces({"image/png"})
    public Response get(@Context Request request) throws Exception {
        try {
            final VideoFrame videoFrame = VideoPlayer.getLatestVideoFrame(0);
            final EntityTag etag = new EntityTag(videoFrame.timestamp.toString());
            final Response.ResponseBuilder builder = request.evaluatePreconditions(etag);
            if (builder != null) {
                // Return 304 Not Modified
                return builder.build();
            }
            return Response.ok(videoFrame.data).tag(etag).build();
        }
        catch (Exception e) {
            log.error(e.toString());
            throw e;
        }
    }
}
