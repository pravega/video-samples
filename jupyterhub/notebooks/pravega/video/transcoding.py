import base64
import cv2
import json
import logging
from itertools import islice
import numpy as np
import grpc
import pravega.grpc_gateway as pgw
from pravega.video import UnindexedStream, OutputStream


class Transcoder():
    def __init__(self,
                 input_scope,
                 input_stream,
                 input_gateway,
                 output_scope,
                 output_stream,
                 output_gateway,
                 max_events=-1,
                 print_size=100,
                 ):
        self.input_stream = input_stream
        self.input_gateway = input_gateway
        self.output_scope = output_scope
        self.output_stream = output_stream
        self.output_gateway = output_gateway
        self.max_events = max_events
        self.print_size = print_size
        self.input_scope = input_scope

    def run(self, delete=False):
        with grpc.insecure_channel(self.input_gateway) as input_pravega_channel:
            input_pravega_client = pgw.grpc.PravegaGatewayStub(input_pravega_channel)
            input_stream = UnindexedStream(input_pravega_client, self.input_scope, self.input_stream)
            with grpc.insecure_channel(self.output_gateway) as output_pravega_channel:
                output_pravega_client = pgw.grpc.PravegaGatewayStub(output_pravega_channel)
                output_stream = OutputStream(output_pravega_client, self.output_scope, self.output_stream)
                if delete:
                    output_stream.delete_stream()
                output_stream.create_stream()
                read_events = input_stream.read_events(stop_at_tail=True)
                events_to_write = (self.transcode_event(read_event) for read_event in read_events)
                if self.max_events >= 0:
                    events_to_write = islice(events_to_write, self.max_events)
                output_stream.write_events(events_to_write)

    def transcode_event(self, read_event):
        # Decode video frame
        event_json = read_event.event
        video_frame = json.loads(event_json)
        input_data_len = len(video_frame['data'])
        routing_key = str(video_frame['camera'])
        image_file_bytes = base64.b64decode(video_frame['data'])
        image_file_array = np.frombuffer(image_file_bytes, dtype=np.uint8)
        image_array = cv2.imdecode(image_file_array, cv2.IMREAD_UNCHANGED)

        # Encode video frame
        success, output_image_file_array = cv2.imencode('.jpg', image_array)
        video_frame['data'] = base64.b64encode(output_image_file_array.tobytes()).decode(encoding='UTF-8')
        output_data_len = len(video_frame['data'])
        output_event_json = json.dumps(video_frame)
        output_event_bytes = output_event_json.encode(encoding='UTF-8')
        event_to_write = pgw.pb.WriteEventsRequest(
            scope=self.output_scope,
            stream=self.output_stream,
            event=output_event_bytes,
            routing_key=routing_key,
        )

        # Log
        to_log_dict = video_frame.copy()
        del to_log_dict['data']
        to_log_dict['input_data'] = '(%d bytes)' % input_data_len
        to_log_dict['output_data'] = '(%d bytes)' % output_data_len
        logging.debug('transcode_event: ' + json.dumps(to_log_dict))

        return event_to_write
