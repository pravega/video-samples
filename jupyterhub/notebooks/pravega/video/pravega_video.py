
import IPython
import base64
import cv2
import json
import numpy as np
import pandas as pd
import pravega.grpc_gateway as pravega
from matplotlib import pyplot as plt
import time


def ignore_non_events(read_events):
    for read_event in read_events:
        if len(read_event.event) > 0:
            yield read_event


def opencv_image_to_mpl(img):
    return cv2.cvtColor(img, cv2.COLOR_BGR2RGB)


class StreamBase():
    def __init__(self, pravega_client, scope, stream, create=False):
        self.pravega_client = pravega_client
        self.scope = scope
        self.stream = stream
        if create:
            self.create_stream()

    def create_stream(self, min_num_segments=1):
        return self.pravega_client.CreateStream(pravega.pb.CreateStreamRequest(
            scope=self.scope,
            stream=self.stream,
            scaling_policy=pravega.pb.ScalingPolicy(min_num_segments=min_num_segments),
        ))

    def get_stream_info(self):
        return self.pravega_client.GetStreamInfo(pravega.pb.GetStreamInfoRequest(
            scope=self.scope,
            stream=self.stream,
        ))


class OutputStream(StreamBase):
    def __init__(self, pravega_client, scope, stream, create=True):
        super(OutputStream, self).__init__(pravega_client, scope, stream, create)

    def write_video_from_file(self, filename, crop=None):
        cap = cv2.VideoCapture(filename)
        video_frames = self.opencv_video_frame_generator(cap)
        cropped_video_frames = (self.cropped_video_frame(f, crop) for f in video_frames)
        events_to_write = self.video_frame_write_generator(cropped_video_frames)
        write_response = self.pravega_client.WriteEvents(events_to_write)
        return write_response

    def opencv_video_frame_generator(self, vidcap):
        while True:
            pos_frames = vidcap.get(cv2.CAP_PROP_POS_FRAMES)
            success, image = vidcap.read()
            if not success:
                return
            video_frame = dict(
                image=image,
                frameNumber=int(pos_frames),
                timestamp=int(time.time() * 1000),
            )
            yield video_frame

    def cropped_video_frame(self, video_frame, crop):
        if crop:
            left, top, right, bottom = crop
            video_frame['image'] = video_frame['image'][top:bottom, left:right]
        return video_frame

    def video_frame_write_generator(self, video_frame_iter, camera=0):
        for video_frame in video_frame_iter:
            event_dict = video_frame.copy()
            event_dict['camera'] = camera
            event_dict['ssrc'] = 0

            success, png_array = cv2.imencode('.png', video_frame['image'])
            event_dict['data'] = base64.b64encode(png_array.tobytes()).decode(encoding='UTF-8')
            del event_dict['image']

            to_log_dict = event_dict.copy()
            to_log_dict['data'] = '(%d bytes)' % len(event_dict['data'])
            # print('video_frame_write_generator: ' + json.dumps(to_log_dict))

            event_json = json.dumps(event_dict)
            event_bytes = event_json.encode(encoding='UTF-8')

            event_to_write = pravega.pb.WriteEventsRequest(
                scope=self.scope,
                stream=self.stream,
                event=event_bytes,
                routing_key=str(camera),
            )
            yield event_to_write


class UnindexedStream(StreamBase):
    def __init__(self, pravega_client, scope, stream):
        super(UnindexedStream, self).__init__(pravega_client, scope, stream)

    def read_events(self, from_stream_cut=None, to_stream_cut=None):
        read_events_request = pravega.pb.ReadEventsRequest(
            scope=self.scope,
            stream=self.stream,
            from_stream_cut=from_stream_cut,
            to_stream_cut=to_stream_cut,
        )
        return ignore_non_events(self.pravega_client.ReadEvents(read_events_request))

    def read_event_to_video_frame(self, read_event):
        event_json = read_event.event
        video_frame = json.loads(event_json)
        image_png = base64.b64decode(video_frame['data'])
        del video_frame['data']
        image_png_array = np.frombuffer(image_png, dtype=np.uint8)
        image_array = cv2.imdecode(image_png_array, cv2.IMREAD_UNCHANGED)
        video_frame['image_array'] = image_array
        video_frame['timestamp'] = pd.to_datetime(video_frame['timestamp'], unit='ms', utc=True)
        return video_frame

    def read_video_frames(self, from_stream_cut=None, to_stream_cut=None):
        read_events = self.read_events(from_stream_cut, to_stream_cut)
        return (self.read_event_to_video_frame(read_event) for read_event in read_events)

    def play_video(self, from_stream_cut=None, to_stream_cut=None, show_frame_interval=1):
        read_events = self.read_video_frames(from_stream_cut, to_stream_cut)
        for i, video_frame in enumerate(read_events):
            if i % show_frame_interval == 0:
                IPython.display.clear_output(wait=True)
                plt.title('frameNumber=%d, timestamp=%s' % (video_frame['frameNumber'], video_frame['timestamp']))
                plt.imshow(opencv_image_to_mpl(video_frame['image_array']));
                plt.show()


class IndexedStream():
    def __init__(self, pravega_client, scope, stream, from_stream_cut=None, timestamp_col='timestamp'):
        self.pravega_client = pravega_client
        self.scope = scope
        self.stream = stream
        self.from_stream_cut = from_stream_cut
        self.timestamp_col = timestamp_col
        self.index_df = None

    def build_index(self):
        stream_info = self.pravega_client.GetStreamInfo(
            pravega.pb.GetStreamInfoRequest(scope=self.scope, stream=self.stream))
        # print('stream_info=%s' % str(stream_info))
        from_stream_cut = stream_info.head_stream_cut if self.from_stream_cut is None else self.from_stream_cut
        to_stream_cut = stream_info.tail_stream_cut
        read_events_request = pravega.pb.ReadEventsRequest(
            scope=self.scope,
            stream=self.stream,
            from_stream_cut=from_stream_cut,
            to_stream_cut=to_stream_cut,
        )
        # print(read_events_request)
        read_events = ignore_non_events(self.pravega_client.ReadEvents(read_events_request))
        index_list = [self.read_event_to_index(read_event) for read_event in read_events]
        df = pd.DataFrame(index_list)
        df[self.timestamp_col] = pd.to_datetime(df[self.timestamp_col], unit='ms', utc=True)
        df = df.set_index([self.timestamp_col])
        df['from_stream_cut'] = df.to_stream_cut.shift(1)
        df.from_stream_cut.iloc[0] = from_stream_cut.text
        df = df[['from_stream_cut','to_stream_cut','event_pointer']]
        self.index_df = df

    def read_event_to_index(self, read_event, index_col='timestamp'):
        event_json = read_event.event
        event = json.loads(event_json)
        return {
            index_col: event[index_col],
            'to_stream_cut': read_event.stream_cut.text,
            'event_pointer': read_event.event_pointer.bytes,
        }

    def save_to_file(self, filename):
        self.index_df.to_pickle(filename)

    def get_stream_cuts_for_time_window(self, ts0, ts1=None, count=None):
        """Get a pair of from and to stream cuts that can be used to read events starting at timestamp ts0 (inclusive).
        If count is specified, this many events will be read.
        Otherwise, reading will continue until timestamp ts1 (inclusive)."""
        from_index = self.index_df.index.searchsorted(ts0, side='left')
        if count is None:
            if ts1 is None: ts1 = ts0
            to_index = self.index_df.index.searchsorted(ts1, side='right') - 1
        else:
            to_index = from_index + count - 1
        from_index_record = self.index_df.iloc[from_index]
        to_index_record = self.index_df.iloc[to_index]
        from_stream_cut = pravega.pb.StreamCut(text=from_index_record.from_stream_cut)
        to_stream_cut = pravega.pb.StreamCut(text=to_index_record.to_stream_cut)
        return pd.Series(dict(
            from_timestamp=from_index_record.name,
            to_timestamp=to_index_record.name,
            from_stream_cut=from_stream_cut,
            to_stream_cut=to_stream_cut))

    def read_events_in_time_window(self, ts0, ts1):
        stream_cuts = self.get_stream_cuts_for_time_window(ts0, ts1)
        read_events_request = pravega.pb.ReadEventsRequest(
            scope=self.scope,
            stream=self.stream,
            from_stream_cut=stream_cuts.from_stream_cut,
            to_stream_cut=stream_cuts.to_stream_cut,
        )
        return ignore_non_events(self.pravega_client.ReadEvents(read_events_request))

    def read_event_to_video_frame(self, read_event):
        event_json = read_event.event
        video_frame = json.loads(event_json)
        image_png = base64.b64decode(video_frame['data'])
        del video_frame['data']
        image_png_array = np.frombuffer(image_png, dtype=np.uint8)
        image_array = cv2.imdecode(image_png_array, cv2.IMREAD_UNCHANGED)
        video_frame['image_array'] = image_array
        video_frame['timestamp'] = pd.to_datetime(video_frame['timestamp'], unit='ms', utc=True)
        return video_frame

    def read_video_frames_in_time_window(self, ts0, ts1):
        read_events = self.read_events_in_time_window(ts0, ts1)
        return [self.read_event_to_video_frame(read_event) for read_event in read_events]

    def play_video(self, ts0, ts1, show_frame_interval=1):
        read_events = self.read_events_in_time_window(ts0, ts1)
        i = 0
        for read_event in read_events:
            video_frame = self.read_event_to_video_frame(read_event)
            if i % show_frame_interval == 0:
                IPython.display.clear_output(wait=True)
                plt.title('frameNumber=%d, timestamp=%s' % (video_frame['frameNumber'], video_frame['timestamp']))
                plt.imshow(opencv_image_to_mpl(video_frame['image_array']));
                plt.show()
            i += 1

    def get_multiple_video_frames(self, timestamp, count=1):
        stream_cuts = self.get_stream_cuts_for_time_window(timestamp, count=count)
        read_events_request = pravega.pb.ReadEventsRequest(
            scope=self.scope,
            stream=self.stream,
            from_stream_cut=stream_cuts.from_stream_cut,
            to_stream_cut=stream_cuts.to_stream_cut,
        )
        read_events = ignore_non_events(self.pravega_client.ReadEvents(read_events_request))
        video_frames = [self.read_event_to_video_frame(e) for e in read_events]
        return pd.DataFrame(video_frames)

    def get_single_video_frame(self, timestamp):
        return self.get_multiple_video_frames(timestamp, count=1).iloc[0]

    def show_video_frame(self, video_frame):
        plt.title('frameNumber=%d, timestamp=%s' % (video_frame['frameNumber'], video_frame['timestamp']))
        plt.imshow(opencv_image_to_mpl(video_frame['image_array']));
