
import IPython
import base64
import cv2
import io
import json
import numpy as np
import pandas as pd
from PIL import Image, ImageDraw, ImageFont
import pravega.grpc_gateway as pravega
from matplotlib import pyplot as plt
import time
import logging
from itertools import islice


def ignore_non_events(read_events):
    for read_event in read_events:
        if len(read_event.event) > 0:
            yield read_event


def opencv_image_to_mpl(img):
    return cv2.cvtColor(img, cv2.COLOR_BGR2RGB)


def generate_image_bytes(width, height, camera, frame_number, random=True, format='JPEG', quality=100, subsampling=0, compress_level=0):
    """Generate an image in JPEG for PNG format.
    format: 'JPEG' or 'PNG'
    quality: 0-100, for JPEG only
    subsampling: 0,1,2 for JPEG only
    compress_level: 0,1 for PNG only
    """
    font_size = int(min(width, height) * 0.15)
    if random:
        img_array = np.random.rand(height, width, 3) * 255
        img = Image.fromarray(img_array.astype('uint8')).convert('RGB')
    else:
        img = Image.new('RGB', (width, height), (25, 25, 240, 0))
    font = ImageFont.truetype('/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf', font_size)
    draw = ImageDraw.Draw(img)
    draw.text((width//10, height//10), 'CAMERA\n %03d\nFRAME\n%05d' % (camera, frame_number), font=font, align='center')
    out_bytesio = io.BytesIO()
    img.save(out_bytesio, format=format, quality=quality, subsampling=subsampling, compress_level=compress_level)
    out_bytes = out_bytesio.getvalue()
    return out_bytes


class StreamBase():
    def __init__(self, pravega_client, scope, stream):
        self.pravega_client = pravega_client
        self.scope = scope
        self.stream = stream

    def create_stream(self, min_num_segments=1):
        return self.pravega_client.CreateStream(pravega.pb.CreateStreamRequest(
            scope=self.scope,
            stream=self.stream,
            scaling_policy=pravega.pb.ScalingPolicy(min_num_segments=min_num_segments),
        ))

    def delete_stream(self):
        return self.pravega_client.DeleteStream(pravega.pb.DeleteStreamRequest(
            scope=self.scope,
            stream=self.stream,
        ))

    def get_stream_info(self):
        return self.pravega_client.GetStreamInfo(pravega.pb.GetStreamInfoRequest(
            scope=self.scope,
            stream=self.stream,
        ))

    def truncate_stream(self):
        return self.pravega_client.TruncateStream(pravega.pb.TruncateStreamRequest(
            scope=self.scope,
            stream=self.stream,
            stream_cut=self.get_stream_info().tail_stream_cut,
        ))

    def write_events(self, events_to_write):
        return self.pravega_client.WriteEvents(events_to_write)


class OutputStream(StreamBase):
    def __init__(self, pravega_client, scope, stream):
        super(OutputStream, self).__init__(pravega_client, scope, stream)

    def write_video_from_file(self, filename, crop=None, size=None, fps=None):
        cap = cv2.VideoCapture(filename)
        video_frames = self.opencv_video_frame_generator(cap, fps=fps)
        cropped_video_frames = (self.cropped_video_frame(f, crop) for f in video_frames)
        resized_video_frames = (self.resized_video_frame(f, size) for f in cropped_video_frames)
        events_to_write = self.video_frame_write_generator(resized_video_frames)
        write_response = self.pravega_client.WriteEvents(events_to_write)
        return write_response

    def opencv_video_frame_generator(self, vidcap, fps=None):
        if fps is None:
            fps = vidcap.get(cv2.CAP_PROP_FPS)
        frame_number = 0
        t0_ms = time.time() * 1000.0
        while True:
            success, image = vidcap.read()
            if not success:
                return
            timestamp = int(frame_number / (fps / 1000.0) + t0_ms)
            sleep_sec = timestamp / 1000.0 - time.time()
            if sleep_sec > 0.0:
                time.sleep(sleep_sec)
            elif sleep_sec < -5.0:
                logging.warn(f"opencv_video_frame_generator can't keep up with real-time. sleep_sec={sleep_sec}")
            logging.info(str(dict(frameNumber=frame_number, timestamp=timestamp)))
            video_frame = dict(
                image_array=image,
                frameNumber=frame_number,
                timestamp=timestamp,
            )
            yield video_frame
            frame_number += 1

    def cropped_video_frame(self, video_frame, crop):
        if crop:
            left, top, right, bottom = crop
            video_frame['image_array'] = video_frame['image_array'][top:bottom, left:right]
        return video_frame

    def resized_video_frame(self, video_frame, size):
        if size:
            video_frame['image_array'] = cv2.resize(video_frame['image_array'], size, interpolation=cv2.INTER_NEAREST)
        return video_frame

    def video_frame_write_generator(self, video_frame_iter, camera=0):
        for video_frame in video_frame_iter:
            event_dict = video_frame.copy()
            event_dict['camera'] = camera
            event_dict['ssrc'] = 0

            success, png_array = cv2.imencode('.jpg', video_frame['image_array'])
            event_dict['data'] = base64.b64encode(png_array.tobytes()).decode(encoding='UTF-8')
            del event_dict['image_array']

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


class VideoDataGenerator(StreamBase):
    def __init__(self, pravega_client, scope, stream, camera=0, frames_per_sec=1.0, verbose=False, **kwargs):
        super(VideoDataGenerator, self).__init__(pravega_client, scope, stream)
        self.camera = camera
        self.frames_per_sec = frames_per_sec
        self.verbose = verbose
        self.generate_image_bytes_kwargs = kwargs

    def write_generated_video(self, num_frames=0):
        frame_numbers = self.frame_number_generator()
        events_to_write = self.video_frame_write_generator(frame_numbers)
        if num_frames > 0:
            events_to_write = islice(events_to_write, num_frames)
        write_response = self.pravega_client.WriteEvents(events_to_write)
        return write_response

    def frame_number_generator(self):
        frame_number = 0
        t0_ms = time.time() * 1000.0
        while True:
            timestamp = int(frame_number / (self.frames_per_sec / 1000.0) + t0_ms)
            sleep_sec = timestamp / 1000.0 - time.time()
            if sleep_sec > 0.0:
                time.sleep(sleep_sec)
            elif sleep_sec < -5.0:
                logging.warn(f"Data generator can't keep up with real-time. sleep_sec={sleep_sec}")
            yield dict(timestamp=timestamp, frameNumber=frame_number)
            frame_number += 1

    def generate_image_bytes(self, frame_number):
        return generate_image_bytes(camera=self.camera, frame_number=frame_number, **self.generate_image_bytes_kwargs)

    def video_frame_write_generator(self, frame_number_iter):
        for video_frame in frame_number_iter:
            event_dict = video_frame.copy()
            event_dict['camera'] = self.camera
            event_dict['ssrc'] = 0
            image_array = self.generate_image_bytes(video_frame['frameNumber'])
            event_dict['data'] = base64.b64encode(image_array).decode(encoding='UTF-8')
            if self.verbose:
                to_log_dict = event_dict.copy()
                to_log_dict['data'] = '(%d bytes)' % len(event_dict['data'])
                print('video_frame_write_generator: ' + json.dumps(to_log_dict))
            event_json = json.dumps(event_dict)
            event_bytes = event_json.encode(encoding='UTF-8')
            event_to_write = pravega.pb.WriteEventsRequest(
                scope=self.scope,
                stream=self.stream,
                event=event_bytes,
                routing_key=str(self.camera),
            )
            yield event_to_write


class UnindexedStream(StreamBase):
    def __init__(self, pravega_client, scope, stream):
        super(UnindexedStream, self).__init__(pravega_client, scope, stream)

    def read_events(self, from_stream_cut=None, to_stream_cut=None, stop_at_tail=False):
        if stop_at_tail:
            to_stream_cut = self.get_stream_info().tail_stream_cut
        read_events_request = pravega.pb.ReadEventsRequest(
            scope=self.scope,
            stream=self.stream,
            from_stream_cut=from_stream_cut,
            to_stream_cut=to_stream_cut,
        )
        return ignore_non_events(self.pravega_client.ReadEvents(read_events_request))

    def read_event_to_video_frame(self, read_event, decode=True):
        event_json = read_event.event
        video_frame = json.loads(event_json)
        video_frame['timestamp'] = pd.to_datetime(video_frame['timestamp'], unit='ms', utc=True)
        video_frame['data'] = base64.b64decode(video_frame['data'])
        if decode:
            image_png_array = np.frombuffer(video_frame['data'], dtype=np.uint8)
            image_array = cv2.imdecode(image_png_array, cv2.IMREAD_UNCHANGED)
            video_frame['image_array'] = image_array
            del video_frame['data']
        return video_frame

    def read_video_frames(self, from_stream_cut=None, to_stream_cut=None, stop_at_tail=False, decode=True):
        read_events = self.read_events(from_stream_cut, to_stream_cut, stop_at_tail=stop_at_tail)
        return (self.read_event_to_video_frame(read_event, decode=decode) for read_event in read_events)

    def play_video(self, from_stream_cut=None, to_stream_cut=None, show_frame_interval=1, figsize=None):
        read_events = self.read_video_frames(from_stream_cut, to_stream_cut)
        for i, video_frame in enumerate(read_events):
            if i % show_frame_interval == 0:
                IPython.display.clear_output(wait=True)
                fig = plt.figure(figsize=figsize)
                plt.title('frameNumber=%d, timestamp=%s' % (video_frame['frameNumber'], video_frame['timestamp']))
                plt.imshow(opencv_image_to_mpl(video_frame['image_array']));
                plt.show()


class IndexStream(StreamBase):
    """Represents a Pravega Stream that stores a Stream Cut index for another Pravega Stream."""
    def index_record_write_generator(self, index_records):
        for index_record in index_records:
            rec = index_record.copy()
            rec['event_pointer'] = base64.b64encode(rec['event_pointer']).decode(encoding='UTF-8')
            rec_bytes = json.dumps(rec).encode(encoding='UTF-8')
            event_to_write = pravega.pb.WriteEventsRequest(
                scope=self.scope,
                stream=self.stream,
                event=rec_bytes,
                routing_key='',
            )
            yield event_to_write

    def append_index_records(self, index_records, truncate=False):
        if truncate:
            self.truncate_stream()
        events_to_write = self.index_record_write_generator(index_records)
        self.pravega_client.WriteEvents(events_to_write)

    def read_index_records(self):
        to_stream_cut = self.get_stream_info().tail_stream_cut
        read_events_request = pravega.pb.ReadEventsRequest(
            scope=self.scope,
            stream=self.stream,
            from_stream_cut=None,
            to_stream_cut=to_stream_cut,
        )
        return (json.loads(read_event.event) for read_event in ignore_non_events(self.pravega_client.ReadEvents(read_events_request)))


# TODO: This should be based on UnindexedStream
class IndexedStream(StreamBase):
    def __init__(self, pravega_client, scope, stream, timestamp_col='timestamp', exclude_cols=('data',)):
        super(IndexedStream, self).__init__(pravega_client, scope, stream)
        self.pravega_client = pravega_client
        self.scope = scope
        self.stream = stream
        self.timestamp_col = timestamp_col
        self.exclude_cols = exclude_cols
        self.index_df = None
        self.unindexed_stream =  UnindexedStream(pravega_client, scope, stream)
        self.index_stream = IndexStream(pravega_client, scope, '%s-index' % stream)
        self.index_stream.create_stream()

    def update_index(self, force_full=False):
        stream_info = self.get_stream_info()
        if self.index_df is None and not force_full:
            self.load_index()
        if self.index_df is None or force_full:
            print('update_index: Performing full index generation')
            from_stream_cut = stream_info.head_stream_cut
            truncate = True
        else:
            print('update_index: Performing incremental index update')
            from_stream_cut = pravega.pb.StreamCut(text=self.index_df.iloc[-1].to_stream_cut)
            truncate = False
        to_stream_cut = stream_info.tail_stream_cut
        read_events_request = pravega.pb.ReadEventsRequest(
            scope=self.scope,
            stream=self.stream,
            from_stream_cut=from_stream_cut,
            to_stream_cut=to_stream_cut,
        )
        print('update_index: Reading stream %s and writing index stream %s' % (self.stream, self.index_stream.stream))
        read_events = ignore_non_events(self.pravega_client.ReadEvents(read_events_request))
        index_records = self.index_record_generator(read_events, from_stream_cut.text)
        self.index_stream.append_index_records(index_records, truncate=truncate)
        self.load_index()

    def index_record_generator(self, read_events, from_stream_cut):
        for read_event in read_events:
            rec = self.read_event_to_index(read_event)
            rec['from_stream_cut'] = from_stream_cut
            from_stream_cut = rec['to_stream_cut']
            yield rec

    def read_event_to_index(self, read_event):
        """Convert an event to a record that will be written to the index stream."""
        event_json = read_event.event
        event = json.loads(event_json)
        for col in self.exclude_cols:
            if col in event: del event[col]
        event['to_stream_cut'] = read_event.stream_cut.text
        event['event_pointer'] = read_event.event_pointer.bytes
        return event

    def load_index(self):
        print('load_index: Reading index stream %s' % self.index_stream.stream)
        index_records = list(self.index_stream.read_index_records())
        if len(index_records) > 0:
            df = pd.DataFrame(index_records)
            df[self.timestamp_col] = pd.to_datetime(df[self.timestamp_col], unit='ms', utc=True)
            df = df.set_index([self.timestamp_col])
            self.index_df = df

    def get_stream_cuts_for_time_window(self, ts0, ts1=None, count=None):
        """Get a pair of from and to stream cuts that can be used to read events starting at timestamp ts0 (inclusive).
        If count is specified, this many events will be read.
        Otherwise, reading will continue until timestamp ts1 (inclusive)."""
        from_index = self.index_df.index.searchsorted(ts0, side='left')
        if count is None:
            if ts1 is None: ts1 = ts0
            to_index = self.index_df.index.searchsorted(ts1, side='right') - 1
            if to_index < 0: raise Exception('End timestamp %s not found' % str(ts1))
        else:
            to_index = from_index + count - 1
        from_index_record = self.index_df.iloc[from_index]
        to_index_record = self.index_df.iloc[to_index]
        from_stream_cut = pravega.pb.StreamCut(text=from_index_record.from_stream_cut)
        to_stream_cut = pravega.pb.StreamCut(text=to_index_record.to_stream_cut)
        return pd.Series(dict(
            from_index=from_index,
            to_index=to_index,
            from_timestamp=from_index_record.name,
            to_timestamp=to_index_record.name,
            from_stream_cut=from_stream_cut,
            to_stream_cut=to_stream_cut))

    def get_event_pointer_for_timestamp(self, timestamp):
        from_index = self.index_df.index.searchsorted(timestamp, side='left')
        from_index_record = self.index_df.iloc[from_index]
        return from_index_record

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
        return (self.read_event_to_video_frame(read_event) for read_event in read_events)

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
        video_frames = (self.read_event_to_video_frame(e) for e in read_events)
        return pd.DataFrame(video_frames)

    def get_single_video_frame(self, timestamp):
        event_pointer = self.get_event_pointer_for_timestamp(timestamp)
        fetch_event_request = pravega.pb.FetchEventRequest(
            scope=self.scope,
            stream=self.stream,
            event_pointer=pravega.pb.EventPointer(bytes=event_pointer.event_pointer),
        )
        fetch_event_response = self.pravega_client.FetchEvent(fetch_event_request)
        return pd.Series(self.read_event_to_video_frame(fetch_event_response))

    def get_single_video_frame_by_index(self, index):
        index_rec = self.index_df.iloc[index]
        event_pointer_bytes = base64.b64decode(index_rec.event_pointer)
        fetch_event_request = pravega.pb.FetchEventRequest(
            scope=self.scope,
            stream=self.stream,
            event_pointer=pravega.pb.EventPointer(bytes=event_pointer_bytes),
        )
        fetch_event_response = self.pravega_client.FetchEvent(fetch_event_request)
        d = self.read_event_to_video_frame(fetch_event_response)
        for k,v in index_rec.iteritems(): d[k] = v
        return pd.Series(d)

    def show_video_frame(self, video_frame):
        plt.title('frameNumber=%d, timestamp=%s' % (video_frame['frameNumber'], video_frame['timestamp']))
        plt.imshow(opencv_image_to_mpl(video_frame['image_array']));
