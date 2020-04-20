import grpc
import pravega.grpc_gateway as pravega
from pravega.video import UnindexedStream, OutputStream
from pathlib import Path
import pandas as pd
import base64
from multiprocessing import Process
import json
import time
import logging


class KittiFleetSimulator():
    def __init__(self, data_dirs, **kwargs):
        self.car_simulators = [KittiCarSimulator(car_id=car_id, data_dir=data_dir, **kwargs) for
                               car_id, data_dir in enumerate(data_dirs)]

    def run(self):
        try:
            for s in self.car_simulators: s.start()
            for s in self.car_simulators: s.join()
        except KeyboardInterrupt:
            print('KittiFleetSimulator: run: stopping...')
            for s in self.car_simulators: s.kill()
            for s in self.car_simulators: s.join()
            print('KittiFleetSimulator: run: stopped')


class KittiCarSimulator():
    def __init__(self, gateway, scope, video_stream_name, sensor_stream_name, data_dir, car_id=0, format='jpg'):
        self.gateway = gateway
        self.scope = scope
        self.video_stream_name = video_stream_name
        self.sensor_stream_name = sensor_stream_name
        self.data_dir = data_dir
        self.car_id = car_id
        self.format = format
        self.ssrc = 0
        self.data_path = Path(self.data_dir)
        with open(self.data_path / 'oxts/dataformat.txt','rt') as f:
            self.oxts_col_names = [s.split(':')[0] for s in f.readlines()]
        self.video_df = self.read_timestamps(self.data_path / 'image_02' / 'timestamps.txt')
        self.oxts_df = self.read_timestamps(self.data_path / 'oxts' / 'timestamps.txt')
        assert len(self.video_df) == len(self.oxts_df)
        self.recorded_t0 = self.video_df.timestamp.iloc[0]
        event_interval_sec = (self.video_df.timestamp.shift(-1) - self.video_df.timestamp).mean()
        next_period_t0 = self.video_df.timestamp.iloc[-1] + event_interval_sec
        self.period_sec = next_period_t0 - self.recorded_t0
        print(f'event_interval_sec={event_interval_sec}, period_sec={self.period_sec}')
        self.processes = []
        self.wall_clock_t0 = None

    def read_timestamps(self, filename):
        df = pd.read_csv(filename, header=None, names=['timestamp'])
        df.timestamp = pd.to_datetime(df.timestamp, utc=True).astype(int) / 1e9
        return df

    def convert_from_recorded_time(self, recorded_time, iteration):
        return self.period_sec * iteration + recorded_time - self.recorded_t0 + self.wall_clock_t0

    def sleep_until(self, timestamp):
        sleep_sec = timestamp - time.time()
        if sleep_sec > 0.0:
            time.sleep(sleep_sec)
        elif sleep_sec < -5.0:
            logging.warn(f"can't keep up with real-time. sleep_sec={sleep_sec}")

    def get_video_event(self, index, iteration):
        filename = self.data_path / 'image_02' / 'data' / ('%010d.%s' % (index, self.format))
        recorded_timestamp = self.video_df.iloc[index].timestamp
        timestamp = self.convert_from_recorded_time(recorded_timestamp, iteration)
        with open(filename, 'rb') as f:
            file_bytes = f.read()
        event_dict = dict(
            car_id=self.car_id,
            camera=self.car_id,
            data=base64.b64encode(file_bytes).decode(encoding='UTF-8'),
            frameNumber=iteration*len(self.video_df) + index,
            ssrc=self.ssrc,
            timestamp=timestamp,
        )
        return event_dict

    def get_oxts_event(self, index, iteration):
        filename = self.data_path / 'oxts' / 'data' / ('%010d.txt' % index)
        recorded_timestamp = self.oxts_df.iloc[index].timestamp
        timestamp = self.convert_from_recorded_time(recorded_timestamp, iteration)
        with open(filename, 'rt') as f:
            vals = [float(s) for s in f.read().split(' ')]
        event_dict = dict(zip(self.oxts_col_names, vals))
        event_dict['timestamp'] = timestamp
        event_dict['car_id'] = self.car_id
        event_dict['camera'] = self.car_id
        return event_dict

    def get_event_to_write(self, event_dict, stream_name):
        event_dict['timestamp'] = int(event_dict['timestamp'] * 1000.0)
        event_json = json.dumps(event_dict)
        event_bytes = event_json.encode(encoding='UTF-8')
        event_to_write = pravega.pb.WriteEventsRequest(
            scope=self.scope,
            stream=stream_name,
            event=event_bytes,
            routing_key=str(self.car_id),
        )
        return event_to_write

    def video_event_to_write_generator(self):
        iteration = 0
        while True:
            for index in range(len(self.video_df)):
                event_dict = self.get_video_event(index, iteration)
                self.sleep_until(event_dict['timestamp'])
                event_to_write = self.get_event_to_write(event_dict, self.video_stream_name)
                yield event_to_write
            iteration += 1

    def oxts_event_to_write_generator(self):
        iteration = 0
        while True:
            for index in range(len(self.oxts_df)):
                event_dict = self.get_oxts_event(index, iteration)
                self.sleep_until(event_dict['timestamp'])
                event_to_write = self.get_event_to_write(event_dict, self.sensor_stream_name)
                yield event_to_write
            iteration += 1

    def get_pravega_client(self):
        pravega_channel = grpc.insecure_channel(self.gateway)
        return pravega.grpc.PravegaGatewayStub(pravega_channel)

    def run_video_process(self):
        print(f'run_video_process: BEGIN: {self.car_id}')
        output_stream = OutputStream(self.get_pravega_client(), self.scope, self.video_stream_name)
        events_to_write = self.video_event_to_write_generator()
        output_stream.write_events(events_to_write)
        print(f'run_video_process: END: {self.car_id}')

    def run_oxts_process(self):
        print(f'run_oxts_process: BEGIN: {self.car_id}')
        output_stream = OutputStream(self.get_pravega_client(), self.scope, self.sensor_stream_name)
        events_to_write = self.oxts_event_to_write_generator()
        output_stream.write_events(events_to_write)
        print(f'run_oxts_process: END: {self.car_id}')

    def start(self):
        print(f'start: BEGIN: {self.car_id}')
        self.wall_clock_t0 = time.time()
        self.processes += [Process(target=self.run_video_process)]
        self.processes += [Process(target=self.run_oxts_process)]
        for p in self.processes: p.start()
        print(f'start: END: {self.car_id}')

    def kill(self):
        print(f'kill: BEGIN: {self.car_id}')
        for p in self.processes: p.kill()
        print(f'kill: END: {self.car_id}')

    def join(self):
        print(f'join: BEGIN: {self.car_id}')
        for p in self.processes: p.join()
        self.processes = []
        print(f'join: END: {self.car_id}')
