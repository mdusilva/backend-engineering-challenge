import unittest
import unbabel_cli
import io
import json
import threading
import queue
import datetime
import time
import os

class StreamerWorks(unittest.TestCase):

    def setUp(self):
        self.streaming_data_unfinished_line = io.StringIO('{"part1')
        self.streaming_data_finished_line = io.StringIO('{"part1": 0, "part2": 0}\n')
        self.streaming_data_finished_two_lines = io.StringIO('{"part1": 0, "part2": 0}\n {"part3')
        self.close_streaming = threading.Event()

    def test_read_complete_line(self):
        """streamer must correctly output a complete line read from a file"""
        line = unbabel_cli.streamer(self.streaming_data_finished_line, self.close_streaming)
        self.assertEqual(next(line), json.loads('{"part1": 0, "part2": 0}'))

class PublisherWorks(unittest.TestCase):
    def setUp(self):
        self.correct_file = 'test1.json'
        with open(self.correct_file, 'w+') as ofile:
            ofile.write('{"timestamp": "%s", "duration": 20}\n{"timestamp": "%s", "duration": 31}\n{"timestamp": "%s", "duration": 54}\n' 
                    % ((datetime.datetime.now()-datetime.timedelta(seconds=2)).strftime('%Y-%m-%d %H:%M:%S.%f'), (datetime.datetime.now()-datetime.timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S.%f'), datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
    def tearDown(self):
        if os.path.exists('test1.json'):
            os.remove('test1.json')

    def test_publish_events(self):
        """Events are published correctly"""
        q = queue.Queue()
        close_publisher = threading.Event()
        thread = threading.Thread(target=unbabel_cli.publisher, args=(self.correct_file, q, close_publisher), kwargs={'window':4, 'timeout':30})
        thread.start()
        time.sleep(0.5)
        close_publisher.set()
        thread.join()
        messages = [q.get(timeout=1).get('duration') for k in range(3)]
        self.assertListEqual(messages, [20, 31, 54])    

class HandlerWorks(unittest.TestCase):

    def test_handle_messages(self):
        """Messages are handled correctly"""
        qin = queue.Queue()
        qout = queue.Queue()
        close_handler = threading.Event()
        thread = threading.Thread(target=unbabel_cli.handler, args=(1, qin, qout, close_handler), kwargs={'window':4})
        thread.start()
        qin.put({"timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "duration": 54}, timeout=1)
        time.sleep(0.5)
        close_handler.set()
        thread.join()
        output = qout.get(timeout=1)
        self.assertEqual(json.loads(output).get('average_delivery_time'), 54)

class WriterWorks(unittest.TestCase):
    def setUp(self):
        self.correct_file = 'out.json'
    def tearDown(self):
        if os.path.exists('out.json'):
            os.remove('out.json')

    def test_write_messages(self):
        """Received messages are correctly written"""
        qin = queue.Queue()
        close_writer = threading.Event()
        thread = threading.Thread(target=unbabel_cli.writer, args=(self.correct_file, qin, close_writer))
        thread.start()
        qin.put({"date":  datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "average_delivery_time": 45.5})
        time.sleep(0.5)
        close_writer.set()
        thread.join()
        with open(self.correct_file, 'r') as ifile:
            l = ifile.readline().replace("'", '"')
        self.assertEqual(json.loads(l).get("average_delivery_time"), 45.5)

class MainWorks(unittest.TestCase):

    def setUp(self):
        self.correct_file = 'test1.json'
        self.args = ([
            '--input_file', 
            'test1.json',  
            '--window_size', 
            '4', 
            '--output_file', 
            'out.json', 
            '--frequency', 
            '1'],)
        with open(self.correct_file, 'w+') as ofile:
            ofile.write('{"timestamp": "%s", "duration": 20}\n{"timestamp": "%s", "duration": 31}\n{"timestamp": "%s", "duration": 54}\n' 
                    % ((datetime.datetime.now()-datetime.timedelta(seconds=2)).strftime('%Y-%m-%d %H:%M:%S.%f'), (datetime.datetime.now()-datetime.timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S.%f'), datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))

    def tearDown(self):
        if os.path.exists('out.json'):
            os.remove('out.json')
        if os.path.exists('test1.json'):
            os.remove('test1.json')

    def test_known_values(self):
        """application should give expected result """
        if os.path.exists("out.json"):
            os.remove("out.json")
        close_main = threading.Event()
        kwargs = {'close_event': close_main}
        thread = threading.Thread(target=unbabel_cli.main, args=self.args, kwargs=kwargs)
        thread.start()
        time.sleep(5)
        close_main.set()
        thread.join()
        result_list = []
        with open('out.json', 'r') as ifile:
            for l in ifile.readlines():
                result_list.append(json.loads(l).get('average_delivery_time'))
        self.assertListEqual(result_list[:4], [35.0, 42.5, 54.0, None])

if __name__ == '__main__':
    unittest.main()