import threading
import time
import requests
from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.core.cache import cache

from app.kafka import getKafka, sendKafka
from app.utils import text_to_bits, split_bits, sanitize_topic_name

from queue import Queue

SEGMENT_SIZE_BYTES = 100
SEGMENT_SIZE_BITS = SEGMENT_SIZE_BYTES * 8

send_queue = Queue()


def ws_sender_worker():
    while True:
        task = send_queue.get()
        if task is None:
            break

        username, send_time, message, error = task
        send_to_ws(username, send_time, message, error)
        time.sleep(0.5)


def send_to_ws(username, send_time, message, error=False):
    print("send_to_ws")

    earth_resp = requests.post('http://localhost:8010/receive/', json={
        "username": username,
        "message": message,
        "send_time": send_time,
        "error": error
    })

    print(earth_resp.status_code)

    mars_resp = requests.post('http://localhost:8020/receive/', json={
        "username": username,
        "message": message,
        "send_time": send_time,
        "error": error
    })
    print(mars_resp.status_code)


def pooling(send_time, total_segments, username):
    prev = []

    while True:
        time.sleep(3)

        segments = cache.get(send_time)
        if len(prev) != len(segments):
            prev = segments

        else:
            if len(segments) != total_segments:
                send_queue.put((username, send_time, "", True))
                cache.delete(send_time)
            else:
                try:
                    binary_string = "".join(segments)
                    message = ''.join(chr(int(binary_string[i:i + 8], 2)) for i in range(0, len(binary_string), 8))
                    print("decoded: " + message)
                    send_queue.put((username, send_time, message, False))
                except:
                    send_queue.put((username, send_time, "", True))
                finally:
                    cache.delete(send_time)

            break


def assembling(segments_count, send_time, username):
    print("Сборка сегментов сообщения с временем отправки " + send_time)

    segments = getKafka(sanitize_topic_name(send_time))

    if segments_count == len(segments):
        for i, segment in enumerate(segments):
            print(f"Отправка сегмента №{i + 1} на канальный уровень")
            print({
                "segment": segment,
                "segment_number": i + 1,
                "total_segments": len(segments),
                "send_time": send_time,
                "username": username
            })

            resp = requests.post('http://localhost:8001/code', json={
                "segment": segment,
                "segment_number": i + 1,
                "total_segments": len(segments),
                "send_time": send_time,
                "username": username
            })

            print(resp.status_code)

            time.sleep(1)


@api_view(["POST"])
def send(request):
    message = request.data.get('message')
    username = request.data.get('username')
    send_time = request.data.get('send_time')

    bitstring = text_to_bits(message)
    segments = split_bits(bitstring, SEGMENT_SIZE_BITS)

    for i, segment in enumerate(segments):
        print(f"Отправка сегмента в кафку №{i + 1}")
        sendKafka(segment, sanitize_topic_name(send_time))

    threading.Thread(target=assembling, args=[len(segments), send_time, username]).start()

    return Response({"message": "OK"}, status=200)


@api_view(["POST"])
def transfer(request):
    print(f"с канального уровня: {request.data}")
    segment = request.data["segment"]
    send_time = request.data["send_time"]
    total_segments = request.data["total_segments"]
    username = request.data["username"]

    if send_time in cache:
        cached_message = cache.get(send_time)
        cache.set(send_time, cached_message + [segment])
    else:
        cache.set(send_time, [segment])
        threading.Thread(target=pooling, args=[send_time, total_segments, username]).start()

    return Response({"message": "OK"}, status=200)


threading.Thread(target=ws_sender_worker, daemon=True).start()
