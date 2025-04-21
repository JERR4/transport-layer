import threading
import time
import requests
from rest_framework.response import Response
from rest_framework.decorators import api_view
from queue import Queue
from django.core.cache import cache

send_queue = Queue()

def pooling(message_id, send_time, total_segments, username):
    prev = []

    while True:
        time.sleep(3)

        segments = cache.get(message_id)
        if len(prev) != len(segments):
            prev = segments

        else:
            if len(segments) != total_segments:
                send_to_ws(username, send_time, "", error=True)
                cache.delete(message_id)
            else:
                try:
                    binary_string = "".join(segments)
                    message = ''.join(chr(int(binary_string[i:i + 8], 2)) for i in range(0, len(binary_string), 8))
                    print("decoded: " + message)
                    send_to_ws(username, send_time, message)
                except:
                    send_to_ws(username, send_time, "", error=True)
                finally:
                    cache.delete(message_id)

            break

def start_sender_thread():
    sender_thread = threading.Thread(target=rate_limited_sender)
    sender_thread.daemon = True
    sender_thread.start()

start_sender_thread()

def rate_limited_sender():
    while True:
        time.sleep(0.5)

        segment_data = send_queue.get()

        if segment_data is None:
            break

        response = requests.post('http://localhost:8001/code/', json=segment_data)

        if response.status_code != 200:
            print(f"Ошибка при отправке сегмента: {response.status_code}")
        else:
            print(f"Сегмент отправлен успешно: {segment_data['segment_number']}")


@api_view(["POST"])
def send(request):
    message = request.data.get('message')
    message_bytes = message.encode("utf-8")
    total_segments = (len(message_bytes) + 99) // 100

    username = request.data.get('username')
    send_time = request.data.get('send_time')

    for i in range(0, len(message_bytes), 100):
        chunk = message_bytes[i:i + 100]
        segment_data = {
            "segment": chunk.decode("utf-8", errors="ignore"),
            "segment_number": i // 100 + 1,
            "total_segment": total_segments,
            "send_time": send_time,
            "username": username
        }

        send_queue.put(segment_data)

    return Response({"message": "OK"}, status=200)

@api_view(["POST"])
def transfer(request):
    message_id = request.data.get('message_id')
    segment = request.data["segment"]
    segment_number = request.data["segment_number"]
    send_time = request.data["send_time"]
    total_segments = request.data["total_segment"]
    username = request.data["username"]


    if message_id in cache:
        cached_message = cache.get(message_id)
        cache.set(message_id, cached_message + [segment])
    else:
        cache.set(message_id, [segment])
        threading.Thread(target=pooling, args=[message_id, send_time, total_segments, username]).start()

    print(cache.get(message_id))

    return Response({"message": "OK"}, status=200)
