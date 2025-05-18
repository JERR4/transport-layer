def text_to_bits(text):
    return ''.join(f"{byte:08b}" for byte in bytes(text, encoding="utf-8"))


def text_from_bits(bits):
    print('Пришло на декодирование', bits)
    return bytes(int(bits[i:i + 8], 2) for i in range(0, len(bits), 8)).decode('utf-8', errors='replace')


def split_bits(bits, segment_size_bits):
    return [bits[i:i + segment_size_bits] for i in range(0, len(bits), segment_size_bits)]


def sanitize_topic_name(send_time):
    return send_time.replace(":", "-").replace("Z", "")
