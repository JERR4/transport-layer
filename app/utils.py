def text_to_bits(text, encoding='utf-8', errors='surrogatepass'):
    byte_data = text.encode(encoding, errors)
    return ''.join(f'{byte:08b}' for byte in byte_data)


def text_from_bits(bits, encoding='utf-8', errors='surrogatepass'):
    byte_array = int(bits, 2).to_bytes((len(bits) + 7) // 8, byteorder='big')
    return byte_array.decode(encoding, errors)


def split_bits(bits, segment_size_bits):
    return [bits[i:i + segment_size_bits] for i in range(0, len(bits), segment_size_bits)]


def sanitize_topic_name(send_time):
    return send_time.replace(":", "-").replace("Z", "")
