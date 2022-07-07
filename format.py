"""
format module provides encode/decode functions for serialisation and deserialisation
operations

format module is generic, does not have any disk or memory specific code.

The disk storage deals with bytes; you cannot just store a string or object without
converting it to bytes. The programming languages provide abstractions where you don't
have to think about all this when storing things in memory (i.e. RAM). Consider the
following example where you are storing stuff in a hash table:

    books = {}
    books["hamlet"] = "shakespeare"
    books["anna karenina"] = "tolstoy"

In the above, the language deals with all the complexities:

    - allocating space on the RAM so that it can store data of `books`
    - whenever you add data to `books`, convert that to bytes and keep it in the memory
    - whenever the size of `books` increases, move that to somewhere in the RAM so that
      we can add new items

Unfortunately, when it comes to disks, we have to do all this by ourselves, write
code which can allocate space, convert objects to/from bytes and many other operations.

format module provides two functions which help us with serialisation of data.

    encode_kv - takes the key value pair and encodes them into bytes
    decode_kv - takes a bunch of bytes and decodes them into key value pairs

**workshop note**

For the workshop, the functions will have the following signature:

    def encode_kv(timestamp: int, key: str, value: str) -> tuple[int, bytes]
    def decode_kv(data: bytes) -> tuple[int, str, str]
"""
import struct

BIG_ENDIAN = "big"
INT_WIDTH_BYTES = 4
ASCII_ENCODING = 'ascii'
HEADER_SIZE = 12


def int_to_bytes(the_int: int) -> bytes:
    if the_int.bit_length() > 4 * 8:
        raise struct.error("int was greater than 4 bytes")
    return the_int.to_bytes(INT_WIDTH_BYTES, BIG_ENDIAN)


def encode_header(timestamp: int, key_size: int, value_size: int) -> bytes:
    return int_to_bytes(timestamp) + int_to_bytes(key_size) + int_to_bytes(value_size)


def encode_kv(timestamp: int, key: str, value: str) -> tuple[int, bytes]:
    encoded_key = key.encode(ASCII_ENCODING)
    encoded_value = value.encode(ASCII_ENCODING)
    header_bytes = encode_header(timestamp, len(encoded_key), len(encoded_value))
    encoded = header_bytes + encoded_key + encoded_value
    return len(encoded), encoded


def decode_kv(data: bytes) -> tuple[int, str, str]:
    timestamp, key_size, value_size = decode_header(data[0:HEADER_SIZE])

    return timestamp, \
           data[HEADER_SIZE:HEADER_SIZE + key_size].decode(ASCII_ENCODING), \
           data[HEADER_SIZE + key_size:len(data)].decode(ASCII_ENCODING)


def decode_header(data: bytes) -> tuple[int, int, int]:
    timestamp = int.from_bytes(data[0:4], BIG_ENDIAN)
    key_size = int.from_bytes(data[4:8], BIG_ENDIAN)
    value_size = int.from_bytes(data[8:12], BIG_ENDIAN)

    return timestamp, key_size, value_size
