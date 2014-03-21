import unittest
from kafka.util import write_int_string, read_short_string, read_int_string, relative_unpack
from kafka.common import BufferUnderflowError

class TestUtility(unittest.TestCase):

    def test_write_int_string_none(self):
        val = write_int_string(None)

        self.assertEqual(val, b'\xFF\xFF\xFF\xFF')

    def test_write_int_string_with_val(self):
        val = write_int_string(b'1234567890abcd');
        self.assertEqual(val, b'\x00\x00\x00\x0E1234567890abcd')

    def test_read_short_string_underbuffer_header(self):
        with self.assertRaises(BufferUnderflowError):
            data = b'Test'
            val, position = read_short_string(data,len(data))

    def test_read_short_string_underbuffer_data(self):
        with self.assertRaises(BufferUnderflowError):
            data = b'\x00\x06Test'
            val, position = read_short_string(data,0)

    def test_read_short_string_return_none(self):
        data = b'\xFF\xFF'
        self.assertEqual(read_short_string(data,0),(None,2))

    def test_read_int_string_underbuffer_header(self):
        with self.assertRaises(BufferUnderflowError):
            data = b'\x00\x00\x00\x0E1234567890abcd'
            read_int_string(data,20)

    def test_read_int_string_underbuffer_data(self):
        with self.assertRaises(BufferUnderflowError):
            data = b'\x00\x00\x00\x05abcd'
            read_int_string(data,0)

    def test_read_int_string_return_none(self):
        data = b'\xFF\xFF\xFF\xFF'
        self.assertEqual(read_int_string(data,0),(None,4))

    def test_relative_unpack_underbuffer_data(self):
        with self.assertRaises(BufferUnderflowError):
            data = b'\x00\x00'
            relative_unpack('>l', data, 0)

    def test_relative_unpack_read_int(self):
        data = b'\x00\x00\x00\xFF'
        val = relative_unpack('>i',data,0)
        self.assertEqual(val,((255,),4))

    def test_relaitve_unpack_read_two_ints(self):
        data = b'\x00\x00\x00\xFF\x00\x00\x02\x00'
        val = relative_unpack('>ii',data,0)
        self.assertEqual(val,((255,512),8))

    def test_relative_unpack_read_string(self):
        data = b'test string'
        val = relative_unpack('>4s',data,0)
        self.assertEqual(val,((b'test',),4))


if __name__ == '__main__':
    unittest.main()
