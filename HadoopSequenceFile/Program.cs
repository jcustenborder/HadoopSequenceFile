using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;


namespace HadoopSequenceFile
{
    class Program
    {
        static void Main(string[] args)
        {
            using (FileStream iostr = new FileStream(@"D:\drop\output.bin", FileMode.Create, FileAccess.Write))
            {
                using (SequenceFileWriter<TextWritable, TextWritable> writer = new SequenceFileWriter<TextWritable, TextWritable>(iostr))
                {
                    for (int i = 0; i < 1000000; i++)
                    {
                        

                        string key = string.Format("key{0:000000}", i);
                        string value = string.Format("This is test value {0}", i);

                        TextWritable k = new TextWritable(key);
                        TextWritable v = new TextWritable(value);
                        writer.Append(k, v);
                    }
                }
            }
        }
    }

    interface Writable
    {
        byte[] GetBytes();
    }

    public class NullWritable : Writable
    {
        public static readonly NullWritable Instance = new NullWritable();

        public byte[] GetBytes()
        {
            throw new NotImplementedException();
        }
    }

    public class BytesWritable : Writable
    {
        

        public byte[] GetBytes()
        {
            throw new NotImplementedException();
        }
    }

    public class TextWritable : Writable
    {
        private string _Text;

        public TextWritable(string value)
        {
            _Text = value;
        }

        public byte[] GetBytes()
        {
            return Encoding.UTF8.GetBytes(_Text);
        }
        
        public static void writeString(Stream iostr, string text)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(text);
            StreamHelper.writeVInt(iostr, buffer.Length);
            iostr.Write(buffer, 0, buffer.Length);
        }
    }

    static class StreamHelper
    {
        public static void WriteBoolean(Stream stream, bool boolean)
        {
            byte value = (byte)(boolean ? 1 : 0);
            stream.WriteByte(value);
        }
        public static void writeVInt(Stream stream, int i)
        {
            writeVLong(stream, i);
        }
        public static void writeInt(Stream stream, int i)
        {
            int value = BitConverter.IsLittleEndian ? System.Net.IPAddress.HostToNetworkOrder(i) : i;
            byte[] buffer = BitConverter.GetBytes(i);
            stream.Write(buffer, 0, buffer.Length);
        }
        public static void writeVLong(Stream stream, long i)
        {
            //long i = BitConverter.IsLittleEndian ? System.Net.IPAddress.HostToNetworkOrder(a) : a;

            if (i >= -112 && i <= 127)
            {
                stream.WriteByte((byte)i);
                return;
            }

            int len = -112;
            if (i < 0)
            {
                i ^= -1L; // take one's complement'
                len = -120;
            }

            long tmp = i;
            while (tmp != 0)
            {
                tmp = tmp >> 8;
                len--;
            }

            stream.WriteByte((byte)len);

            len = (len < -120) ? -(len + 120) : -(len + 112);

            for (int idx = len; idx != 0; idx--)
            {
                int shiftbits = (idx - 1) * 8;
                long mask = 0xFFL << shiftbits;
                stream.WriteByte((byte)((i & mask) >> shiftbits));
            }
        }
    }

    class SequenceFileWriter<Key, Value>:IDisposable  where Key:Writable
                                   where Value:Writable
    {
        private const byte BLOCK_COMPRESS_VERSION = 4;
        private const byte VERSION_WITH_METADATA = 6;
        private static readonly byte[] VERSION = new byte[]{
            (byte)'S',
            (byte)'E',
            (byte)'Q',
            VERSION_WITH_METADATA
        };

        private const int SYNC_ESCAPE = -1;
        private const int SYNC_HASH_SIZE = 16;
        private const int SYNC_SIZE = 4 + SYNC_HASH_SIZE;
        private const int SYNC_INTERVAL = 100 * SYNC_SIZE;
        private byte[] sync = new byte[16];

        private Stream output;

        private MemoryStream keyLenBuffer = new MemoryStream();
        private MemoryStream keyBuffer = new MemoryStream();
        private MemoryStream valLenBuffer = new MemoryStream();
        private MemoryStream valBuffer = new MemoryStream();

        public SequenceFileWriter(Stream stream)
        {
            this.output = stream;

            Random random = new Random();
            random.NextBytes(sync);
            initializeFileHeader();
            writeFileHeader();
            finializeFileHeader();
        }


        void initializeFileHeader()
        {
            output.Write(VERSION, 0, VERSION.Length);
        }

        void writeFileHeader()
        {
            TextWritable.writeString(output, "org.apache.hadoop.io.Text");
            TextWritable.writeString(output, "org.apache.hadoop.io.Text");
            StreamHelper.WriteBoolean(output, true);
            StreamHelper.WriteBoolean(output, true);

            TextWritable.writeString(output, "org.apache.hadoop.io.compress.DefaultCodec");

            //TODO: Add support for metadata
            StreamHelper.writeInt(output, 0);
        }

        void finializeFileHeader()
        {
            output.Write(sync, 0, sync.Length);
            output.Flush();
        }
        private long compressionBlockSize = 1000000;

        public void Append(Key key, Value value)
        {
            if (null == key) throw new ArgumentNullException("key", "key cannot be null.");
            if (null == value) throw new ArgumentNullException("value", "value cannot be null.");

            byte[] keyBytes = key.GetBytes();
            byte[] valueBytes = value.GetBytes();

            StreamHelper.writeVInt(keyLenBuffer, keyBytes.Length);
            keyBuffer.Write(keyBytes, 0, keyBytes.Length);

            StreamHelper.writeVInt(valLenBuffer, valueBytes.Length);
            valBuffer.Write(valueBytes, 0, valueBytes.Length);

            ++noBufferedRecords;

            long currentblocksize = keyBuffer.Length + valBuffer.Length;

            if (currentblocksize >= compressionBlockSize)
                Sync();

        }

        private long lastSyncPos;
        private int noBufferedRecords;

        private void writeBuffer(Stream stream)
        {
            stream.Position = 0L;

            byte[] compressed = null;
            using (MemoryStream iostr = new MemoryStream())
            {
                using (zlib.ZOutputStream deflate = new zlib.ZOutputStream(iostr, 7))
                {
                    const int MAXBUFFER = 64 * 1024;
                    byte[] buffer = new byte[MAXBUFFER];
                    int length = 0;

                    while ((length = stream.Read(buffer, 0, MAXBUFFER)) > 0)
                        deflate.Write(buffer, 0, length);

                    deflate.Flush();
                }
                compressed = iostr.ToArray();
                
            }
            StreamHelper.writeVLong(output, compressed.Length);
            output.Write(compressed, 0, compressed.Length);
        }

        public void Sync()
        {
            if (null != sync && output.Position != lastSyncPos)
            {
                StreamHelper.writeInt(output, SYNC_ESCAPE);
                output.Write(sync, 0, sync.Length);
                lastSyncPos = output.Position;
            }

            if (noBufferedRecords > 0)
            {
                StreamHelper.writeVInt(output, noBufferedRecords);

                writeBuffer(keyLenBuffer);
                writeBuffer(keyBuffer);
                
                writeBuffer(valLenBuffer);
                writeBuffer(valBuffer);

                output.Flush();

                clearBuffer(keyLenBuffer);
                clearBuffer(keyBuffer);
                clearBuffer(valLenBuffer);
                clearBuffer(valBuffer);
                noBufferedRecords = 0;
            }
        }

        private void clearBuffer(MemoryStream iostr)
        {
            iostr.Position = 0;
            iostr.SetLength(0);
        }

        public void Close()
        {
            if(null!=output)
                Sync();
        }



        public void Dispose()
        {
            Close();
        }

       
    }
}
