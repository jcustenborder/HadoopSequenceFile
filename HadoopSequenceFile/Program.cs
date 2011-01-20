using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;


namespace HadoopSequenceFile
{
    class Program
    {
        static void Main(string[] args)
        {
            using (FileStream iostr = new FileStream("output.bin", FileMode.Create, FileAccess.Write))
            {
                SequenceFileWriter<NullWritable, BytesWritable> writer = new SequenceFileWriter<NullWritable, BytesWritable>(iostr);


            }
        }
    }

    interface Writable
    {
        byte[] GetBytes();
        string GetName();
    }

    public class NullWritable : Writable
    {
        public static readonly NullWritable Instance = new NullWritable();

        public byte[] GetBytes()
        {
            throw new NotImplementedException();
        }

        public string GetName()
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

        public string GetName()
        {
            throw new NotImplementedException();
        }
    }

    public class TextWritable : Writable
    {
        public string GetName()
        {
            throw new NotImplementedException();
        }
        
        public byte[] GetBytes()
        {
            throw new NotImplementedException();
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

        public static void writeVLong(Stream stream, long i)
        {
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

    class SequenceFileWriter<Key, Value> where Key:Writable
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
            TextWritable.writeString(output, "org.apache.hadoop.io.NullWritable");
            TextWritable.writeString(output, "org.apache.hadoop.io.BytesWritable");
            StreamHelper.WriteBoolean(output, true);
            StreamHelper.WriteBoolean(output, true);

            TextWritable.writeString(output, "org.apache.hadoop.io.compress.DefaultCodec");

            //TODO: Add support for metadata
        }

        void finializeFileHeader()
        {
            output.Write(sync, 0, sync.Length);
            output.Flush();
        }

    }
}
