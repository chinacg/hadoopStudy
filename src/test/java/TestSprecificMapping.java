import generated.StringPair;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * specific方式使用avro
 */
public class TestSprecificMapping {

    @Test
    public void test()throws IOException {
        //因为已经生成StringPair的源代码，所以不再使用schema了
        StringPair datnum = StringPair.newBuilder()
                .setLeft("L")
                .setRight("R")
                .build();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ////不再需要传schema了，直接用StringPair作为范型和参数，
        DatumWriter<StringPair> writer = new SpecificDatumWriter<>(StringPair.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datnum,encoder);
        encoder.flush();
        out.close();

        DatumReader<StringPair> reader= new SpecificDatumReader<>(StringPair.class);
        Decoder decoder= DecoderFactory.get().binaryDecoder(out.toByteArray(),null);
        StringPair result=reader.read(null,decoder);
        Assert.assertEquals("L",result.getLeft().toString());
        Assert.assertEquals("R",result.getRight().toString());
    }
}
