package datafu.mr.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import datafu.mr.avro.Schemas;
import datafu.mr.test.util.BasicAvroWriter;
import datafu.mr.util.AvroHelper;


/**
 * Tests for AvroHelper
 */
public class TestAvroHelper {

  private static final Schema ID_SCHEMA = Schemas.createRecordSchema(TestAvroJob.class, "Event",
      new Field("id", Schema.create(Type.LONG), "ID", null));
  private final File testPath = new File("avrohelperinput");

  @BeforeMethod
  public void setUp() throws Throwable {
    FileUtils.deleteDirectory(testPath);
    testPath.mkdir();
  }

  @AfterMethod
  public void cleanUp() throws Throwable {
    FileUtils.deleteDirectory(testPath);
  }

  @Test
  public void testReadAvroFilesMissingFile() {
    Assert.assertFalse(AvroHelper.readAvroFiles(new File(testPath, "foo"), null).iterator().hasNext());
  }

  @Test
  public void testReadAvroFilesEmptyFolder() {
    Assert.assertFalse(AvroHelper.readAvroFiles(testPath, null).iterator().hasNext());
  }

  @Test
  public void testReadAvroFilesEmptyFile() throws IOException {
    BasicAvroWriter<GenericRecord> writer =
        new BasicAvroWriter<GenericRecord>(new Path(testPath.getName()), ID_SCHEMA, getFileSystem());
    writer.open();
    writer.close();
    Assert.assertTrue(getFileSystem().exists(writer.getOutputFilePath()));
    Assert.assertFalse(AvroHelper.readAvroFiles(testPath, null).iterator().hasNext());
  }

  @Test
  public void testReadAvroFilesNonAvroExtension() throws IOException {
    FileWriter writer = new FileWriter(new File(testPath, "foo.txt"));
    writer.write("bar");
    writer.close();
    Assert.assertFalse(AvroHelper.readAvroFiles(testPath, null).iterator().hasNext());
  }

  @Test
  public void testReadAvroFilesUniqueFile() throws IOException {
    GenericRecord expected = new GenericData.Record(ID_SCHEMA);
    expected.put("id", 12l);

    FSDataOutputStream outputStream = getFileSystem().create(new Path(testPath.getName() + "foo.avro"));
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>();
    DataFileWriter<GenericRecord> dataWriter = new DataFileWriter<GenericRecord>(writer);
    dataWriter.create(ID_SCHEMA, outputStream);
    dataWriter.append(expected);
    dataWriter.close();
    outputStream.close();

    File file = new File(testPath.getName() + "foo.avro");
    try {
      @SuppressWarnings("unchecked")
      List<GenericRecord> res = IteratorUtils.toList(AvroHelper.readAvroFiles(file, null).iterator());
      Assert.assertEquals(res.size(), 1);
      Assert.assertEquals(res.get(0), expected);

    } finally {
      file.delete();
      new File("." + file.getName() + ".crc").delete();
    }
  }

  @Test
  public void testReadAvroFilesOneFileInFolder() throws IOException {
    long[] ids = new long[] { 1, 3, 6, 7, 9 };
    BasicAvroWriter<GenericRecord> writer =
        new BasicAvroWriter<GenericRecord>(new Path(testPath + "/foo"), ID_SCHEMA, getFileSystem());
    storeIds(writer, ids);

    File file = new File(writer.getOutputFilePath().toString());

    @SuppressWarnings("unchecked")
    List<GenericRecord> res = IteratorUtils.toList(AvroHelper.readAvroFiles(file, null).iterator());
    Assert.assertEquals(res.size(), ids.length);
    for (int i = 0; i < ids.length; i++) {
      Assert.assertEquals(res.get(i).get("id"), ids[i]);
    }
  }

  @Test
  public void testReadAvroFilesMultipleFiles() throws IOException {
    String[] files = new String[] { "foo1", "foo2" };
    long[][] ids = new long[][] { { 1, 3 }, { 6, 7, 9 } };
    int total = 0;

    for (int i = 0; i < files.length; i++) {
      BasicAvroWriter<GenericRecord> writer =
          new BasicAvroWriter<GenericRecord>(new Path(testPath + "/" + files[i]), ID_SCHEMA, getFileSystem());
      storeIds(writer, ids[i]);
      total += ids[i].length;
    }

    @SuppressWarnings("unchecked")
    List<GenericRecord> res = IteratorUtils.toList(AvroHelper.readAvroFiles(testPath, null).iterator());
    Assert.assertEquals(res.size(), total);
    for (int i = 0; i < ids.length; i++) {
      for (int j = 0; j < ids[i].length; j++) {
        Assert.assertEquals(res.get(i * ids.length + j).get("id"), ids[i][j]);
      }
    }
  }

  // UTILITY

  private FileSystem getFileSystem() throws IOException {
    return FileSystem.get(new JobConf());
  }

  private void storeIds(BasicAvroWriter<GenericRecord> writer, long... ids) throws IOException {
    writer.open();
    GenericRecord record = new GenericData.Record(ID_SCHEMA);
    for (long id : ids) {
      record.put("id", id);
      writer.append(record);
    }
    writer.close();
  }
}
