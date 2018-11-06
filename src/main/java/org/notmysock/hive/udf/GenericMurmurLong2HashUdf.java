package org.notmysock.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.io.IntWritable;
import org.apache.hive.common.util.Murmur3;

public class GenericMurmurLong2HashUdf extends GenericUDF {

  private static final int C1_32 = -862048943;
  private static final int C2_32 = 461845907;
  private static final int R1_32 = 15;
  private static final int R2_32 = 13;
  private static final int M_32 = 5;
  private static final int N_32 = -430675100;
  private static final int DEFAULT_SEED = 104729;

  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];
  private transient Converter[] converters = new Converter[2];
  private transient IntWritable output = new IntWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);


    checkArgGroups(arguments, 0, inputTypes, PrimitiveGrouping.NUMERIC_GROUP);
    checkArgGroups(arguments, 1, inputTypes, PrimitiveGrouping.NUMERIC_GROUP);


    obtainLongConverter(arguments, 0, inputTypes, converters);
    obtainLongConverter(arguments, 1, inputTypes, converters);

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;

    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    final long l0 = getLongValue(arguments, 0, converters);
    final long l1 = getLongValue(arguments, 1, converters);

    final int h = hash32(l0, l1);
    output.set(h);
    return output;
  }

  public static int hash32(long l0, long l1) {
    return hash32(l0, l1, DEFAULT_SEED);
  }

  public static int hash32(long l0, long l1, int seed) {
    int hash = seed;

    final byte  b0 = (byte) (l0 >> 56);
    final byte  b1 = (byte) (l0 >> 48);
    final byte  b2 = (byte) (l0 >> 40);
    final byte  b3 = (byte) (l0 >> 32);
    final byte  b4 = (byte) (l0 >> 24);
    final byte  b5 = (byte) (l0 >> 16);
    final byte  b6 = (byte) (l0 >>  8);
    final byte  b7 = (byte) (l0 >>  0);
    final byte  b8 = (byte) (l1 >> 56);
    final byte  b9 = (byte) (l1 >> 48);
    final byte b10 = (byte) (l1 >> 40);
    final byte b11 = (byte) (l1 >> 32);
    final byte b12 = (byte) (l1 >> 24);
    final byte b13 = (byte) (l1 >> 16);
    final byte b14 = (byte) (l1 >>  8);
    final byte b15 = (byte) (l1 >>  0);

    // body
    int k;

    // Roll 1
    k = (b0 & 0xff)
        | ((b1 & 0xff) << 8)
        | ((b2 & 0xff) << 16)
        | ((b3 & 0xff) << 24);
    k *= C1_32;
    k = Integer.rotateLeft(k, R1_32);
    k *= C2_32;
    hash ^= k;
    hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;

    // Roll 2
    k = (b4 & 0xff)
        | ((b5 & 0xff) << 8)
        | ((b6 & 0xff) << 16)
        | ((b7 & 0xff) << 24);
    k *= C1_32;
    k = Integer.rotateLeft(k, R1_32);
    k *= C2_32;
    hash ^= k;
    hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;

    // Roll 3
    k = (b8 & 0xff)
        | ((b9 & 0xff) << 8)
        | ((b10 & 0xff) << 16)
        | ((b11 & 0xff) << 24);
    k *= C1_32;
    k = Integer.rotateLeft(k, R1_32);
    k *= C2_32;
    hash ^= k;
    hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;

    // Roll 4
    k = (b12 & 0xff)
        | ((b13 & 0xff) << 8)
        | ((b14 & 0xff) << 16)
        | ((b15 & 0xff) << 24);
    k *= C1_32;
    k = Integer.rotateLeft(k, R1_32);
    k *= C2_32;
    hash ^= k;
    hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;

    // finalization
    hash ^= 16;
    hash ^= (hash >>> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >>> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >>> 16);

    return hash;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("long2hash", children, ",");
  }
}
