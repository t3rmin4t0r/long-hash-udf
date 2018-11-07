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

    final int h = calculateTwoLongHashCode(l0, l1);
    output.set(h);
    return output;
  }

  private static int murmur32Body(int k, int hash) {
    k *= C1_32;
    k = Integer.rotateLeft(k, R1_32);
    k *= C2_32;
    hash ^= k;
    return Integer.rotateLeft(hash, R2_32) * M_32 + N_32;
  }

  private static int murmur32Finalize(int hash) {
    hash ^= 16;
    hash ^= (hash >>> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >>> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >>> 16);
    return hash;
  }

  /**
   * Murmur3 32-bit variant.
   * @see Murmur3#hash32(byte[], int, int, int)
   */
  private static int murmur32(long l0, long l1, int seed) {
    int hash = seed;
    final long r0 = Long.reverseBytes(l0);
    final long r1 = Long.reverseBytes(l1);

    hash = murmur32Body((int) r0, hash);
    hash = murmur32Body((int) (r0 >>> 32), hash);
    hash = murmur32Body((int) (r1), hash);
    hash = murmur32Body((int) (r1 >>> 32), hash);

    return murmur32Finalize(hash);
  }

  /**
   * Murmur3 32-bit variant.
   * @see Murmur3#hash32(byte[], int, int, int)
   */
  private static int murmur32(long l0, int seed) {
    int hash = seed;
    final long r0 = Long.reverseBytes(l0);

    hash = murmur32Body((int) r0, hash);
    hash = murmur32Body((int) (r0 >>> 32), hash);

    return murmur32Finalize(hash);
  }

  public static int calculateTwoLongHashCode(long l0, long l1) {
    return murmur32(l0, l1, DEFAULT_SEED);
  }

  public static int calculateLongHashCode(long key) {
    return murmur32(key, DEFAULT_SEED);
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("long2hash", children, ",");
  }
}
