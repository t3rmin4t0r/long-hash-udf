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
    /*
     * 2x murmur + XOR + cast (lazy, but fix this with a chained Murmur32)
     */
    final int h = (int) (Murmur3.hash64(l0) ^ Murmur3.hash64(l1));
    output.set(h);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("long2hash", children, ",");
  }

}
