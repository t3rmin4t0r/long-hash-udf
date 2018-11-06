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
import org.apache.hive.common.util.HashCodeUtil;

public class GenericLongHashUdf extends GenericUDF {

  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];
  private transient Converter[] converters = new Converter[1];
  private transient IntWritable output = new IntWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);

    checkArgPrimitive(arguments, 0);

    checkArgGroups(arguments, 0, inputTypes, PrimitiveGrouping.NUMERIC_GROUP);

    obtainLongConverter(arguments, 0, inputTypes, converters);

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;

    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    final long l0 = getLongValue(arguments, 0, converters);
    final int h = HashCodeUtil.calculateLongHashCode(l0);
    output.set(h);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("longhash", children, ",");
  }

}
