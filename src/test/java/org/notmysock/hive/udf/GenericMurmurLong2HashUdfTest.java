package org.notmysock.hive.udf;

import junit.framework.TestCase;
import org.apache.hive.common.util.Murmur3;

import java.nio.ByteBuffer;
import java.util.Random;

public class GenericMurmurLong2HashUdfTest extends TestCase {
  public void testOrdered() {
    ByteBuffer buffer = ByteBuffer.allocate(16);
    for (int i = 0; i < 1000; i++) {
      for (int j = 0; j < 1000; j++) {
        buffer.putLong(0, i);
        buffer.putLong(8, j);
        assertEquals(Murmur3.hash32(buffer.array()),
            GenericMurmurLong2HashUdf.calculateTwoLongHashCode(i, j));
      }
    }
  }

  public void testRandom() {
    ByteBuffer buffer = ByteBuffer.allocate(16);
    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      for (int j = 0; j < 1000; j++) {
        long x = random.nextLong();
        long y = random.nextLong();
        buffer.putLong(0, x);
        buffer.putLong(8, y);
        assertEquals(Murmur3.hash32(buffer.array()),
            GenericMurmurLong2HashUdf.calculateTwoLongHashCode(x, y));
      }
    }
  }
}