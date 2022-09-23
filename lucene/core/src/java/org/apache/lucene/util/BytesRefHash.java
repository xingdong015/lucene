/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util;

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_MASK;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SHIFT;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.util.ByteBlockPool.DirectAllocator;

/**
 * {@link BytesRefHash} is a special purpose hash-map like data-structure optimized for {@link
 * BytesRef} instances. BytesRefHash maintains mappings of byte arrays to ids
 * (Map&lt;BytesRef,int&gt;) storing the hashed bytes efficiently in continuous storage. The mapping
 * to the id is encapsulated inside {@link BytesRefHash} and is guaranteed to be increased for each
 * added {@link BytesRef}.
 *
 * <p>Note: The maximum capacity {@link BytesRef} instance passed to {@link #add(BytesRef)} must not
 * be longer than {@link ByteBlockPool#BYTE_BLOCK_SIZE}-2. The internal storage is limited to 2GB
 * total byte storage.
 *
 * @lucene.internal
 *
 * BytesRefHash就把它理解成是一个Map。它有两个特点：
 *
 * 添加新pair时，只提供key，value是在新增的时候为key分配的id，是从0递增的整型。
 * 不仅是O(1)时间通过key找value，也是O(1)时间通过value找key。这种实现我们可以借鉴。
 */
public final class BytesRefHash implements Accountable {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(BytesRefHash.class)
          +
          // size of Counter
          RamUsageEstimator.primitiveSizes.get(long.class);

  public static final int DEFAULT_CAPACITY = 16;

  // the following fields are needed by comparator,
  // so package private to prevent access$-methods:
  // 存储所有key值，也就是BytesRef
  final ByteBlockPool pool;
  // 下标是每个BytesRef的id，值是在pool中的起始位置，通过这个变量实现了O(1)时间通过value找key
  //根据 bytesRef 找到在 pool中的位置、具体可以查看  {@link #get(BytesRef)} 方法
  int[] bytesStart;

  // 哈希表的最大容量
  private int hashSize;
  // 超过这个值，需要rehash
  private int hashHalfSize;
  // 用来获取BytesRef哈希值在ids数组中的下标
  private int hashMask;
  // 目前哈希表中的总个数
  private int count;
  // 辅助变量，在清空或者压缩的时候使用，不用关注
  private int lastCount = -1;
  // 下标是BytesRef哈希值与hashMask的&操作，值是BytesRef的id
  private int[] ids;
  // bytesStart数组初始化，重置，扩容的辅助类，不用关注
  private final BytesStartArray bytesStartArray;
  // 统计内存占用，不用关注
  private Counter bytesUsed;

  /**
   * Creates a new {@link BytesRefHash} with a {@link ByteBlockPool} using a {@link
   * DirectAllocator}.
   */
  public BytesRefHash() {
    this(new ByteBlockPool(new DirectAllocator()));
  }

  /** Creates a new {@link BytesRefHash} */
  public BytesRefHash(ByteBlockPool pool) {
    this(pool, DEFAULT_CAPACITY, new DirectBytesStartArray(DEFAULT_CAPACITY));
  }

  /** Creates a new {@link BytesRefHash} */
  public BytesRefHash(ByteBlockPool pool, int capacity, BytesStartArray bytesStartArray) {
    hashSize = capacity;
    hashHalfSize = hashSize >> 1;
    hashMask = hashSize - 1;
    this.pool = pool;
    ids = new int[hashSize];
    Arrays.fill(ids, -1);
    this.bytesStartArray = bytesStartArray;
    bytesStart = bytesStartArray.init();
    bytesUsed =
        bytesStartArray.bytesUsed() == null ? Counter.newCounter() : bytesStartArray.bytesUsed();
    bytesUsed.addAndGet(hashSize * Integer.BYTES);
  }

  /**
   * Returns the number of {@link BytesRef} values in this {@link BytesRefHash}.
   *
   * @return the number of {@link BytesRef} values in this {@link BytesRefHash}.
   */
  public int size() {
    return count;
  }

  /**
   * Populates and returns a {@link BytesRef} with the bytes for the given bytesID.
   *
   * <p>Note: the given bytesID must be a positive integer less than the current size ({@link
   * #size()})
   *
   * @param bytesID the id
   * @param ref the {@link BytesRef} to populate
   * @return the given BytesRef instance populated with the bytes for the given bytesID
   */
  public BytesRef get(int bytesID, BytesRef ref) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID < bytesStart.length : "bytesID exceeds byteStart len: " + bytesStart.length;
    // 从pool的  bytesStart[bytesID] 的位置读取ref，注意会先读长度，再读内容，详见ByteBlockPool
    pool.setBytesRef(ref, bytesStart[bytesID]);
    return ref;
  }

  /**
   * Returns the ids array in arbitrary order. Valid ids start at offset of 0 and end at a limit of
   * {@link #size()} - 1
   *
   * <p>Note: This is a destructive operation. {@link #clear()} must be called in order to reuse
   * this {@link BytesRefHash} instance.
   *
   * @lucene.internal
   *
   * 因为底层数据是当成哈希表使用，所以数组中存在空隙。在数据全部处理完之后，要进行持久化，
   * 先压缩id数组返回进行下一步的处理：
   */
  public int[] compact() {
    assert bytesStart != null : "bytesStart is null - not initialized";
    // 如果存在空节点，则upto会停留在空节点的下标，等待填充
    int upto = 0;
    // 从前往后找
    for (int i = 0; i < hashSize; i++) {
      // 如果当前的节点非空，则需要判断在其之前是否有空间点可以填充
      if (ids[i] != -1) {
        // upto < i 说明存在空节点，则把当前节点移动到空节点
        if (upto < i) {
          ids[upto] = ids[i];
          ids[i] = -1;
        }
        upto++;
      }
    }

    assert upto == count;
    lastCount = count;
    return ids;
  }

  /**
   * Returns the values array sorted by the referenced byte values.
   *
   * <p>Note: This is a destructive operation. {@link #clear()} must be called in order to reuse
   * this {@link BytesRefHash} instance.
   *
   * 按ByteRef对ids数组排序。在倒排最后要落盘持久化的时候，会先对所有的term排序，
   * 因为term字典（FST）需要term列表是有序的：
   */
  public int[] sort() {
    final int[] compact = compact();
    // lucene实现的排序算法的框架，get方法就是获取比较的值来进行比较
    new StringMSBRadixSorter() {

      BytesRef scratch = new BytesRef();

      @Override
      protected void swap(int i, int j) {
        int tmp = compact[i];
        compact[i] = compact[j];
        compact[j] = tmp;
      }

      @Override
      protected BytesRef get(int i) {
        pool.setBytesRef(scratch, bytesStart[compact[i]]);
        return scratch;
      }
    }.sort(0, count);
    return compact;
  }

  private boolean equals(int id, BytesRef b) {
    final int textStart = bytesStart[id];
    final byte[] bytes = pool.buffers[textStart >> BYTE_BLOCK_SHIFT];
    int pos = textStart & BYTE_BLOCK_MASK;
    final int length;
    final int offset;
    if ((bytes[pos] & 0x80) == 0) {
      // length is 1 byte
      length = bytes[pos];
      offset = pos + 1;
    } else {
      // length is 2 bytes
      length = ((short) BitUtil.VH_BE_SHORT.get(bytes, pos)) & 0x7FFF;
      offset = pos + 2;
    }
    return Arrays.equals(bytes, offset, offset + length, b.bytes, b.offset, b.offset + b.length);
  }
  //在清空哈希表之后，需要收缩哈希表：
  private boolean shrink(int targetSize) {
    // Cannot use ArrayUtil.shrink because we require power
    // of 2:
    int newSize = hashSize;
    // 如果现有的容量大于等于8才需要收缩
    while (newSize >= 8 && newSize / 4 > targetSize) {
      newSize /= 2;
    }
    // 如果需要收缩
    if (newSize != hashSize) {
      bytesUsed.addAndGet(Integer.BYTES * -(hashSize - newSize));
      hashSize = newSize;
      ids = new int[hashSize];
      Arrays.fill(ids, -1);
      hashHalfSize = newSize / 2;
      hashMask = newSize - 1;
      return true;
    } else {
      return false;
    }
  }

  /** Clears the {@link BytesRef} which maps to the given {@link BytesRef} */
  public void clear(boolean resetPool) {
    lastCount = count;
    count = 0;
    if (resetPool) {
      pool.reset(false, false); // we don't need to 0-fill the buffers
    }
    bytesStart = bytesStartArray.clear();
    // 收缩成功则ids数组就已经重置了
    if (lastCount != -1 && shrink(lastCount)) {
      // shrink clears the hash entries
      return;
    }
    Arrays.fill(ids, -1);
  }

  public void clear() {
    clear(true);
  }

  /** Closes the BytesRefHash and releases all internally used memory */
  public void close() {
    clear(true);
    ids = null;
    bytesUsed.addAndGet(Integer.BYTES * -hashSize);
  }

  /**
   * Adds a new {@link BytesRef}
   *
   * @param bytes the bytes to hash
   * @return the id the given bytes are hashed if there was no mapping for the given bytes,
   *     otherwise <code>(-(id)-1)</code>. This guarantees that the return value will always be
   *     &gt;= 0 if the given bytes haven't been hashed before.
   * @throws MaxBytesLengthExceededException if the given bytes are {@code > 2 +} {@link
   *     ByteBlockPool#BYTE_BLOCK_SIZE}
   */
  public int add(BytesRef bytes) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    final int length = bytes.length;
    // final position
    // 获取在ids中的下标
    final int hashPos = findHash(bytes);
    int e = ids[hashPos];
    // 如果bytes是新加入的
    if (e == -1) {
      // new entry
      // 加2是因为长度最多用两个字节存储
      final int len2 = 2 + bytes.length;
      // 如果当前buffer存不下，则获取下一个buffer
      if (len2 + pool.byteUpto > BYTE_BLOCK_SIZE) {
        if (len2 > BYTE_BLOCK_SIZE) {
          throw new MaxBytesLengthExceededException(
              "bytes can be at most " + (BYTE_BLOCK_SIZE - 2) + " in length; got " + bytes.length);
        }
        pool.nextBuffer();
      }
      final byte[] buffer = pool.buffer;
      final int bufferUpto = pool.byteUpto;
      // 当前的bytestarts满了，则扩容
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: " + bytesStart.length;
      }
      // id是递增的
      e = count++;
      // 记录bytes在pool中的offset
      bytesStart[e] = bufferUpto + pool.byteOffset;

      // We first encode the length, followed by the
      // bytes. Length is encoded as vInt, but will consume
      // 1 or 2 bytes at most (we reject too-long terms,
      // above).
      // 先存term长度，再存term.
      // 小于128用1个字节，否则用2个字节。
      if (length < 128) {
        // 1 byte to store length
        buffer[bufferUpto] = (byte) length;
        pool.byteUpto += length + 1;
        assert length >= 0 : "Length must be positive: " + length;
        System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 1, length);
      } else {
        // 2 byte to store length
        BitUtil.VH_BE_SHORT.set(buffer, bufferUpto, (short) (length | 0x8000));
        pool.byteUpto += length + 2;
        System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 2, length);
      }
      assert ids[hashPos] == -1;
      // 把id存起来
      ids[hashPos] = e;

      // 如果哈希表过半，则rehash
      if (count == hashHalfSize) {
        rehash(2 * hashSize, true);
      }
      return e;
    }
    // 如果bytes是已经存在的
    return -(e + 1);
  }

  /**
   * Returns the id of the given {@link BytesRef}.
   *
   * @param bytes the bytes to look for
   * @return the id of the given bytes, or {@code -1} if there is no mapping for the given bytes.
   */
  //O(1)时间通过key找value的方法。
  public int find(BytesRef bytes) {
    return ids[findHash(bytes)];
  }

  private int findHash(BytesRef bytes) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    // 获取bytes的哈希值
    int code = doHash(bytes.bytes, bytes.offset, bytes.length);

    // final position
    // 获取在ids中的下标
    int hashPos = code & hashMask;
    int e = ids[hashPos];
    // 如果哈希的下标已经有值了，并且不是bytes，则表示冲突了
    if (e != -1 && !equals(e, bytes)) {
      // Conflict; use linear probe to find an open slot
      // (see LUCENE-5604):
      do {
        // 冲突了，采用线性探测循环往后找
        code++;
        hashPos = code & hashMask;
        e = ids[hashPos];
      } while (e != -1 && !equals(e, bytes));
    }

    return hashPos;
  }

  /**
   * Adds a "arbitrary" int offset instead of a BytesRef term. This is used in the indexer to hold
   * the hash for term vectors, because they do not redundantly store the byte[] term directly and
   * instead reference the byte[] term already stored by the postings BytesRefHash. See add(int
   * textStart) in TermsHashPerField.
   * 没有bytes，但是为offset分配一个id：
   */
  public int addByPoolOffset(int offset) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    // final position
    int code = offset;
    int hashPos = offset & hashMask;
    int e = ids[hashPos];
    if (e != -1 && bytesStart[e] != offset) {
      // Conflict; use linear probe to find an open slot
      // (see LUCENE-5604):
      do {
        code++;
        hashPos = code & hashMask;
        e = ids[hashPos];
      } while (e != -1 && bytesStart[e] != offset);
    }
    if (e == -1) {
      // new entry
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: " + bytesStart.length;
      }
      e = count++;
      bytesStart[e] = offset;
      assert ids[hashPos] == -1;
      ids[hashPos] = e;

      if (count == hashHalfSize) {
        rehash(2 * hashSize, false);
      }
      return e;
    }
    return -(e + 1);
  }

  /**
   * Called when hash is too small ({@code > 50%} occupied) or too large ({@code < 20%} occupied).
   */
  private void rehash(final int newSize, boolean hashOnData) {
    final int newMask = newSize - 1;
    bytesUsed.addAndGet(Integer.BYTES * (newSize));
    final int[] newHash = new int[newSize];
    Arrays.fill(newHash, -1);
    for (int i = 0; i < hashSize; i++) {
      final int e0 = ids[i];
      if (e0 != -1) {
        int code;
        if (hashOnData) {
          final int off = bytesStart[e0];
          final int start = off & BYTE_BLOCK_MASK;
          final byte[] bytes = pool.buffers[off >> BYTE_BLOCK_SHIFT];
          final int len;
          int pos;
          if ((bytes[start] & 0x80) == 0) {
            // length is 1 byte
            len = bytes[start];
            pos = start + 1;
          } else {
            len = ((short) BitUtil.VH_BE_SHORT.get(bytes, start)) & 0x7FFF;
            pos = start + 2;
          }
          code = doHash(bytes, pos, len);
        } else {
          code = bytesStart[e0];
        }

        int hashPos = code & newMask;
        assert hashPos >= 0;
        if (newHash[hashPos] != -1) {
          // Conflict; use linear probe to find an open slot
          // (see LUCENE-5604):
          do {
            code++;
            hashPos = code & newMask;
          } while (newHash[hashPos] != -1);
        }
        newHash[hashPos] = e0;
      }
    }

    hashMask = newMask;
    bytesUsed.addAndGet(Integer.BYTES * (-ids.length));
    ids = newHash;
    hashSize = newSize;
    hashHalfSize = newSize / 2;
  }

  // TODO: maybe use long?  But our keys are typically short...
  private int doHash(byte[] bytes, int offset, int length) {
    return StringHelper.murmurhash3_x86_32(bytes, offset, length, StringHelper.GOOD_FAST_HASH_SEED);
  }

  /**
   * reinitializes the {@link BytesRefHash} after a previous {@link #clear()} call. If {@link
   * #clear()} has not been called previously this method has no effect.
   */
  public void reinit() {
    if (bytesStart == null) {
      bytesStart = bytesStartArray.init();
    }

    if (ids == null) {
      ids = new int[hashSize];
      bytesUsed.addAndGet(Integer.BYTES * hashSize);
    }
  }

  /**
   * Returns the bytesStart offset into the internally used {@link ByteBlockPool} for the given
   * bytesID
   *
   * @param bytesID the id to look up
   * @return the bytesStart offset into the internally used {@link ByteBlockPool} for the given id
   */
  public int byteStart(int bytesID) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID >= 0 && bytesID < count : bytesID;
    return bytesStart[bytesID];
  }

  @Override
  public long ramBytesUsed() {
    long size =
        BASE_RAM_BYTES
            + RamUsageEstimator.sizeOfObject(bytesStart)
            + RamUsageEstimator.sizeOfObject(ids)
            + RamUsageEstimator.sizeOfObject(pool);
    return size;
  }

  /**
   * Thrown if a {@link BytesRef} exceeds the {@link BytesRefHash} limit of {@link
   * ByteBlockPool#BYTE_BLOCK_SIZE}-2.
   */
  @SuppressWarnings("serial")
  public static class MaxBytesLengthExceededException extends RuntimeException {
    MaxBytesLengthExceededException(String message) {
      super(message);
    }
  }

  /** Manages allocation of the per-term addresses. */
  public abstract static class BytesStartArray {
    /**
     * Initializes the BytesStartArray. This call will allocate memory
     *
     * @return the initialized bytes start array
     */
    public abstract int[] init();

    /**
     * Grows the {@link BytesStartArray}
     *
     * @return the grown array
     */
    public abstract int[] grow();

    /**
     * clears the {@link BytesStartArray} and returns the cleared instance.
     *
     * @return the cleared instance, this might be <code>null</code>
     */
    public abstract int[] clear();

    /**
     * A {@link Counter} reference holding the number of bytes used by this {@link BytesStartArray}.
     * The {@link BytesRefHash} uses this reference to track it memory usage
     *
     * @return a {@link AtomicLong} reference holding the number of bytes used by this {@link
     *     BytesStartArray}.
     */
    public abstract Counter bytesUsed();
  }

  /**
   * A simple {@link BytesStartArray} that tracks memory allocation using a private {@link Counter}
   * instance.
   */
  public static class DirectBytesStartArray extends BytesStartArray {
    // TODO: can't we just merge this w/
    // TrackingDirectBytesStartArray...?  Just add a ctor
    // that makes a private bytesUsed?

    protected final int initSize;
    private int[] bytesStart;
    private final Counter bytesUsed;

    public DirectBytesStartArray(int initSize, Counter counter) {
      this.bytesUsed = counter;
      this.initSize = initSize;
    }

    public DirectBytesStartArray(int initSize) {
      this(initSize, Counter.newCounter());
    }

    @Override
    public int[] clear() {
      return bytesStart = null;
    }

    @Override
    public int[] grow() {
      assert bytesStart != null;
      return bytesStart = ArrayUtil.grow(bytesStart, bytesStart.length + 1);
    }

    @Override
    public int[] init() {
      return bytesStart = new int[ArrayUtil.oversize(initSize, Integer.BYTES)];
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }
}
