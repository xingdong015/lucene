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

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

import java.util.Arrays;
import java.util.List;

/**
 * Class that Posting and PostingVector use to write byte streams into shared fixed-size byte[]
 * arrays. The idea is to allocate slices of increasing lengths For example, the first slice is 5
 * bytes, the next slice is 14, etc. We start by writing our bytes into the first 5 bytes. When we
 * hit the end of the slice, we allocate the next slice and then write the address of the new slice
 * into the last 4 bytes of the previous slice (the "forwarding address").
 *
 * <p>Each slice is filled with 0's initially, and we mark the end with a non-zero byte. This way
 * the methods that are writing into the slice don't need to record its length and instead allocate
 * a new slice once they hit a non-zero byte.
 *
 * @lucene.internal
 */
public final class ByteBlockPool implements Accountable {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(ByteBlockPool.class);

  public static final int BYTE_BLOCK_SHIFT = 15;
  public static final int BYTE_BLOCK_SIZE = 1 << BYTE_BLOCK_SHIFT;
  public static final int BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;

  /** Abstract class for allocating and freeing byte blocks. */
  public abstract static class Allocator {
    protected final int blockSize;

    protected Allocator(int blockSize) {
      this.blockSize = blockSize;
    }

    public abstract void recycleByteBlocks(byte[][] blocks, int start, int end);

    public void recycleByteBlocks(List<byte[]> blocks) {
      final byte[][] b = blocks.toArray(new byte[blocks.size()][]);
      recycleByteBlocks(b, 0, b.length);
    }

    public byte[] getByteBlock() {
      return new byte[blockSize];
    }
  }

  /** A simple {@link Allocator} that never recycles. */
  public static final class DirectAllocator extends Allocator {

    public DirectAllocator() {
      this(BYTE_BLOCK_SIZE);
    }

    public DirectAllocator(int blockSize) {
      super(blockSize);
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {}
  }

  /** A simple {@link Allocator} that never recycles, but tracks how much total RAM is in use. */
  public static class DirectTrackingAllocator extends Allocator {
    private final Counter bytesUsed;

    public DirectTrackingAllocator(Counter bytesUsed) {
      this(BYTE_BLOCK_SIZE, bytesUsed);
    }

    public DirectTrackingAllocator(int blockSize, Counter bytesUsed) {
      super(blockSize);
      this.bytesUsed = bytesUsed;
    }

    @Override
    public byte[] getByteBlock() {
      bytesUsed.addAndGet(blockSize);
      return new byte[blockSize];
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {
      bytesUsed.addAndGet(-((end - start) * blockSize));
      for (int i = start; i < end; i++) {
        blocks[i] = null;
      }
    }
  }
  ;

  /**
   * array of buffers currently used in the pool. Buffers are allocated if needed don't modify this
   * outside of this class.
   *
   * 此类中用一个二维数组用来存放term的倒排表数据。
   */
  public byte[][] buffers = new byte[10][];

  /** index into the buffers array pointing to the current buffer used as the head */
  //指向二元buffers的第一元指针, 当前正在向第几个byte[]存放数据
  private int bufferUpto = -1; // Which buffer we are upto
  /** Where we are in head buffer */
  public int byteUpto = BYTE_BLOCK_SIZE;

  /** Current head buffer */
  public byte[] buffer;
  /** Current head offset */
  // buffer在所有buffers的位置（当前buffer在全局的偏移量），(bufferUpto -1)*BYTE_BLOCK_SIZE
  public int byteOffset = -BYTE_BLOCK_SIZE;

  private final Allocator allocator;

  public ByteBlockPool(Allocator allocator) {
    this.allocator = allocator;
  }

  /**
   * Resets the pool to its initial state reusing the first buffer and fills all buffers with <code>
   * 0</code> bytes before they reused or passed to {@link Allocator#recycleByteBlocks(byte[][],
   * int, int)}. Calling {@link ByteBlockPool#nextBuffer()} is not needed after reset.
   */
  public void reset() {
    reset(true, true);
  }

  /**
   * Expert: Resets the pool to its initial state reusing the first buffer. Calling {@link
   * ByteBlockPool#nextBuffer()} is not needed after reset.
   *
   * @param zeroFillBuffers if <code>true</code> the buffers are filled with <code>0</code>. This
   *     should be set to <code>true</code> if this pool is used with slices.
   * @param reuseFirst if <code>true</code> the first buffer will be reused and calling {@link
   *     ByteBlockPool#nextBuffer()} is not needed after reset iff the block pool was used before
   *     ie. {@link ByteBlockPool#nextBuffer()} was called before.
   */
  public void reset(boolean zeroFillBuffers, boolean reuseFirst) {
    if (bufferUpto != -1) {
      // We allocated at least one buffer

      if (zeroFillBuffers) {
        for (int i = 0; i < bufferUpto; i++) {
          // Fully zero fill buffers that we fully used
          Arrays.fill(buffers[i], (byte) 0);
        }
        // Partial zero fill the final buffer
        Arrays.fill(buffers[bufferUpto], 0, byteUpto, (byte) 0);
      }

      if (bufferUpto > 0 || !reuseFirst) {
        final int offset = reuseFirst ? 1 : 0;
        // Recycle all but the first buffer
        allocator.recycleByteBlocks(buffers, offset, 1 + bufferUpto);
        Arrays.fill(buffers, offset, 1 + bufferUpto, null);
      }
      if (reuseFirst) {
        // Re-use the first buffer
        bufferUpto = 0;
        byteUpto = 0;
        byteOffset = 0;
        buffer = buffers[0];
      } else {
        bufferUpto = -1;
        byteUpto = BYTE_BLOCK_SIZE;
        byteOffset = -BYTE_BLOCK_SIZE;
        buffer = null;
      }
    }
  }

  /**
   * Advances the pool to its next buffer. This method should be called once after the constructor
   * to initialize the pool. In contrast to the constructor a {@link ByteBlockPool#reset()} call
   * will advance the pool to its first buffer immediately.
   */
  public void nextBuffer() {
    // buffers中已存满buffer了, 需要扩容
    if (1 + bufferUpto == buffers.length) {
      byte[][] newBuffers =
          new byte[ArrayUtil.oversize(buffers.length + 1, NUM_BYTES_OBJECT_REF)][];
      // 只是拷贝了数组引用，并没有copy数组, 这里体现了ByteBlockPool的优势
      //第一维度数组对于第二维度数组的引用、
      System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
      buffers = newBuffers;
    }
    //产生一个新的buffer, 大小为32kb, 供slice来分配数据
    buffer = buffers[1 + bufferUpto] = allocator.getByteBlock();
    bufferUpto++;//buffers写入位置+1

    byteUpto = 0;//buffer写入位置
    byteOffset += BYTE_BLOCK_SIZE;//指针位置初始化
  }

  /**
   * Allocates a new slice with the given size.
   *
   * @see ByteBlockPool#FIRST_LEVEL_SIZE
   */
  // 从当前buffer中申请一个size大小的slice
  public int newSlice(final int size) {
    if (byteUpto > BYTE_BLOCK_SIZE - size) nextBuffer();
    // 此时能保证当前buffer一定可以存的下size了
    final int upto = byteUpto;
    byteUpto += size;
    //赋值为16 调用时 会从byteUpto开始写入，当遇到buffer[pos]位置不为0 时，
    //会调用allocSlice方法 通过 16&15得到当前 NEXT_LEVEL_ARRAY 中 level
    buffer[byteUpto - 1] = 16;
    //申请的slice的相对起始位置
    return upto;
  }

  public static void main(String[] args) {
    System.out.println(16 & 15);
  }

  // Size of each slice.  These arrays should be at most 16
  // elements (index is encoded with 4 bits).  First array
  // is just a compact way to encode X+1 with a max.  Second
  // array is the length of each slice, ie first slice is 5
  // bytes, next slice is 14 bytes, etc.

  /**
   * An array holding the offset into the {@link ByteBlockPool#LEVEL_SIZE_ARRAY} to quickly navigate
   * to the next slice level.
   */
  public static final int[] NEXT_LEVEL_ARRAY = {1, 2, 3, 4, 5, 6, 7, 8, 9, 9};

  /** An array holding the level sizes for byte slices. */
  public static final int[] LEVEL_SIZE_ARRAY = {5, 14, 20, 30, 40, 40, 80, 80, 120, 200};

  /**
   * The first level size for new slices
   *
   * @see ByteBlockPool#newSlice(int)
   */
  public static final int FIRST_LEVEL_SIZE = LEVEL_SIZE_ARRAY[0];

  /**
   * Creates a new byte slice with the given starting size and returns the slices offset in the
   * pool.
   */
  public int allocSlice(final byte[] slice, final int upto) {
    return allocKnownSizeSlice(slice, upto) >> 8;
  }

  /**
   * Create a new byte slice with the given starting size return the slice offset in the pool and
   * length. The lower 8 bits of the returned int represent the length of the slice, and the upper
   * 24 bits represent the offset.
   */
  public int allocKnownSizeSlice(final byte[] slice, final int upto) {
    final int level = slice[upto] & 15;
    final int newLevel = NEXT_LEVEL_ARRAY[level];
    final int newSize = LEVEL_SIZE_ARRAY[newLevel];

    // Maybe allocate another block
    if (byteUpto > BYTE_BLOCK_SIZE - newSize) {
      nextBuffer();
    }

    final int newUpto = byteUpto;
    final int offset = newUpto + byteOffset;
    byteUpto += newSize;

    // Copy forward the past 3 bytes (which we are about to overwrite with the forwarding address).
    // We actually copy 4 bytes at once since VarHandles make it cheap.
    // 获取当前upto前面三个字节，也就是哨兵前面的3个字节。这是为了把这部分空间留出来，设置下一个slice的地址。
    // 注意是小端读取，通过与运算把哨兵字节（upto指向的字节）的过滤了。
    int past3Bytes = ((int) BitUtil.VH_LE_INT.get(slice, upto - 3)) & 0xFFFFFF;
    // Ensure we're not changing the content of `buffer` by setting 4 bytes instead of 3. This
    // should never happen since the next `newSize` bytes must be equal to 0.
    assert buffer[newUpto + 3] == 0;
    // 把这三个字节写入新slice的开头
    BitUtil.VH_LE_INT.set(buffer, newUpto, past3Bytes);

    // Write forwarding address at end of last slice:
    // 把新slice的地址写入前一个slice腾出三个字节的地方，包括了最后一个哨兵字节，总共地址是4个字节。
    BitUtil.VH_LE_INT.set(slice, upto - 3, offset);

    // Write new level:
    // 当前的slice加入哨兵，可以看到这里把当前的level也存起来了，和前面通过当前level获取新level呼应
    buffer[byteUpto - 1] = (byte) (16 | newLevel);

    return ((newUpto + 3) << 8) | (newSize - 3);
  }

  /**
   * Fill the provided {@link BytesRef} with the bytes at the specified offset/length slice. This
   * will avoid copying the bytes, if the slice fits into a single block; otherwise, it uses the
   * provided {@link BytesRefBuilder} to copy bytes over.
   */
  void setBytesRef(BytesRefBuilder builder, BytesRef result, long offset, int length) {
    result.length = length;

    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    byte[] buffer = buffers[bufferIndex];
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    if (pos + length <= BYTE_BLOCK_SIZE) {
      // common case where the slice lives in a single block: just reference the buffer directly
      // without copying
      result.bytes = buffer;
      result.offset = pos;
    } else {
      // uncommon case: the slice spans at least 2 blocks, so we must copy the bytes:
      builder.grow(length);
      result.bytes = builder.get().bytes;
      result.offset = 0;
      readBytes(offset, result.bytes, 0, length);
    }
  }

  // Fill in a BytesRef from term's length & bytes encoded in
  // byte block
  public void setBytesRef(BytesRef term, int textStart) {
    final byte[] bytes = term.bytes = buffers[textStart >> BYTE_BLOCK_SHIFT];
    int pos = textStart & BYTE_BLOCK_MASK;
    if ((bytes[pos] & 0x80) == 0) {
      // length is 1 byte
      term.length = bytes[pos];
      term.offset = pos + 1;
    } else {
      // length is 2 bytes
      term.length = ((short) BitUtil.VH_BE_SHORT.get(bytes, pos)) & 0x7FFF;
      term.offset = pos + 2;
    }
    assert term.length >= 0;
  }

  /** Appends the bytes in the provided {@link BytesRef} at the current position. */
  public void append(final BytesRef bytes) {
    int bytesLeft = bytes.length;
    int offset = bytes.offset;
    while (bytesLeft > 0) {
      int bufferLeft = BYTE_BLOCK_SIZE - byteUpto;
      if (bytesLeft < bufferLeft) {
        // fits within current buffer
        System.arraycopy(bytes.bytes, offset, buffer, byteUpto, bytesLeft);
        byteUpto += bytesLeft;
        break;
      } else {
        // fill up this buffer and move to next one
        if (bufferLeft > 0) {
          System.arraycopy(bytes.bytes, offset, buffer, byteUpto, bufferLeft);
        }
        nextBuffer();
        bytesLeft -= bufferLeft;
        offset += bufferLeft;
      }
    }
  }

  /**
   * Reads bytes out of the pool starting at the given offset with the given length into the given
   * byte array at offset <code>off</code>.
   *
   * <p>Note: this method allows to copy across block boundaries.
   */
  public void readBytes(final long offset, final byte[] bytes, int bytesOffset, int bytesLength) {
    int bytesLeft = bytesLength;
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    while (bytesLeft > 0) {
      byte[] buffer = buffers[bufferIndex++];
      int chunk = Math.min(bytesLeft, BYTE_BLOCK_SIZE - pos);
      System.arraycopy(buffer, pos, bytes, bytesOffset, chunk);
      bytesOffset += chunk;
      bytesLeft -= chunk;
      pos = 0;
    }
  }

  /**
   * Set the given {@link BytesRef} so that its content is equal to the {@code ref.length} bytes
   * starting at {@code offset}. Most of the time this method will set pointers to internal
   * data-structures. However, in case a value crosses a boundary, a fresh copy will be returned. On
   * the contrary to {@link #setBytesRef(BytesRef, int)}, this does not expect the length to be
   * encoded with the data.
   */
  public void setRawBytesRef(BytesRef ref, final long offset) {
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    if (pos + ref.length <= BYTE_BLOCK_SIZE) {
      ref.bytes = buffers[bufferIndex];
      ref.offset = pos;
    } else {
      ref.bytes = new byte[ref.length];
      ref.offset = 0;
      readBytes(offset, ref.bytes, 0, ref.length);
    }
  }

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES;
    size += RamUsageEstimator.sizeOfObject(buffer);
    size += RamUsageEstimator.shallowSizeOf(buffers);
    for (byte[] buf : buffers) {
      if (buf == buffer) {
        continue;
      }
      size += RamUsageEstimator.sizeOfObject(buf);
    }
    return size;
  }
}
