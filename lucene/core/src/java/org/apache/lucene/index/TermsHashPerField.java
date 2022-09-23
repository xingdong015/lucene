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
package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefHash.BytesStartArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;

/**
 * This class stores streams of information per term without knowing the size of the stream ahead of
 * time. Each stream typically encodes one level of information like term frequency per document or
 * term proximity. Internally this class allocates a linked list of slices that can be read by a
 * {@link ByteSliceReader} for each term. Terms are first deduplicated in a {@link BytesRefHash}
 * once this is done internal data-structures point to the current offset of each stream that can be
 * written to.
 *
 * 构建的核心逻辑在TermsHashPerField中，每个field对应了一个TermsHashPerField，
 * field的所有倒排数据都存储在TermsHashPerField中。
 */
abstract class TermsHashPerField implements Comparable<TermsHashPerField> {
  private static final int HASH_INIT_SIZE = 4;
  // 倒排和词向量都会处理term信息，所以倒排和词向量是使用责任链的模式实现，nextPerField就是下一个要处理term信息的组件
  private final TermsHashPerField nextPerField;
  // 记录term在bytePool中下一个要写入的位置
  private final IntBlockPool intPool;
  // 记录term和term的相关的倒排信息，倒排信息是以stream的方式写入的
  final ByteBlockPool bytePool;
  // for each term we store an integer per stream that points into the bytePool above
  // the address is updated once data is written to the stream to point to the next free offset
  // in the terms stream. The start address for the stream is stored in
  // postingsArray.byteStarts[termId]
  // This is initialized in the #addTerm method, either to a brand new per term stream if the term
  // is new or
  // to the addresses where the term stream was written to when we saw it the last time.
  // intPool中当前使用的buffer
  private int[] termStreamAddressBuffer;
  // intPool中当前使用的buffer当前定位到的offset
  private int streamAddressOffset;
  // term的信息分为几个数据源
  private final int streamCount;
  // 字段名称
  private final String fieldName;
  // 索引选项：是否记录频率，是否记录position，是否记录offset等
  final IndexOptions indexOptions;
  /* This stores the actual term bytes for postings and offsets into the parent hash in the case that this
   * TermsHashPerField is hashing term vectors.*/
  // 为term分配唯一id，并且用来判断term是不是第一次出现
  private final BytesRefHash bytesHash;
  // 倒排内存结构构建的辅助类
  ParallelPostingsArray postingsArray;
  private int lastDocID; // only with assert

  /**
   * streamCount: how many streams this field stores per term. E.g. doc(+freq) is 1 stream,
   * prox+offset is a second.
   */
  TermsHashPerField(
      int streamCount,
      IntBlockPool intPool,
      ByteBlockPool bytePool,
      ByteBlockPool termBytePool,
      Counter bytesUsed,
      TermsHashPerField nextPerField,
      String fieldName,
      IndexOptions indexOptions) {
    this.intPool = intPool;
    this.bytePool = bytePool;
    this.streamCount = streamCount;
    this.fieldName = fieldName;
    this.nextPerField = nextPerField;
    assert indexOptions != IndexOptions.NONE;
    this.indexOptions = indexOptions;
    PostingsBytesStartArray byteStarts = new PostingsBytesStartArray(this, bytesUsed);
    bytesHash = new BytesRefHash(termBytePool, HASH_INIT_SIZE, byteStarts);
  }

  void reset() {
    bytesHash.clear(false);
    sortedTermIDs = null;
    if (nextPerField != null) {
      nextPerField.reset();
    }
  }

  final void initReader(ByteSliceReader reader, int termID, int stream) {
    assert stream < streamCount;
    int streamStartOffset = postingsArray.addressOffset[termID];
    final int[] streamAddressBuffer =
        intPool.buffers[streamStartOffset >> IntBlockPool.INT_BLOCK_SHIFT];
    final int offsetInAddressBuffer = streamStartOffset & IntBlockPool.INT_BLOCK_MASK;
    reader.init(
        bytePool,
        postingsArray.byteStarts[termID] + stream * ByteBlockPool.FIRST_LEVEL_SIZE,
        streamAddressBuffer[offsetInAddressBuffer + stream]);
  }
  // 所有的文档都处理完之后，按照term大小对termid进行排序，持久化的时候是按照term顺序处理的
  private int[] sortedTermIDs;

  /**
   * Collapse the hash table and sort in-place; also sets this.sortedTermIDs to the results This
   * method must not be called twice unless {@link #reset()} or {@link #reinitHash()} was called.
   */
  final void sortTerms() {
    assert sortedTermIDs == null;
    sortedTermIDs = bytesHash.sort();
  }

  /** Returns the sorted term IDs. {@link #sortTerms()} must be called before */
  final int[] getSortedTermIDs() {
    assert sortedTermIDs != null;
    return sortedTermIDs;
  }

  final void reinitHash() {
    sortedTermIDs = null;
    bytesHash.reinit();
  }

  private boolean doNextCall;

  // Secondary entry point (for 2nd & subsequent TermsHash),
  // because token text has already been "interned" into
  // textStart, so we hash by textStart.  term vectors use
  // this API.
  private void add(int textStart, final int docID) throws IOException {
    int termID = bytesHash.addByPoolOffset(textStart);
    if (termID >= 0) { // New posting
      // First time we are seeing this token since we last
      // flushed the hash.
      initStreamSlices(termID, docID);
    } else {
      positionStreamSlice(termID, docID);
    }
  }

  /**
   * Called when we first encounter a new term. We must allocate slies to store the postings (vInt
   * compressed doc/freq/prox), and also the int pointers to where (in our ByteBlockPool storage)
   * the postings for this term begin.
   */
  private void initStreamSlices(int termID, int docID) throws IOException {
    // Init stream slices
    // intPool的当前buffer不足，则需要获取下一个buffer。
    // 这里需要注意的是，这样判断确保了termID对应的intPool中的数据都是相连的，是为了读取的时候考虑的
    if (streamCount + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
      // not enough space remaining in this buffer -- jump to next buffer and lose this remaining
      // piece
      intPool.nextBuffer();
    }
    //如果 bytePool 中当前buffer无法容纳 streamCount 个level 0的slice，则创建新的 buffer
    // 这里需要注意的是，这样判断确保了termID对应的bytePool中的所有的stream的第一个slice都是相连的，
    // 是为了读取的时候考虑的  这里的 2？？？？
    if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto
        < (2 * streamCount) * ByteBlockPool.FIRST_LEVEL_SIZE) {
      // can we fit at least one byte per stream in the current buffer, if not allocate a new one
      bytePool.nextBuffer();
    }

    termStreamAddressBuffer = intPool.buffer;
    streamAddressOffset = intPool.intUpto;
    intPool.intUpto += streamCount; // advance the pool to reserve the N streams for this term
    // 记录termID在intPool中的位置
    postingsArray.addressOffset[termID] = streamAddressOffset + intPool.intOffset;

    //在bytePool中创建 streamCount 个level0 的 slice
    // 总是预分配两块大小都为5个字节的分片，
    //两块stream、第一块 存放term的文档号、词频信息，
    // 第二块存放term的位置、payload、offset信息。

    for (int i = 0; i < streamCount; i++) {
      // initialize each stream with a slice we start with ByteBlockPool.FIRST_LEVEL_SIZE)
      // and grow as we need more space. see ByteBlockPool.LEVEL_SIZE_ARRAY
      final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
      // intPool中的buffer记录的是每个stream下一个要写入的位置
      termStreamAddressBuffer[streamAddressOffset + i] = upto + bytePool.byteOffset;
    }
    // 记录stream在bytePool中的起始位置，这个信息是用来读取时候使用的
    postingsArray.byteStarts[termID] = termStreamAddressBuffer[streamAddressOffset];
    //处理倒排信息
    // 第一次出现的term的处理逻辑，在TermsHashPerField是个抽象方法。
    newTerm(termID, docID);
  }

  private boolean assertDocId(int docId) {
    assert docId >= lastDocID : "docID must be >= " + lastDocID + " but was: " + docId;
    lastDocID = docId;
    return true;
  }

  /**
   * Called once per inverted token. This is the primary entry point (for first TermsHash); postings
   * use this API.
   *
   *termBytes是term，docID是term所在的文档id。
   */
  void add(BytesRef termBytes, final int docID) throws IOException {
    assert assertDocId(docID);
    // We are first in the chain so we must "intern" the
    // term text into textStart address
    // Get the text & hash of this term.
    // bytesHash第一次遇到的term会返回大于等于0的termID
    int termID = bytesHash.add(termBytes);
    // System.out.println("add term=" + termBytesRef.utf8ToString() + " doc=" + docState.docID + "
    // termID=" + termID);
    if (termID >= 0) { // New posting
      // Init stream slices
      //如果term是第一次出现，则需要走TermsHashPerField#initStreamSlices初始化相关stream，
      // 然后走FreqProxTermsWriterPerField#newTerm处理倒排信息
      // term第一次出现，则初始化各个stream的第一个slice
      initStreamSlices(termID, docID);
    } else {
      //如果term之前出现过，则需要走TermsHashPerField#positionStreamSlice定位到term的写入位置，
      // 然后走FreqProxTermsWriterPerField#addTerm处理倒排信息。
      termID = positionStreamSlice(termID, docID);
    }
    if (doNextCall) {
      nextPerField.add(postingsArray.textStarts[termID], docID);
    }
  }

  private int positionStreamSlice(int termID, final int docID) throws IOException {
    //term还原为 (-term) - 1
    termID = (-termID) - 1;
    // termID在intPool的buffer中的起始位置
    int intStart = postingsArray.addressOffset[termID];
    // 根据 intStart 获取termID在intPool的buffer和offset
    termStreamAddressBuffer = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
    streamAddressOffset = intStart & IntBlockPool.INT_BLOCK_MASK;
    // 抽象方法，具体新增已有term的信息逻辑在子类实现
    addTerm(termID, docID);
    return termID;
  }

  final void writeByte(int stream, byte b) {
    // streamAddress是stream在intPool中的位置
    int streamAddress = streamAddressOffset + stream;
    // 从intPool中读取stream下一个要写入的位置
    int upto = termStreamAddressBuffer[streamAddress];
    byte[] bytes = bytePool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
    assert bytes != null;
    int offset = upto & ByteBlockPool.BYTE_BLOCK_MASK;
    // 如果碰到了slice的哨兵
    if (bytes[offset] != 0) {
      // End of slice; allocate a new one
      // 创建下一个slice，并返回写入的地址
      offset = bytePool.allocSlice(bytes, offset);
      bytes = bytePool.buffer;
      termStreamAddressBuffer[streamAddress] = offset + bytePool.byteOffset;
    }
    // 在slice写入数据
    bytes[offset] = b;
    (termStreamAddressBuffer[streamAddress])++;
  }

  final void writeBytes(int stream, byte[] b, int offset, int len) {
    final int end = offset + len;
    int streamAddress = streamAddressOffset + stream;
    int upto = termStreamAddressBuffer[streamAddress];
    byte[] slice = bytePool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
    assert slice != null;
    int sliceOffset = upto & ByteBlockPool.BYTE_BLOCK_MASK;

    while (slice[sliceOffset] == 0 && offset < end) {
      slice[sliceOffset++] = b[offset++];
      (termStreamAddressBuffer[streamAddress])++;
    }

    while (offset < end) {
      int offsetAndLength = bytePool.allocKnownSizeSlice(slice, sliceOffset);
      sliceOffset = offsetAndLength >> 8;
      int sliceLength = offsetAndLength & 0xff;
      slice = bytePool.buffer;
      int writeLength = Math.min(sliceLength - 1, end - offset);
      System.arraycopy(b, offset, slice, sliceOffset, writeLength);
      sliceOffset += writeLength;
      offset += writeLength;
      termStreamAddressBuffer[streamAddress] = sliceOffset + bytePool.byteOffset;
    }
  }

  final void writeVInt(int stream, int i) {
    assert stream < streamCount;
    while ((i & ~0x7F) != 0) {
      writeByte(stream, (byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte(stream, (byte) i);
  }

  final TermsHashPerField getNextPerField() {
    return nextPerField;
  }

  final String getFieldName() {
    return fieldName;
  }

  private static final class PostingsBytesStartArray extends BytesStartArray {

    private final TermsHashPerField perField;
    private final Counter bytesUsed;

    private PostingsBytesStartArray(TermsHashPerField perField, Counter bytesUsed) {
      this.perField = perField;
      this.bytesUsed = bytesUsed;
    }

    @Override
    public int[] init() {
      if (perField.postingsArray == null) {
        perField.postingsArray = perField.createPostingsArray(2);
        perField.newPostingsArray();
        bytesUsed.addAndGet(perField.postingsArray.size * perField.postingsArray.bytesPerPosting());
      }
      return perField.postingsArray.textStarts;
    }

    @Override
    public int[] grow() {
      ParallelPostingsArray postingsArray = perField.postingsArray;
      final int oldSize = perField.postingsArray.size;
      postingsArray = perField.postingsArray = postingsArray.grow();
      perField.newPostingsArray();
      bytesUsed.addAndGet((postingsArray.bytesPerPosting() * (postingsArray.size - oldSize)));
      return postingsArray.textStarts;
    }

    @Override
    public int[] clear() {
      if (perField.postingsArray != null) {
        bytesUsed.addAndGet(
            -(perField.postingsArray.size * perField.postingsArray.bytesPerPosting()));
        perField.postingsArray = null;
        perField.newPostingsArray();
      }
      return null;
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }

  @Override
  public final int compareTo(TermsHashPerField other) {
    return fieldName.compareTo(other.fieldName);
  }

  /** Finish adding all instances of this field to the current document. */
  void finish() throws IOException {
    if (nextPerField != null) {
      nextPerField.finish();
    }
  }

  final int getNumTerms() {
    return bytesHash.size();
  }

  /**
   * Start adding a new field instance; first is true if this is the first time this field name was
   * seen in the document.
   */
  boolean start(IndexableField field, boolean first) {
    if (nextPerField != null) {
      doNextCall = nextPerField.start(field, first);
    }
    return true;
  }

  /** Called when a term is seen for the first time. */
  abstract void newTerm(int termID, final int docID) throws IOException;

  /** Called when a previously seen term is seen again. */
  abstract void addTerm(int termID, final int docID) throws IOException;

  /** Called when the postings array is initialized or resized. */
  abstract void newPostingsArray();

  /** Creates a new postings array of the specified size. */
  abstract ParallelPostingsArray createPostingsArray(int size);
}
