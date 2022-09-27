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
package org.apache.lucene.codecs;

import java.io.IOException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * Extension of {@link PostingsWriterBase}, adding a push API for writing each element of the
 * postings. This API is somewhat analogous to an XML SAX API, while {@link PostingsWriterBase} is
 * more like an XML DOM API.
 *
 * @see PostingsReaderBase
 * @lucene.experimental
 *
 * PushPostingsWriterBase是实现了PostingsWriterBase接口的抽象类，
 * 其中最最重要的是实现了一个构建的调度方法：writeTerm。
 */
// TODO: find a better name; this defines the API that the
// terms dict impls use to talk to a postings impl.
// TermsDict + PostingsReader/WriterBase == PostingsConsumer/Producer
public abstract class PushPostingsWriterBase extends PostingsWriterBase {

  // Reused in writeTerm
  // 获取倒排信息，这个迭代器封装了内存中的倒排数据
  private PostingsEnum postingsEnum;
  // 判断需要处理的倒排数据有哪些？docID，freq，position，offset，payload
  private int enumFlags;

  /** {@link FieldInfo} of current field being written. */
  // 当前正在处理的字段
  protected FieldInfo fieldInfo;

  /** {@link IndexOptions} of current field being written */
  // 索引选项
  protected IndexOptions indexOptions;

  /** True if the current field writes freqs. */
  // 需要把频率信息构建进索引文件
  protected boolean writeFreqs;

  /** True if the current field writes positions. */
  // 需要把position信息构建进索引文件
  protected boolean writePositions;

  /** True if the current field writes payloads. */
  // 需要把payload信息构建进索引文件
  protected boolean writePayloads;

  /** True if the current field writes offsets. */
  // 需要把offset信息构建进索引文件
  protected boolean writeOffsets;

  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected PushPostingsWriterBase() {}

  /** Return a newly created empty TermState */
  // 返回term的元信息，比如在doc，pay，pos文件中的起始位置等，这些元信息会存储在term字典的索引文件中，
  // 这样就能通过term字典查找term相关倒排信息
  public abstract BlockTermState newTermState() throws IOException;

  /**
   * Start a new term. Note that a matching call to {@link #finishTerm(BlockTermState)} is done,
   * only if the term has at least one document.
   */
  // 开始处理某个term
  public abstract void startTerm(NumericDocValues norms) throws IOException;

  /**
   * Finishes the current term. The provided {@link BlockTermState} contains the term's summary
   * statistics, and will holds metadata from PBF when returned
   */
  // 结束处理某个term
  public abstract void finishTerm(BlockTermState state) throws IOException;

  /**
   * Sets the current field for writing, and returns the fixed length of long[] metadata (which is
   * fixed per field), called when the writing switches to another field.
   */
  @Override
  // 根据fieldInfo设置当前处理的字段的一些信息
  public void setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    indexOptions = fieldInfo.getIndexOptions();

    writeFreqs = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    writePositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    writeOffsets =
        indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    writePayloads = fieldInfo.hasPayloads();

    if (writeFreqs == false) {
      enumFlags = 0;
    } else if (writePositions == false) {
      enumFlags = PostingsEnum.FREQS;
    } else if (writeOffsets == false) {
      if (writePayloads) {
        enumFlags = PostingsEnum.PAYLOADS;
      } else {
        enumFlags = PostingsEnum.POSITIONS;
      }
    } else {
      if (writePayloads) {
        enumFlags = PostingsEnum.PAYLOADS | PostingsEnum.OFFSETS;
      } else {
        enumFlags = PostingsEnum.OFFSETS;
      }
    }
  }

  @Override
  // 模板模式实现的调度框架，一些具体操作由子类实现
  public final BlockTermState writeTerm(
      BytesRef term, TermsEnum termsEnum, FixedBitSet docsSeen, NormsProducer norms)
      throws IOException {
    NumericDocValues normValues;
    if (fieldInfo.hasNorms() == false) {
      normValues = null;
    } else {
      normValues = norms.getNorms(fieldInfo);
    }
    // 开始处理term了
    startTerm(normValues);
    // 获取倒排信息的迭代器
    postingsEnum = termsEnum.postings(postingsEnum, enumFlags);
    assert postingsEnum != null;
    // 记录term出现的文档总数，一个文档贡献1次
    int docFreq = 0;
    // term在所有文档中出现的总频率
    long totalTermFreq = 0;
    while (true) {
      int docID = postingsEnum.nextDoc();
      if (docID == PostingsEnum.NO_MORE_DOCS) {
        break;
      }
      docFreq++;
      docsSeen.set(docID);
      int freq;
      if (writeFreqs) {
        freq = postingsEnum.freq();
        totalTermFreq += freq;
      } else {
        freq = -1;
      }
      // 开始处理文档
      startDoc(docID, freq);

      if (writePositions) {
        for (int i = 0; i < freq; i++) {// 处理所有的位置信息
          int pos = postingsEnum.nextPosition();
          BytesRef payload = writePayloads ? postingsEnum.getPayload() : null;
          int startOffset;
          int endOffset;
          if (writeOffsets) {
            startOffset = postingsEnum.startOffset();
            endOffset = postingsEnum.endOffset();
          } else {
            startOffset = -1;
            endOffset = -1;
          }
          addPosition(pos, payload, startOffset, endOffset);
        }
      }
      // 结束一个文档的处理
      finishDoc();
    }

    if (docFreq == 0) {// 如果term没有出现过
      return null;
    } else {// 把一些统计及在文件中的位置作为元信息封装在state中，最终持久化到term字典索引文件中
      BlockTermState state = newTermState();
      state.docFreq = docFreq;
      state.totalTermFreq = writeFreqs ? totalTermFreq : -1;
      finishTerm(state);
      return state;
    }
  }

  /**
   * Adds a new doc in this term. <code>freq</code> will be -1 when term frequencies are omitted for
   * the field.
   */
  // 开始处理term所在的某个doc
  public abstract void startDoc(int docID, int freq) throws IOException;

  /**
   * Add a new position and payload, and start/end offset. A null payload means no payload; a
   * non-null payload with zero length also means no payload. Caller may reuse the {@link BytesRef}
   * for the payload between calls (method must fully consume the payload). <code>startOffset</code>
   * and <code>endOffset</code> will be -1 when offsets are not indexed.
   */
  // 添加term所在doc中的位置信息
  public abstract void addPosition(int position, BytesRef payload, int startOffset, int endOffset)
      throws IOException;

  /** Called when we are done adding positions and payloads for each doc. */
  // 结束处理term所在的某个doc
  public abstract void finishDoc() throws IOException;
}
