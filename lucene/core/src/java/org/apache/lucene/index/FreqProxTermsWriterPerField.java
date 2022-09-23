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
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.util.BytesRef;

// TODO: break into separate freq and prox writers as
// codecs; make separate container (tii/tis/skip/*) that can
// be configured as any number of files 1..N
final class FreqProxTermsWriterPerField extends TermsHashPerField {

  private FreqProxPostingsArray freqProxPostingsArray;
  private final FieldInvertState fieldState;
  private final FieldInfo fieldInfo;

  final boolean hasFreq;
  final boolean hasProx;
  final boolean hasOffsets;
  PayloadAttribute payloadAttribute;
  OffsetAttribute offsetAttribute;
  TermFrequencyAttribute termFreqAtt;

  /** Set to true if any token had a payload in the current segment. */
  boolean sawPayloads;

  FreqProxTermsWriterPerField(
      FieldInvertState invertState,
      TermsHash termsHash,
      FieldInfo fieldInfo,
      TermsHashPerField nextPerField) {
    super(
        fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0
            ? 2
            : 1,
        termsHash.intPool,
        termsHash.bytePool,
        termsHash.termBytePool,
        termsHash.bytesUsed,
        nextPerField,
        fieldInfo.name,
        fieldInfo.getIndexOptions());
    this.fieldState = invertState;
    this.fieldInfo = fieldInfo;
    hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
  }

  @Override
  void finish() throws IOException {
    super.finish();
    if (sawPayloads) {
      fieldInfo.setStorePayloads();
    }
  }

  @Override
  boolean start(IndexableField f, boolean first) {
    super.start(f, first);
    termFreqAtt = fieldState.termFreqAttribute;
    payloadAttribute = fieldState.payloadAttribute;
    offsetAttribute = fieldState.offsetAttribute;
    return true;
  }

  void writeProx(int termID, int proxCode) {
    // 如果该term没有payLoad信息，则把proxCode << 1 追加到stream 1，注意没有payLoad最后一位肯定是0
    // 当不存在payload信息时，单独存储pos的差值左移一位，这样用最后一位0表示后面没有payload数据。
    // 如果存在payload数据，则先存储pos差值左移一位 | 1，这样用最后一位1表示后面有payload数据，
    // 然后再写payload长度，类型是Vint，最后写入payload的数据
    if (payloadAttribute == null) {
      writeVInt(1, proxCode << 1);
    } else {
      BytesRef payload = payloadAttribute.getPayload();
      // 如果该position的term存在payLoad信息
      if (payload != null && payload.length > 0) {
        // 注意有payLoad的时候，最后一位肯定是1
        writeVInt(1, (proxCode << 1) | 1);
        // payLoad的长度
        writeVInt(1, payload.length);
        // payLoad的内容
        writeBytes(1, payload.bytes, payload.offset, payload.length);
        sawPayloads = true;
      } else {
        // 如果该 position 的 term 不存在payLoad信息
        writeVInt(1, proxCode << 1);
      }
    }

    assert postingsArray == freqProxPostingsArray;
    // 更新term在当前处理文档中上一次出现的position
    freqProxPostingsArray.lastPositions[termID] = fieldState.position;
  }

  // 有时候，我们对一个Document添加了相同的Field，则在处理时是把这些Field的内容拼起来，
  // offsetAccum记录的就是当前field在拼接中的偏移量
  void writeOffsets(int termID, int offsetAccum) {
    // 记录的所有相同Field拼接之后的offset
    final int startOffset = offsetAccum + offsetAttribute.startOffset();
    final int endOffset = offsetAccum + offsetAttribute.endOffset();
    assert startOffset - freqProxPostingsArray.lastOffsets[termID] >= 0;
    // 差值存储 startOffset
    writeVInt(1, startOffset - freqProxPostingsArray.lastOffsets[termID]);
    // 这边记录的其实就是term的长度，这里感觉比较冗余，因为在记录term的时候已经记录的term的长度
    writeVInt(1, endOffset - startOffset);
    // 更新term在当前处理文档中的上一次出现的startOffset
    freqProxPostingsArray.lastOffsets[termID] = startOffset;
  }

  @Override
  void newTerm(final int termID, final int docID) {
    // First time we're seeing this term since the last
    // flush
    final FreqProxPostingsArray postings = freqProxPostingsArray;
    // 记录term上一次出现的文档
    postings.lastDocIDs[termID] = docID;
    //是否有频率信息
    if (!hasFreq) {
      assert postings.termFreqs == null;
      postings.lastDocCodes[termID] = docID;
      fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);
    } else {
      postings.lastDocCodes[termID] = docID << 1;
      postings.termFreqs[termID] = getTermFreq();
      //是否有pos信息
      if (hasProx) {
        writeProx(termID, fieldState.position);
        //是否有offset信息
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset);
        }
      } else {
        assert !hasOffsets;
      }
      fieldState.maxTermFrequency =
          Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
    }
    fieldState.uniqueTermCount++;
  }

  @Override
  void addTerm(final int termID, final int docID) {
    final FreqProxPostingsArray postings = freqProxPostingsArray;
    assert !hasFreq || postings.termFreqs[termID] > 0;
    //是否有频率信息
    if (!hasFreq) {
      assert postings.termFreqs == null;
      // 默认的实现TermFrequencyAttribute 是返回1，如果是自定义实现，则必须在索引选项中加入频率
      if (termFreqAtt.getTermFrequency() != 1) {
        throw new IllegalStateException(
            "field \""
                + getFieldName()
                + "\": must index term freq while using custom TermFrequencyAttribute");
      }
      //当前docId的term出现的上一个docId是否相同?
      if (docID != postings.lastDocIDs[termID]) {// 上一个文档处理结束
        // New document; now encode docCode for previous doc:
        assert docID > postings.lastDocIDs[termID];
        writeVInt(0, postings.lastDocCodes[termID]);
        postings.lastDocCodes[termID] = docID - postings.lastDocIDs[termID];
        postings.lastDocIDs[termID] = docID;
        fieldState.uniqueTermCount++;
      }
    } else if (docID != postings.lastDocIDs[termID]) {// 上一个文档处理结束
      assert docID > postings.lastDocIDs[termID]
          : "id: " + docID + " postings ID: " + postings.lastDocIDs[termID] + " termID: " + termID;
      // Term not yet seen in the current doc but previously
      // seen in other doc(s) since the last flush

      // Now that we know doc freq for previous doc,
      // write it & lastDocCode
      // 写入倒排表。
      // 对于同一个term来说，在某一篇文档中，只有所有该term都被处理结束才会写到倒排表中去，
      // 否则的话，term在当前文档中的词频frequencies无法正确统计。所以每次处理同一个term时，
      // 根据它目前所属的文档跟它上一次所属的文档来判断当前的操作是统计词频还是将它写入到倒排表。
      // 另外包含某个term的所有文档号是用差值存储，该数组用来计算差值。

      //基于压缩存储，如果一个term在一篇文档中的词频只有1，那么文档号跟词频的信息组合存储，否则文档号跟词频分开存储。
      if (1 == postings.termFreqs[termID]) {
        // 如果频率是1，则和左移一位的文档id一起存储
        writeVInt(0, postings.lastDocCodes[termID] | 1);
      } else {
        // 如果频率大于1，则先存文档id，注意左移一位的文档id
        // 如果频率大于1，则先存储docID差值左移一位，
        // 这样用最后一位0表示后面有单独的频率数据，然后再写频率，类型是Vint。
        writeVInt(0, postings.lastDocCodes[termID]);
        writeVInt(0, postings.termFreqs[termID]);
      }

      // Init freq for the current document
      // 记录在当前文档中出现的频率，默认实现是1
      postings.termFreqs[termID] = getTermFreq();
      // 更新最大的term的频率
      fieldState.maxTermFrequency =
          Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
      // 文档id编码也是差值存储
      postings.lastDocCodes[termID] = (docID - postings.lastDocIDs[termID]) << 1;
      postings.lastDocIDs[termID] = docID;

      // 是否有position信息
      // 基于压缩存储，倒排表中的位置(position)信息是一个差值，这个值指的是在同一篇文档中，
      // 当前term的位置和上一次出现的位置的差值。
      // 每次获得一个term的位置信息，就马上写入到倒排表中。
      // 注意的是，实际存储到倒排表时，跟存储文档号一样是一个组合值，
      // 不过这个编码值是用来描述当前位置的term是否有payload信息。
      if (hasProx) {
        writeProx(termID, fieldState.position);
        //是否有offset信息
        if (hasOffsets) {
          // 第一次出现的offset是0
          postings.lastOffsets[termID] = 0;
          writeOffsets(termID, fieldState.offset);
        }
      } else {
        assert !hasOffsets;
      }
      // 出现的term的个数+1
      fieldState.uniqueTermCount++;
    } else {
      // 如果不是在处理新的文档，则追加term的信息即可, 用来做词频统计。
      // 更新term的频率
      postings.termFreqs[termID] = Math.addExact(postings.termFreqs[termID], getTermFreq());
      // 更新最大的term的频率
      fieldState.maxTermFrequency =
          Math.max(fieldState.maxTermFrequency, postings.termFreqs[termID]);
      if (hasProx) {
        // position也是差值存储
        // 基于压缩存储，倒排表中的位置(position)信息是一个差值，这个值指的是在同一篇文档中，当前term的位置和上一次出现的位置的差值。
        // 每次获得一个term的位置信息，就马上写入到倒排表中。
        // 注意的是，实际存储到倒排表时，跟存储文档号一样是一个组合值，不过这个编码值是用来描述当前位置的term是否有payload信息。
        writeProx(termID, fieldState.position - postings.lastPositions[termID]);
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset);
        }
      }
    }
  }

  private int getTermFreq() {
    int freq = termFreqAtt.getTermFrequency();
    if (freq != 1) {
      if (hasProx) {
        throw new IllegalStateException(
            "field \""
                + getFieldName()
                + "\": cannot index positions while using custom TermFrequencyAttribute");
      }
    }

    return freq;
  }

  @Override
  public void newPostingsArray() {
    freqProxPostingsArray = (FreqProxPostingsArray) postingsArray;
  }

  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    boolean hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    boolean hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean hasOffsets =
        indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    return new FreqProxPostingsArray(size, hasFreq, hasProx, hasOffsets);
  }

  static final class FreqProxPostingsArray extends ParallelPostingsArray {
    public FreqProxPostingsArray(
        int size, boolean writeFreqs, boolean writeProx, boolean writeOffsets) {
      super(size);
      if (writeFreqs) {
        termFreqs = new int[size];
      }
      lastDocIDs = new int[size];
      lastDocCodes = new int[size];
      if (writeProx) {
        lastPositions = new int[size];
        if (writeOffsets) {
          lastOffsets = new int[size];
        }
      } else {
        assert !writeOffsets;
      }
      // System.out.println("PA init freqs=" + writeFreqs + " pos=" + writeProx + " offs=" +
      // writeOffsets);
    }
    // 下标是termID,值是termID对应的在当前处理文档中的频率
    int[] termFreqs; // # times this term occurs in the current doc
    // 下标是termID,值是上一个出现这个term的文档id
    int[] lastDocIDs; // Last docID where this term occurred
    // 下标是termID,值是上一个出现这个term的文档id的编码：docId << 1
    int[] lastDocCodes; // Code for prior doc
    // 下标是termID,值是term在当前文档中上一次出现的position
    int[] lastPositions; // Last position where this term occurred
    // 下标是termID,值是term在当前文档中上一次出现的startOffset（注意这里源码注释不对）
    int[] lastOffsets; // Last endOffset where this term occurred

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new FreqProxPostingsArray(
          size, termFreqs != null, lastPositions != null, lastOffsets != null);
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof FreqProxPostingsArray;
      FreqProxPostingsArray to = (FreqProxPostingsArray) toArray;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(lastDocIDs, 0, to.lastDocIDs, 0, numToCopy);
      System.arraycopy(lastDocCodes, 0, to.lastDocCodes, 0, numToCopy);
      if (lastPositions != null) {
        assert to.lastPositions != null;
        System.arraycopy(lastPositions, 0, to.lastPositions, 0, numToCopy);
      }
      if (lastOffsets != null) {
        assert to.lastOffsets != null;
        System.arraycopy(lastOffsets, 0, to.lastOffsets, 0, numToCopy);
      }
      if (termFreqs != null) {
        assert to.termFreqs != null;
        System.arraycopy(termFreqs, 0, to.termFreqs, 0, numToCopy);
      }
    }

    @Override
    int bytesPerPosting() {
      int bytes = ParallelPostingsArray.BYTES_PER_POSTING + 2 * Integer.BYTES;
      if (lastPositions != null) {
        bytes += Integer.BYTES;
      }
      if (lastOffsets != null) {
        bytes += Integer.BYTES;
      }
      if (termFreqs != null) {
        bytes += Integer.BYTES;
      }

      return bytes;
    }
  }
}
