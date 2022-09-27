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
package org.apache.lucene.codecs.lucene90;

import static org.apache.lucene.codecs.lucene90.ForUtil.BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.DOC_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.MAX_SKIP_LEVELS;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.PAY_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.POS_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.TERMS_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.VERSION_CURRENT;

import java.io.IOException;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Concrete class that writes docId(maybe frq,pos,offset,payloads) list with postings format.
 *
 * <p>Postings list for each term will be stored separately.
 *
 * @see Lucene90SkipWriter for details about skipping setting and postings layout.
 * @lucene.experimental
 *
 * PushPostingsWriterBase调度方法中很多抽象方法都在Lucene90PostingsWriter中实现，
 * Lucene90PostingsWriter是执行构建的底层类，会生成doc，pos和pay后缀的索引文件。
 *
 * docID，freq，position，offset都是按block存储的，一个block是128个值，为什么要区分block呢？
 * 因为Lucene中针对小正数使用批量的压缩算法处理，如果不区分block，则如果有一个值特别大
 * ，就会导致压缩效果骤降，而分block处理，就把异常值的影响只落在它所在的block中。
 *
 * 对于同一个term而言，docID是递增的，并且在同一篇文档中position和startOffset也是递增的。
 *
 * 为了加速doc的查找，lucene使用了跳表来加速查询。每128个doc，
 * 也就是一个 block生成一个skip节点，一个skip节点的docID就是这个block中最大的docID。
 *
 */
public final class Lucene90PostingsWriter extends PushPostingsWriterBase {
  // doc索引文件输出流
  IndexOutput docOut;
  // pos索引文件输出流
  IndexOutput posOut;
  // pay索引文件输出流
  IndexOutput payOut;

  static final IntBlockTermState emptyState = new IntBlockTermState();
  IntBlockTermState lastState;

  // Holds starting file pointers for current term:
  // 当前处理的term在各个索引文件中的起始位置
  private long docStartFP;
  private long posStartFP;
  private long payStartFP;
  // docID差值缓存，最终持久化是进行批量压缩编码的
  final long[] docDeltaBuffer;
  // 频率缓存，最终持久化是进行批量压缩编码的
  final long[] freqBuffer;
  // docDeltaBuffer和freqBuffer中下一个可以写入的位置
  private int docBufferUpto;
  // position差值缓存，最终持久化是进行批量压缩编码的
  final long[] posDeltaBuffer;
  // payload长度缓存， 最终持久化是进行批量压缩编码的
  final long[] payloadLengthBuffer;
  // startOffset差值缓存，最终持久化是进行批量压缩编码的
  final long[] offsetStartDeltaBuffer;
  // term长度缓存，最终持久化是进行批量压缩编码的
  final long[] offsetLengthBuffer;
  // posDeltaBuffer，payloadLengthBuffer，offsetStartDeltaBuffer和offsetLengthBuffer下一个可以写入的位置
  private int posBufferUpto;
  // payload数据缓存
  private byte[] payloadBytes;
  // payloadBytes下一个可以写入的位置
  private int payloadByteUpto;
  // 上一个block中的最后一个docID
  private int lastBlockDocID;
  // 上一个block在pos中的结束位置
  private long lastBlockPosFP;
  // 上一个block在pay中的结束位置
  private long lastBlockPayFP;
  // 上一个block结束时posBufferUpto的值
  private int lastBlockPosBufferUpto;
  // 上一个block结束时payloadByteUpto的值
  private int lastBlockPayloadByteUpto;
  // 上一个处理的docID
  private int lastDocID;
  // 上一个处理的position
  private int lastPosition;
  // 上一个处理的startOffset
  private int lastStartOffset;
  // 到目前位置处理的文档总数
  private int docCount;
  // 上面一堆long数组缓存的压缩编码器
  private final PForUtil pforUtil;
  // term中所有block构建的跳表，可以加速查找term中某个文档在索引文件中的位置
  private final Lucene90SkipWriter skipWriter;
  // 字段是否有标准化
  private boolean fieldHasNorms;
  // 获取标准化数据
  private NumericDocValues norms;
  // norm和freq是参与相关性打分的，可以根据norm和freq判断哪些文档是比较有竞争力的
  private final CompetitiveImpactAccumulator competitiveFreqNormAccumulator =
      new CompetitiveImpactAccumulator();

  /** Creates a postings writer */
  public Lucene90PostingsWriter(SegmentWriteState state) throws IOException {

    String docFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene90PostingsFormat.DOC_EXTENSION);
    docOut = state.directory.createOutput(docFileName, state.context);
    IndexOutput posOut = null;
    IndexOutput payOut = null;
    boolean success = false;
    try {
      CodecUtil.writeIndexHeader(
          docOut, DOC_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      pforUtil = new PForUtil(new ForUtil());
      if (state.fieldInfos.hasProx()) {
        posDeltaBuffer = new long[BLOCK_SIZE];
        String posFileName =
            IndexFileNames.segmentFileName(
                state.segmentInfo.name, state.segmentSuffix, Lucene90PostingsFormat.POS_EXTENSION);
        posOut = state.directory.createOutput(posFileName, state.context);
        CodecUtil.writeIndexHeader(
            posOut, POS_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);

        if (state.fieldInfos.hasPayloads()) {
          payloadBytes = new byte[128];
          payloadLengthBuffer = new long[BLOCK_SIZE];
        } else {
          payloadBytes = null;
          payloadLengthBuffer = null;
        }

        if (state.fieldInfos.hasOffsets()) {
          offsetStartDeltaBuffer = new long[BLOCK_SIZE];
          offsetLengthBuffer = new long[BLOCK_SIZE];
        } else {
          offsetStartDeltaBuffer = null;
          offsetLengthBuffer = null;
        }

        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          String payFileName =
              IndexFileNames.segmentFileName(
                  state.segmentInfo.name,
                  state.segmentSuffix,
                  Lucene90PostingsFormat.PAY_EXTENSION);
          payOut = state.directory.createOutput(payFileName, state.context);
          CodecUtil.writeIndexHeader(
              payOut, PAY_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
        }
      } else {
        posDeltaBuffer = null;
        payloadLengthBuffer = null;
        offsetStartDeltaBuffer = null;
        offsetLengthBuffer = null;
        payloadBytes = null;
      }
      this.payOut = payOut;
      this.posOut = posOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
    }

    docDeltaBuffer = new long[BLOCK_SIZE];
    freqBuffer = new long[BLOCK_SIZE];

    // TODO: should we try skipping every 2/4 blocks...?
    skipWriter =
        new Lucene90SkipWriter(
            MAX_SKIP_LEVELS, BLOCK_SIZE, state.segmentInfo.maxDoc(), docOut, posOut, payOut);
  }

  @Override
  public IntBlockTermState newTermState() {
    return new IntBlockTermState();
  }

  @Override
  public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
    CodecUtil.writeIndexHeader(
        termsOut, TERMS_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
    termsOut.writeVInt(BLOCK_SIZE);
  }

  @Override
  public void setField(FieldInfo fieldInfo) {
    super.setField(fieldInfo);
    skipWriter.setField(writePositions, writeOffsets, writePayloads);
    lastState = emptyState;
    fieldHasNorms = fieldInfo.hasNorms();
  }

  @Override
  public void startTerm(NumericDocValues norms) {
    // 当前term在doc索引文件中的的起始位置
    docStartFP = docOut.getFilePointer();
    if (writePositions) {
      // 当前term在pos索引文件中的起始位置
      posStartFP = posOut.getFilePointer();
      if (writePayloads || writeOffsets) {
        // 当前term在pay索引文件中的起始位置
        payStartFP = payOut.getFilePointer();
      }
    }
    lastDocID = 0;
    lastBlockDocID = -1;
    skipWriter.resetSkip();
    this.norms = norms;
    competitiveFreqNormAccumulator.clear();
  }

  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    // Have collected a block of docs, and get a new doc.
    // Should write skip data as well as postings list for
    // current block.
    // 上一个文档已经处理完了，并且刚好完成了128个文档，需要按block进行处理。
    // 注意，这里if判断是生成跳表的，暂存在缓存中，走到if里面的逻辑，说明前128个文档的相关倒排数据已经持久化完成了
    if (lastBlockDocID != -1 && docBufferUpto == 0) {
      skipWriter.bufferSkip(
          lastBlockDocID,
          competitiveFreqNormAccumulator,
          docCount,
          lastBlockPosFP,
          lastBlockPayFP,
          lastBlockPosBufferUpto,
          lastBlockPayloadByteUpto);
      competitiveFreqNormAccumulator.clear();
    }
    // docID是差值存储
    final int docDelta = docID - lastDocID;

    if (docID < 0 || (docCount > 0 && docDelta <= 0)) {
      throw new CorruptIndexException(
          "docs out of order (" + docID + " <= " + lastDocID + " )", docOut);
    }
    // 记录docID的差值
    docDeltaBuffer[docBufferUpto] = docDelta;
    if (writeFreqs) {
      // 记录频率
      freqBuffer[docBufferUpto] = termDocFreq;
    }

    docBufferUpto++;
    docCount++;

    if (docBufferUpto == BLOCK_SIZE) {
      // 如果已经暂存了128个文档ID和频率，则进行压缩编码持久化
      pforUtil.encode(docDeltaBuffer, docOut);
      if (writeFreqs) {
        pforUtil.encode(freqBuffer, docOut);
      }
      // NOTE: don't set docBufferUpto back to 0 here;
      // finishDoc will do so (because it needs to see that
      // the block was filled so it can save skip data)
      // 注意：docBufferUpto没有在这里被重置为0，是放在了finishDoc()中重置的
    }

    lastDocID = docID;
    lastPosition = 0;
    lastStartOffset = 0;
    // 下面处理标准化数据
    long norm;
    if (fieldHasNorms) {
      boolean found = norms.advanceExact(docID);
      if (found == false) {
        // This can happen if indexing hits a problem after adding a doc to the
        // postings but before buffering the norm. Such documents are written
        // deleted and will go away on the first merge.
        norm = 1L;
      } else {
        norm = norms.longValue();
        assert norm != 0 : docID;
      }
    } else {
      norm = 1L;
    }

    competitiveFreqNormAccumulator.add(writeFreqs ? termDocFreq : 1, norm);
  }

  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset)
      throws IOException {
    if (position > IndexWriter.MAX_POSITION) {
      throw new CorruptIndexException(
          "position="
              + position
              + " is too large (> IndexWriter.MAX_POSITION="
              + IndexWriter.MAX_POSITION
              + ")",
          docOut);
    }
    if (position < 0) {
      throw new CorruptIndexException("position=" + position + " is < 0", docOut);
    }
    // position也是差值存储
    posDeltaBuffer[posBufferUpto] = position - lastPosition;
    // 如果需要存储payload数据
    if (writePayloads) {
      // 当前位置没有payload数据
      if (payload == null || payload.length == 0) {
        // no payload
        payloadLengthBuffer[posBufferUpto] = 0;
      } else {
        payloadLengthBuffer[posBufferUpto] = payload.length;
        if (payloadByteUpto + payload.length > payloadBytes.length) {
          payloadBytes = ArrayUtil.grow(payloadBytes, payloadByteUpto + payload.length);
        }
        // 存储payload数据
        System.arraycopy(
            payload.bytes, payload.offset, payloadBytes, payloadByteUpto, payload.length);
        payloadByteUpto += payload.length;
      }
    }

    if (writeOffsets) {// 需要存储offset
      assert startOffset >= lastStartOffset;
      assert endOffset >= startOffset;
      offsetStartDeltaBuffer[posBufferUpto] = startOffset - lastStartOffset;
      offsetLengthBuffer[posBufferUpto] = endOffset - startOffset;
      lastStartOffset = startOffset;
    }

    posBufferUpto++;
    lastPosition = position;
    // 如果一个block满了
    if (posBufferUpto == BLOCK_SIZE) {
      // 压缩编码position差值信息，写入pos索引文件
      pforUtil.encode(posDeltaBuffer, posOut);

      if (writePayloads) {
        // payload长度信息写入pay索引文件
        pforUtil.encode(payloadLengthBuffer, payOut);
        // 写入payloadByteUpto
        payOut.writeVInt(payloadByteUpto);
        // 写入payload数据
        payOut.writeBytes(payloadBytes, 0, payloadByteUpto);
        payloadByteUpto = 0;
      }
      if (writeOffsets) {
        // startOffset压缩编码写入pay索引文件
        pforUtil.encode(offsetStartDeltaBuffer, payOut);
        // term长度压缩编码写入pay索引文件
        pforUtil.encode(offsetLengthBuffer, payOut);
      }
      posBufferUpto = 0;
    }
  }

  @Override
  public void finishDoc() throws IOException {
    // Since we don't know df for current term, we had to buffer
    // those skip data for each block, and when a new doc comes,
    // write them to skip file.
    // 这里可以看到是每 BLOCK_SIZE = 128 个文档为一个block
    if (docBufferUpto == BLOCK_SIZE) {
      lastBlockDocID = lastDocID;// 记录上一个block的docID
      if (posOut != null) {
        if (payOut != null) {
          lastBlockPayFP = payOut.getFilePointer();// 记录上一个block在pay文件的位置
        }
        lastBlockPosFP = posOut.getFilePointer();// 记录上一个block在pos文件的位置
        lastBlockPosBufferUpto = posBufferUpto;
        lastBlockPayloadByteUpto = payloadByteUpto;
      }
      docBufferUpto = 0;// 重置docID差值缓存的可写入位置
    }
  }

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(BlockTermState _state) throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    assert state.docFreq > 0;

    // TODO: wasteful we are counting this (counting # docs
    // for this term) in two places?
    assert state.docFreq == docCount : state.docFreq + " vs " + docCount;

    // docFreq == 1, don't write the single docid/freq to a separate file along with a pointer to
    // it.
    final int singletonDocID;
    // 如果在最后一个block中term只出现在一个doc中
    if (state.docFreq == 1) {
      // pulse the singleton docid into the term dictionary, freq is implicitly totalTermFreq
      // 用singletonDocID记录唯一出现的docID
      singletonDocID = (int) docDeltaBuffer[0];
    } else {
      singletonDocID = -1;
      // vInt encode the remaining doc deltas and freqs:
      // 用vint编码最后不满足一个block部分的docID差值和频率
      for (int i = 0; i < docBufferUpto; i++) {
        final int docDelta = (int) docDeltaBuffer[i];
        final int freq = (int) freqBuffer[i];
        if (!writeFreqs) {
          docOut.writeVInt(docDelta);
        } else if (freq == 1) {
          docOut.writeVInt((docDelta << 1) | 1);
        } else {
          docOut.writeVInt(docDelta << 1);
          docOut.writeVInt(freq);
        }
      }
    }

    final long lastPosBlockOffset;

    if (writePositions) {
      // totalTermFreq is just total number of positions(or payloads, or offsets)
      // associated with current term.
      assert state.totalTermFreq != -1;
      // 超过一个block才需要记录lastPosBlockOffset
      if (state.totalTermFreq > BLOCK_SIZE) {
        // record file offset for last pos in last block
        lastPosBlockOffset = posOut.getFilePointer() - posStartFP;
      } else {
        lastPosBlockOffset = -1;
      }
      if (posBufferUpto > 0) {
        // TODO: should we send offsets/payloads to
        // .pay...?  seems wasteful (have to store extra
        // vLong for low (< BLOCK_SIZE) DF terms = vast vast
        // majority)

        // vInt encode the remaining positions/payloads/offsets:
        int lastPayloadLength = -1; // force first payload length to be written
        int lastOffsetLength = -1; // force first offset length to be written
        int payloadBytesReadUpto = 0;
        for (int i = 0; i < posBufferUpto; i++) {
          final int posDelta = (int) posDeltaBuffer[i];
          if (writePayloads) {
            final int payloadLength = (int) payloadLengthBuffer[i];
            if (payloadLength != lastPayloadLength) {
              lastPayloadLength = payloadLength;
              posOut.writeVInt((posDelta << 1) | 1);
              posOut.writeVInt(payloadLength);
            } else {
              posOut.writeVInt(posDelta << 1);
            }

            if (payloadLength != 0) {
              posOut.writeBytes(payloadBytes, payloadBytesReadUpto, payloadLength);
              payloadBytesReadUpto += payloadLength;
            }
          } else {
            posOut.writeVInt(posDelta);
          }
          // 以vint写入剩余不满足一个block的offset数据
          if (writeOffsets) {
            int delta = (int) offsetStartDeltaBuffer[i];
            int length = (int) offsetLengthBuffer[i];
            if (length == lastOffsetLength) {
              posOut.writeVInt(delta << 1);
            } else {
              posOut.writeVInt(delta << 1 | 1);
              posOut.writeVInt(length);
              lastOffsetLength = length;
            }
          }
        }

        if (writePayloads) {
          assert payloadBytesReadUpto == payloadByteUpto;
          payloadByteUpto = 0;
        }
      }
    } else {
      lastPosBlockOffset = -1;
    }

    long skipOffset;
    // 至少一个block说明肯定存在跳表，需要持久化跳表
    if (docCount > BLOCK_SIZE) {
      skipOffset = skipWriter.writeSkip(docOut) - docStartFP;
    } else {
      skipOffset = -1;
    }
    // state中的这些信息就是term的元信息，会存储在term字典中
    state.docStartFP = docStartFP;
    state.posStartFP = posStartFP;
    state.payStartFP = payStartFP;
    state.singletonDocID = singletonDocID;
    state.skipOffset = skipOffset;
    state.lastPosBlockOffset = lastPosBlockOffset;
    docBufferUpto = 0;
    posBufferUpto = 0;
    lastDocID = 0;
    docCount = 0;
  }

  @Override
  public void encodeTerm(
      DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute)
      throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    if (absolute) {
      lastState = emptyState;
      assert lastState.docStartFP == 0;
    }

    if (lastState.singletonDocID != -1
        && state.singletonDocID != -1
        && state.docStartFP == lastState.docStartFP) {
      // With runs of rare values such as ID fields, the increment of pointers in the docs file is
      // often 0.
      // Furthermore some ID schemes like auto-increment IDs or Flake IDs are monotonic, so we
      // encode the delta
      // between consecutive doc IDs to save space.
      final long delta = (long) state.singletonDocID - lastState.singletonDocID;
      out.writeVLong((BitUtil.zigZagEncode(delta) << 1) | 0x01);
    } else {
      out.writeVLong((state.docStartFP - lastState.docStartFP) << 1);
      if (state.singletonDocID != -1) {
        out.writeVInt(state.singletonDocID);
      }
    }

    if (writePositions) {
      out.writeVLong(state.posStartFP - lastState.posStartFP);
      if (writePayloads || writeOffsets) {
        out.writeVLong(state.payStartFP - lastState.payStartFP);
      }
    }
    if (writePositions) {
      if (state.lastPosBlockOffset != -1) {
        out.writeVLong(state.lastPosBlockOffset);
      }
    }
    if (state.skipOffset != -1) {
      out.writeVLong(state.skipOffset);
    }
    lastState = state;
  }

  @Override
  public void close() throws IOException {
    // TODO: add a finish() at least to PushBase? DV too...?
    boolean success = false;
    try {
      if (docOut != null) {
        CodecUtil.writeFooter(docOut);
      }
      if (posOut != null) {
        CodecUtil.writeFooter(posOut);
      }
      if (payOut != null) {
        CodecUtil.writeFooter(payOut);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(docOut, posOut, payOut);
      } else {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
      docOut = posOut = payOut = null;
    }
  }
}
