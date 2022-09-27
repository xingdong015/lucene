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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.MultiLevelSkipListWriter;
import org.apache.lucene.index.Impact;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;

/**
 * Write skip lists with multiple levels, and support skip within block ints.
 *
 * <p>Assume that docFreq = 28, skipInterval = blockSize = 12
 *
 * <pre>
 *  |       block#0       | |      block#1        | |vInts|
 *  d d d d d d d d d d d d d d d d d d d d d d d d d d d d (posting list)
 *                          ^                       ^       (level 0 skip point)
 * </pre>
 *
 * <p>Note that skipWriter will ignore first document in block#0, since it is useless as a skip
 * point. Also, we'll never skip into the vInts block, only record skip data at the start its start
 * point(if it exist).
 *
 * <p>For each skip point, we will record: 1. docID in former position, i.e. for position 12, record
 * docID[11], etc. 2. its related file points(position, payload), 3. related numbers or
 * uptos(position, payload). 4. start offset.
 *
 * Lucene90SkipWriter是MultiLevelSkipListWriter的实现类，它具体作用是决定每个跳表节点存储的数据有哪些。
 */
final class Lucene90SkipWriter extends MultiLevelSkipListWriter {
  // 下标是level，值是指定level的前一个跳表节点的docID
  private int[] lastSkipDoc;
  // 下标是level，值是指定level的前一个block的doc索引文件的结束位置，其实就是当前block的开始的位置
  private long[] lastSkipDocPointer;
  // 下标是level，值是指定level的前一个block的pos索引文件的结束位置，其实就是当前block的开始的位置
  private long[] lastSkipPosPointer;
  // 下标是level，值是指定level的前一个block的pay索引文件的结束位置，其实就是当前block的开始的位置
  private long[] lastSkipPayPointer;
  private int[] lastPayloadByteUpto;

  private final IndexOutput docOut;
  private final IndexOutput posOut;
  private final IndexOutput payOut;
  // 当前block的docID
  private int curDoc;
  // 当前block在doc文件的结束位置
  private long curDocPointer;
  // 当前block在pos文件的结束位置
  private long curPosPointer;
  // 当前block在pay文件的结束位置
  private long curPayPointer;
  // 当前block在posBuffer中的结束位置
  private int curPosBufferUpto;
  // 当前block在payloadBuffer中的结束位置
  private int curPayloadByteUpto;
  private CompetitiveImpactAccumulator[] curCompetitiveFreqNorms;
  private boolean fieldHasPositions;
  private boolean fieldHasOffsets;
  private boolean fieldHasPayloads;

  public Lucene90SkipWriter(
      int maxSkipLevels,
      int blockSize,
      int docCount,
      IndexOutput docOut,
      IndexOutput posOut,
      IndexOutput payOut) {
    super(blockSize, 8, maxSkipLevels, docCount);
    this.docOut = docOut;
    this.posOut = posOut;
    this.payOut = payOut;

    lastSkipDoc = new int[maxSkipLevels];
    lastSkipDocPointer = new long[maxSkipLevels];
    if (posOut != null) {
      lastSkipPosPointer = new long[maxSkipLevels];
      if (payOut != null) {
        lastSkipPayPointer = new long[maxSkipLevels];
      }
      lastPayloadByteUpto = new int[maxSkipLevels];
    }
    curCompetitiveFreqNorms = new CompetitiveImpactAccumulator[maxSkipLevels];
    for (int i = 0; i < maxSkipLevels; ++i) {
      curCompetitiveFreqNorms[i] = new CompetitiveImpactAccumulator();
    }
  }

  public void setField(
      boolean fieldHasPositions, boolean fieldHasOffsets, boolean fieldHasPayloads) {
    this.fieldHasPositions = fieldHasPositions;
    this.fieldHasOffsets = fieldHasOffsets;
    this.fieldHasPayloads = fieldHasPayloads;
  }

  // tricky: we only skip data for blocks (terms with more than 128 docs), but re-init'ing the
  // skipper
  // is pretty slow for rare terms in large segments as we have to fill O(log #docs in segment) of
  // junk.
  // this is the vast majority of terms (worst case: ID field or similar).  so in resetSkip() we
  // save
  // away the previous pointers, and lazy-init only if we need to buffer skip data for the term.
  private boolean initialized;
  long lastDocFP;
  long lastPosFP;
  long lastPayFP;

  @Override
  public void resetSkip() {
    lastDocFP = docOut.getFilePointer();
    if (fieldHasPositions) {
      lastPosFP = posOut.getFilePointer();
      if (fieldHasOffsets || fieldHasPayloads) {
        lastPayFP = payOut.getFilePointer();
      }
    }
    if (initialized) {
      for (CompetitiveImpactAccumulator acc : curCompetitiveFreqNorms) {
        acc.clear();
      }
    }
    initialized = false;
  }

  private void initSkip() {
    if (!initialized) {
      super.resetSkip();
      Arrays.fill(lastSkipDoc, 0);
      Arrays.fill(lastSkipDocPointer, lastDocFP);
      if (fieldHasPositions) {
        Arrays.fill(lastSkipPosPointer, lastPosFP);
        if (fieldHasPayloads) {
          Arrays.fill(lastPayloadByteUpto, 0);
        }
        if (fieldHasOffsets || fieldHasPayloads) {
          Arrays.fill(lastSkipPayPointer, lastPayFP);
        }
      }
      // sets of competitive freq,norm pairs should be empty at this point
      assert Arrays.stream(curCompetitiveFreqNorms)
              .map(CompetitiveImpactAccumulator::getCompetitiveFreqNormPairs)
              .mapToInt(Collection::size)
              .sum()
          == 0;
      initialized = true;
    }
  }

  /** Sets the values for the current skip data. */
  public void bufferSkip(
      int doc,
      CompetitiveImpactAccumulator competitiveFreqNorms,
      int numDocs,
      long posFP,
      long payFP,
      int posBufferUpto,
      int payloadByteUpto)
      throws IOException {
    initSkip();
    this.curDoc = doc;
    this.curDocPointer = docOut.getFilePointer();
    this.curPosPointer = posFP;
    this.curPayPointer = payFP;
    this.curPosBufferUpto = posBufferUpto;
    this.curPayloadByteUpto = payloadByteUpto;
    this.curCompetitiveFreqNorms[0].addAll(competitiveFreqNorms);
    bufferSkip(numDocs);
  }

  private final ByteBuffersDataOutput freqNormOut = ByteBuffersDataOutput.newResettableInstance();

  @Override
  protected void writeSkipData(int level, DataOutput skipBuffer) throws IOException {
    // 当前block的docID和第level层的前一个block的docID的差值
    int delta = curDoc - lastSkipDoc[level];
    // 写入block的docID差值
    skipBuffer.writeVInt(delta);
    lastSkipDoc[level] = curDoc;
    // 写入block在doc索引文件中的大小
    skipBuffer.writeVLong(curDocPointer - lastSkipDocPointer[level]);
    lastSkipDocPointer[level] = curDocPointer;

    if (fieldHasPositions) {
      // 写入block在pos索引文件中的大小
      skipBuffer.writeVLong(curPosPointer - lastSkipPosPointer[level]);
      lastSkipPosPointer[level] = curPosPointer;
      // 因为position是按block存储的，但是有可能一个block是多个doc共享的，所以需要记录当前skip在block的截止位置
      //没看懂 ？？？
      skipBuffer.writeVInt(curPosBufferUpto);

      if (fieldHasPayloads) {
        skipBuffer.writeVInt(curPayloadByteUpto);
      }

      if (fieldHasOffsets || fieldHasPayloads) {
        // 写入block在pay索引文件中的大小
        skipBuffer.writeVLong(curPayPointer - lastSkipPayPointer[level]);
        lastSkipPayPointer[level] = curPayPointer;
      }
    }

    CompetitiveImpactAccumulator competitiveFreqNorms = curCompetitiveFreqNorms[level];
    assert competitiveFreqNorms.getCompetitiveFreqNormPairs().size() > 0;
    // 因为跳表中上层的节点在下层肯定是存在的，所以需要把impact先存到上一层
    if (level + 1 < numberOfSkipLevels) {
      curCompetitiveFreqNorms[level + 1].addAll(competitiveFreqNorms);
    }
    writeImpacts(competitiveFreqNorms, freqNormOut);
    skipBuffer.writeVInt(Math.toIntExact(freqNormOut.size()));
    freqNormOut.copyTo(skipBuffer);
    freqNormOut.reset();
    competitiveFreqNorms.clear();
  }

  static void writeImpacts(CompetitiveImpactAccumulator acc, DataOutput out) throws IOException {
    Collection<Impact> impacts = acc.getCompetitiveFreqNormPairs();
    Impact previous = new Impact(0, 0);
    for (Impact impact : impacts) {
      assert impact.freq > previous.freq;
      assert Long.compareUnsigned(impact.norm, previous.norm) > 0;
      // 存储的是差值-1
      int freqDelta = impact.freq - previous.freq - 1;
      long normDelta = impact.norm - previous.norm - 1;
      if (normDelta == 0) {
        // most of time, norm only increases by 1, so we can fold everything in a single byte
        out.writeVInt(freqDelta << 1);
      } else {
        out.writeVInt((freqDelta << 1) | 1);
        out.writeZLong(normDelta);
      }
      previous = impact;
    }
  }
}
