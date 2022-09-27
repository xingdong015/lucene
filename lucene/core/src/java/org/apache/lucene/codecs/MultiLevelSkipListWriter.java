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
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.MathUtil;

/**
 * This abstract class writes skip lists with multiple levels.
 *
 * <pre>
 *
 * Example for skipInterval = 3:
 *                                                     c            (skip level 2)
 *                 c                 c                 c            (skip level 1)
 *     x     x     x     x     x     x     x     x     x     x      (skip level 0)
 * d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d  (posting list)
 *     3     6     9     12    15    18    21    24    27    30     (df)
 *
 * d - document
 * x - skip data
 * c - skip data with child pointer
 *
 * Skip level i contains every skipInterval-th entry from skip level i-1.
 * Therefore the number of entries on level i is: floor(df / ((skipInterval ^ (i + 1))).
 *
 * Each skip entry on a level {@code i>0} contains a pointer to the corresponding skip entry in list i-1.
 * This guarantees a logarithmic amount of skips to find the target document.
 *
 * While this class takes care of writing the different skip levels,
 * subclasses must define the actual format of the skip data.
 * </pre>
 *
 * lucene中的跳表是在构建倒排和查询的时候专用的，所以实现上比较定制化。
 *
 * @lucene.experimental
 */
public abstract class MultiLevelSkipListWriter {
  /** number of levels in this skip list */
  // 跳表的层数
  protected final int numberOfSkipLevels;

  /** the skip interval in the list with level = 0 */
  // 每处理skipInterval个文档生成一个第0层跳表的节点，其实就是每个跳表节点包含的文档数，在 lucene90SkipWrite 为 128
  // 该值描述了在level=0层，每处理skipInterval篇文档，就生成一个skipDatum，该值默认为128
  private final int skipInterval;

  /** skipInterval used for level &gt; 0 */
  // 从第0层开始，每层间隔skipMultiplier的节点就加入上一层,在 lucene90SkipWrite 为8
  // 该值描述了在所有层，每处理skipMultiplier个skipDatum，就在上一层生成一个新的skipDatum，该值默认为8
  private final int skipMultiplier;

  /** for every skip level a different buffer is used */
  // 存储跳表每一层的数据的缓存
  // 该数组中存放的元素为每一层的数据
  private ByteBuffersDataOutput[] skipBuffer;

  /** Creates a {@code MultiLevelSkipListWriter}. */
  // df就是整个跳表要容纳的文档总数，根据这个参数可以算出跳表最高层数
  // 根据当前已经处理的文档数量，预先计算出将待写入SkipDatum信息的层数，计算方式如下
  protected MultiLevelSkipListWriter(
      int skipInterval, int skipMultiplier, int maxSkipLevels, int df) {
    this.skipInterval = skipInterval;
    this.skipMultiplier = skipMultiplier;

    int numberOfSkipLevels;
    // calculate the maximum number of skip levels for this document frequency
    if (df <= skipInterval) {
      // 如果文档总数小于skipInterval，则只有一层
      numberOfSkipLevels = 1;
    } else {
      // df / skipInterval表示最底层有多少个节点
      // skipMultiplier 表示下一层间隔多少个节点生成上一层的节点
      numberOfSkipLevels = 1 + MathUtil.log(df / skipInterval, skipMultiplier);
    }

    // make sure it does not exceed maxSkipLevels
    // 不能超过最大层数限制
    if (numberOfSkipLevels > maxSkipLevels) {
      numberOfSkipLevels = maxSkipLevels;
    }
    this.numberOfSkipLevels = numberOfSkipLevels;
  }

  /**
   * Creates a {@code MultiLevelSkipListWriter}, where {@code skipInterval} and {@code
   * skipMultiplier} are the same.
   *
   */
  // df就是整个跳表要容纳的文档总数，根据这个参数可以算出跳表最高层数
  protected MultiLevelSkipListWriter(int skipInterval, int maxSkipLevels, int df) {
    this(skipInterval, skipInterval, maxSkipLevels, df);
  }

  /** Allocates internal skip buffers. */
  protected void init() {
    // 每一层一个buffer
    skipBuffer = new ByteBuffersDataOutput[numberOfSkipLevels];
    for (int i = 0; i < numberOfSkipLevels; i++) {
      skipBuffer[i] = ByteBuffersDataOutput.newResettableInstance();
    }
  }

  /** Creates new buffers or empties the existing ones */
  //如果跳表的缓存还未创建，则为跳表每一层创建缓存。如果缓存已经创建，则清空每个缓存。
  protected void resetSkip() {
    if (skipBuffer == null) {
      // 如果没有初始化过，则直接初始化
      init();
    } else {
      for (int i = 0; i < skipBuffer.length; i++) {
        skipBuffer[i].reset();
      }
    }
  }

  /**
   * Subclasses must implement the actual skip data encoding in this method.
   *
   * @param level the level skip data shall be writing for
   * @param skipBuffer the skip buffer to write to
   */
  protected abstract void writeSkipData(int level, DataOutput skipBuffer) throws IOException;

  /**
   * Writes the current skip data to the buffers. The current document frequency determines the max
   * level is skip data is to be written to.
   *
   * @param df the current document frequency
   * @throws IOException If an I/O error occurs
   *
   * 此方法是生成跳表的数据，把跳表数据暂存在缓存中。
   * df表示到目前为止的文档总数，根据df可以得到当前要生成的跳表节点最多可以到达第几层。
   * 从bufferSkip方法中，我们可以看到子类实现的writeSkipData决定了skip节点存储了哪些信息，
   * 在节点的最后面，存储了指向下一层节点的指针。
   */
  public void bufferSkip(int df) throws IOException {

    assert df % skipInterval == 0;
    int numLevels = 1;
    // 现在df表示最底层有多少个节点,这里的节点指得是 skipList 的节点。一个 skipList 节点包含多个doc集合。
    // 计算出在level=0层有多少个SkipDatum
    df /= skipInterval;

    // determine max level
    // 计算当前的skip节点可以到达第几层，上限是 numberOfSkipLevels 层
    while ((df % skipMultiplier) == 0 && numLevels < numberOfSkipLevels) {
      numLevels++;
      // 每skipMultiplier个SkipDatum就在上一层生成一个SkipDatum
      df /= skipMultiplier;
    }
    // 上层skip节点指向下层skip节点的指针，其实是相对于skip起始位置的相对位置
    long childPointer = 0;
    // 生成每一层的跳表数据
    for (int level = 0; level < numLevels; level++) {
      // 生成跳表数据写入  skipBuffer[level]，具体有子类实现每个跳表节点存储的数据
      writeSkipData(level, skipBuffer[level]);
      // 下一层节点的指针是目前level层的大小，也就是相对跳表起始位置
      long newChildPointer = skipBuffer[level].size();

      if (level != 0) {
        // store child pointers for all levels except the lowest
        // 除了最底层，其他层都记录下一层的起始位置
        writeChildPointer(childPointer, skipBuffer[level]);
      }

      // remember the childPointer for the next level
      childPointer = newChildPointer;
    }
  }

  /**
   * Writes the buffered skip lists to the given output.
   *
   * @param output the IndexOutput the skip lists shall be written to
   * @return the pointer the skip list starts
   *
   * 一个term一个跳表，当一个term在所有文档的倒排信息都处理完成之后，调用writeSkip持久化跳表
   * ，跳表数据持久化到doc索引文件中，因为跳表就是用来快速定位doc的位置的。
   *
   * 持久化就是把每一层的缓存数据持久化，从最高层开始处理。
   */
  public long writeSkip(IndexOutput output) throws IOException {
    long skipPointer = output.getFilePointer();
    // System.out.println("skipper.writeSkip fp=" + skipPointer);
    if (skipBuffer == null || skipBuffer.length == 0) return skipPointer;
    // 从最高层往下持久化
    for (int level = numberOfSkipLevels - 1; level > 0; level--) {
      long length = skipBuffer[level].size();
      // 除了第一层，其他层都是先写长度再写内容
      if (length > 0) {
        writeLevelLength(length, output);
        skipBuffer[level].copyTo(output);
      }
    }
    skipBuffer[0].copyTo(output);

    return skipPointer;
  }

  /**
   * Writes the length of a level to the given output.
   *
   * @param levelLength the length of a level
   * @param output the IndexOutput the length shall be written to
   */
  protected void writeLevelLength(long levelLength, IndexOutput output) throws IOException {
    output.writeVLong(levelLength);
  }

  /**
   * Writes the child pointer of a block to the given output.
   *
   * @param childPointer block of higher level point to the lower level
   * @param skipBuffer the skip buffer to write to
   */
  protected void writeChildPointer(long childPointer, DataOutput skipBuffer) throws IOException {
    skipBuffer.writeVLong(childPointer);
  }
}
