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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import org.apache.lucene.index.Impact;

/**
 * This class accumulates the (freq, norm) pairs that may produce competitive scores.
 */
public final class CompetitiveImpactAccumulator {

  // We speed up accumulation for common norm values with this array that maps
  // norm values in -128..127 to the maximum frequency observed for these norm
  // values
  // 下标是norm，值是freq，对于norm相同的，只保留较大的freq
  private final int[] maxFreqs;
  // This TreeSet stores competitive (freq,norm) pairs for norm values that fall
  // outside of -128..127. It is always empty with the default similarity, which
  // encodes norms as bytes.
  // 这个为了处理那些norm值超出-128~127情况，默认BM25 similarity始终是空的，因为BM25把norm映射成一个byte。
  private final TreeSet<Impact> otherFreqNormPairs;

  /** Sole constructor. */
  // 在构造方法中，主要是初始化maxFreqs数组，大小是一个byte的大小。另外，初始化一个TreeSet，
  // 用来存储norm值在-128~127的情况。
  public CompetitiveImpactAccumulator() {
    maxFreqs = new int[256];
    Comparator<Impact> comparator =
        new Comparator<Impact>() {
          @Override
          public int compare(Impact o1, Impact o2) {
            // greater freqs compare greater
            int cmp = Integer.compare(o1.freq, o2.freq);
            if (cmp == 0) {
              // greater norms compare lower
              cmp = Long.compareUnsigned(o2.norm, o1.norm);
            }
            return cmp;
          }
        };
    otherFreqNormPairs = new TreeSet<>(comparator);
  }

  /** Reset to the same state it was in after creation. */
  public void clear() {
    Arrays.fill(maxFreqs, 0);
    otherFreqNormPairs.clear();
    assert assertConsistent();
  }

  /**
   * 如果norm值在-128~127之间，则存储在maxFreq数组中，相同norm只保留freq大的。
   * 其他情况，会根据是否有竞争力存储在TreeSet中，同时也会把明确不具竞争力的删除。
   *
   * Accumulate a (freq,norm) pair, updating this structure if there is no equivalent or more
   * competitive entry already.
   */
  public void add(int freq, long norm) {
    // 如果norm的范围在 -128~127之间
    if (norm >= Byte.MIN_VALUE && norm <= Byte.MAX_VALUE) {
      // 处理norm负数的情况
      int index = Byte.toUnsignedInt((byte) norm);
      // 相同norm只保留较大的freq
      maxFreqs[index] = Math.max(maxFreqs[index], freq);
    } else {
      // 其他情况直接加入TreeSet
      add(new Impact(freq, norm), otherFreqNormPairs);
    }
    assert assertConsistent();
  }

  /** Merge {@code acc} into this. */
  public void addAll(CompetitiveImpactAccumulator acc) {
    int[] maxFreqs = this.maxFreqs;
    int[] otherMaxFreqs = acc.maxFreqs;
    for (int i = 0; i < maxFreqs.length; ++i) {
      maxFreqs[i] = Math.max(maxFreqs[i], otherMaxFreqs[i]);
    }

    for (Impact entry : acc.otherFreqNormPairs) {
      add(entry, otherFreqNormPairs);
    }

    assert assertConsistent();
  }

  /** Get the set of competitive freq and norm pairs, ordered by increasing freq and norm. */
  // 获取所有有竞争力的Impact
  public Collection<Impact> getCompetitiveFreqNormPairs() {
    List<Impact> impacts = new ArrayList<>();
    int maxFreqForLowerNorms = 0;
    for (int i = 0; i < maxFreqs.length; ++i) {
      int maxFreq = maxFreqs[i];
      if (maxFreq > maxFreqForLowerNorms) {// norm更小，freq更大，肯定是有竞争力的
        impacts.add(new Impact(maxFreq, (byte) i));
        maxFreqForLowerNorms = maxFreq;
      }
    }

    if (otherFreqNormPairs.isEmpty()) {
      // Common case: all norms are bytes
      return impacts;
    }

    TreeSet<Impact> freqNormPairs = new TreeSet<>(this.otherFreqNormPairs);
    for (Impact impact : impacts) {
      add(impact, freqNormPairs);
    }
    return Collections.unmodifiableSet(freqNormPairs);
  }

  /**
   *
   * @param newEntry
   * @param freqNormPairs
   */
  private void add(Impact newEntry, TreeSet<Impact> freqNormPairs) {
    // 获取大于等于newEntry的第一个元素
    Impact next = freqNormPairs.ceiling(newEntry);
    if (next == null) {
      // 如果没有比要新加入的大，则直接加入TreeSet
      // nothing is more competitive
      freqNormPairs.add(newEntry);
    } else if (Long.compareUnsigned(next.norm, newEntry.norm) <= 0) {
      // we already have this entry or more competitive entries in the tree
      // 走到这里说明next的freq大于等于newEntry，因为 freqNormPairs 的比较器首先比较的是 freq
      // 如果next的norm还比newEntry的小，则说明newEntry没有竞争力
      return;
    } else {
      // some entries have a greater freq but a less competitive norm, so we
      // don't know which one will trigger greater scores, still add to the tree
      // 走到这里说明无法比较得分的优劣，则把newEntry加入
      freqNormPairs.add(newEntry);
    }

    for (Iterator<Impact> it = freqNormPairs.headSet(newEntry, false).descendingIterator();
        it.hasNext(); ) {
      Impact entry = it.next();
      if (Long.compareUnsigned(entry.norm, newEntry.norm) >= 0) {
        // less competitive
        // 把 entry.freq <= newEntry.freq && entry.norm >= newEntry.norm 的去掉，这些是明确没有竞争力的
        it.remove();
      } else {
        // lesser freq but better norm, further entries are not comparable
        // 后面的都无法判断了，无法判断的情况就是 entry.freq <= newEntry.freq && entry.norm <= newEntry.norm
        break;
      }
    }
  }

  @Override
  public String toString() {
    return new ArrayList<>(getCompetitiveFreqNormPairs()).toString();
  }

  // Only called by assertions
  private boolean assertConsistent() {
    int previousFreq = 0;
    long previousNorm = 0;
    for (Impact impact : otherFreqNormPairs) {
      assert impact.norm < Byte.MIN_VALUE || impact.norm > Byte.MAX_VALUE;
      assert previousFreq < impact.freq;
      assert Long.compareUnsigned(previousNorm, impact.norm) < 0;
      previousFreq = impact.freq;
      previousNorm = impact.norm;
    }
    return true;
  }
}
