package org.apache.lucene.chengzheng;

import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;

import static org.apache.lucene.tests.util.LuceneTestCase.*;

/**
 * @author chengzhengzheng
 * @date 2022/9/1
 */
public class TestDemo {
    public static void main(String[] args) throws IOException, URISyntaxException {
        System.out.println(5 << 1);//1010

        // main directory
        Directory dir = new MMapDirectory(Path.of(new URI("/Users/chengzheng/self_project/luceneDir")));

        IndexWriter writer = new IndexWriter(dir,new IndexWriterConfig());
        FieldType type = new FieldType();
        type.setStored(true);
        type.setTokenized(true);
        type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        type.freeze();


        Document document = new Document();
        document.add(new Field("content","book book is", type));
        document.add(new Field("title","book", type));
        writer.addDocument(document);
    }


}
