package org.apache.lucene.chengzheng;

import org.apache.lucene.util.MathUtil;

/**
 * @author chengzhengzheng
 * @date 2022/9/26
 */
public class TestSkipList {
    public static void main(String[] args) {
//        int log = MathUtil.log(256, 128);
//        System.out.println(log);
        System.out.println(log(128,8));
    }

    public static int log(long df,int skipMultiplier){
        int ret = 1;
        while (df % skipMultiplier == 0){
            df = df / skipMultiplier;
            ret++;
        }
        return ret;
    }


}
