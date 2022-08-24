package com.ysu.fjx.bean;

public class ItemViewCount {
    //分别表示单词（以此为分组），单词所属的窗口Id以及数量
    private String word;
    private Long windowEnd;
    private Long count;

    public ItemViewCount() {
    }

    public ItemViewCount(String word, Long windowEnd, Long count) {
        this.word = word;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "word='" + word + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
