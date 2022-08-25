package com.ysu.fjx.bean;

public class WordSource {
    //定义输入单词的私有属性
    private String word;
    private Long count;
    private Long timestamp;
    private String ignore;

    public WordSource() {

    }

    public WordSource(String word, Long count, Long timestamp, String ignore) {
        this.word = word;
        this.count = count;
        this.timestamp = timestamp;
        this.ignore = ignore;
    }

    @Override
    public String toString() {
        return "WordSource{" +
                "word='" + word + '\'' +
                ", count=" + count +
                ", timestamp=" + timestamp +
                ", ignore='" + ignore + '\'' +
                '}';
    }

    public String getIgnore() {
        return ignore;
    }

    public void setIgnore(String ignore) {
        this.ignore = ignore;
    }

    public WordSource(String word, Long count, Long timestamp) {
        this.word = word;
        this.count = count;
        this.timestamp = timestamp;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

}
