package edu.snu.gylee.adb.irpactice;

public class SearchResult {

  public int id;
  public String title;
  public double tfIdfScore;
  public double pageRankScore;

  public SearchResult(int id, String title) {
    this.id = id;
    this.title = title;
    this.tfIdfScore = 0;
    this.pageRankScore = 1;
  }
}
