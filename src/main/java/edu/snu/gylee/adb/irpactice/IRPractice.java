package edu.snu.gylee.adb.irpactice;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;

import java.io.*;
import java.sql.*;
import java.util.*;

public class IRPractice {

  public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
    Class.forName("org.mariadb.jdbc.Driver");

    Connection connection = DriverManager.getConnection("jdbc:mariadb://147.46.15.238:3306/ADB-2015-21252",
        "ADB-2015-21252", "ADB-2015-21252");
    Statement statement = connection.createStatement();

    try {

//      statement.executeUpdate("DROP TABLE inverted_index");
      statement.executeUpdate("CREATE TABLE inverted_index(term varchar(1000), id int(11))");
      ResultSet wikiResultSet = statement.executeQuery("SELECT * from wiki");
      int batchSize = 5000;
      int batchCount = 0;
      PreparedStatement insertInvertedIndex = connection.prepareStatement("INSERT INTO inverted_index VALUES (?, ?)");
      while (wikiResultSet.next()) {
        int docId = wikiResultSet.getInt("id");
        String docText = wikiResultSet.getString("text");
        Set<String> wordSet = new HashSet<>();
        PTBTokenizer<CoreLabel> tokenizer = new PTBTokenizer<>(new StringReader(docText), new CoreLabelTokenFactory(),
            "");
        while (tokenizer.hasNext()) {
          wordSet.add(tokenizer.next().toString());
        }
        for (String word : wordSet) {
          insertInvertedIndex.setString(1, word);
          insertInvertedIndex.setInt(2, docId);
          insertInvertedIndex.addBatch();
          batchCount++;
          if (batchCount % batchSize == 0) {
            insertInvertedIndex.executeBatch();
            batchCount = 0;
          }
        }
      }
      insertInvertedIndex.executeBatch();
      insertInvertedIndex.close();

      // PageRank
      ResultSet allDocCount = statement.executeQuery("SELECT COUNT(*) from wiki");
      int allDocNum;
      if (allDocCount.next()) {
        allDocNum = allDocCount.getInt(1);
      } else {
        throw new IllegalStateException("Cannot get the count number of wiki table!");
      }
      ResultSet allDocId = statement.executeQuery("SELECT id from wiki");
      Map<Integer, Double> oldPageRankScore = new HashMap<>();
      Map<Integer, Double> newPageRankScore;
      Map<Integer, Set<Integer>> reverseAdjList = new HashMap<>();
      Map<Integer, Integer> outgoingDegree = new HashMap<>();
      while (allDocId.next()) {
        int docId = allDocId.getInt("id");
        oldPageRankScore.put(docId, 1. / (double) allDocNum);
        reverseAdjList.put(docId, new HashSet<>());
        outgoingDegree.put(docId, 0);
      }
      ResultSet allDocLinks = statement.executeQuery("SELECT * from link");
      while (allDocLinks.next()) {
        int id_from = allDocLinks.getInt("id_from");
        int id_to = allDocLinks.getInt("id_to");
        if (reverseAdjList.containsKey(id_to) && reverseAdjList.containsKey(id_from)) {
          reverseAdjList.get(id_to).add(id_from);
          outgoingDegree.replace(id_from, outgoingDegree.get(id_from) + 1);
        }
      }
      double delta;
      double epsilon = Math.exp(-8.);
      double jumpProb = 0.15;
      do {
        delta = 0;
        newPageRankScore = new HashMap<>();
        for (int id_to : reverseAdjList.keySet()) {
          double neighborScore = 0;
          for (int id_from : reverseAdjList.get(id_to)) {
            neighborScore += oldPageRankScore.get(id_from) * 1. / (double) outgoingDegree.get(id_from);
          }
          double calculatedPageRankScore = jumpProb / (double) allDocNum + (1. - jumpProb) * neighborScore;
          newPageRankScore.put(id_to, calculatedPageRankScore);
          delta += Math.abs(calculatedPageRankScore - oldPageRankScore.get(id_to));
        }
        oldPageRankScore = newPageRankScore;
      } while (delta >= epsilon);
      final Map<Integer, Double> pageRankScore = newPageRankScore;

      while (true) {
        System.out.print("ADB-2015-21252> ");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String query = br.readLine();
        String[] queryWords = query.toLowerCase().split(" ");
        List<SearchResult> searchResultList = new ArrayList<>();
        Map<Integer, Integer> docIdListIndexMap = new HashMap<>();
        // Calculating TF-IDF
        PreparedStatement countNumDocs = connection.prepareStatement(
            "SELECT COUNT(id) FROM inverted_index WHERE term = ?");
        PreparedStatement selectDocId = connection.prepareStatement(
            "SELECT id FROM inverted_index WHERE term = ?");
        PreparedStatement selectWikiText = connection.prepareStatement(
            "SELECT * from wiki WHERE id = ?");
        for (String queryWord : queryWords) {
          countNumDocs.setString(1, queryWord);
          ResultSet countRS = countNumDocs.executeQuery();
          countRS.next();
          int queryWordDocumentCount = countRS.getInt(1);
//          System.out.println(queryWordDocumentCount);
          selectDocId.setString(1, queryWord);
          ResultSet invertedIndexRS = selectDocId.executeQuery();
          while (invertedIndexRS.next()) {
            int docId = invertedIndexRS.getInt("id");
            // Calculate TF-IDF scores here.
            selectWikiText.setInt(1, docId);
            ResultSet wikiDocRS = selectWikiText.executeQuery();
            if (!wikiDocRS.next()) {
              throw new IllegalStateException(
                  "Cannot find a doc with the id in inverted_index! Integrity has been violated.");
            } else {
              String text = wikiDocRS.getString("text");
              PTBTokenizer<CoreLabel> tokenizer = new PTBTokenizer<>(new StringReader(text),
                  new CoreLabelTokenFactory(), "");
              int numberOfAllWords = 0;
              int numberOfQueryWord = 0;
              while (tokenizer.hasNext()) {
                String documentWord = tokenizer.next().toString().toLowerCase();
                if (documentWord.equals(queryWord)) {
                  numberOfQueryWord += 1;
                }
                numberOfAllWords += 1;
              }
              double wordTfIdfScore = Math.log(1 + (double) numberOfQueryWord / (double) numberOfAllWords) /
                  (double) queryWordDocumentCount;
              SearchResult searchResult;
              if (docIdListIndexMap.containsKey(docId)) {
                searchResult = searchResultList.get(docIdListIndexMap.get(docId));
              } else {
                searchResult = new SearchResult(docId, wikiDocRS.getString("title"));
                docIdListIndexMap.put(docId, searchResultList.size());
                searchResultList.add(searchResult);
              }
              searchResult.tfIdfScore += wordTfIdfScore;
            }
          }
        }
        searchResultList.sort((s, t) -> {
          int scoreCompare
            = Double.compare(t.tfIdfScore * pageRankScore.get(t.id), s.tfIdfScore * pageRankScore.get(s
              .id));
          if (scoreCompare != 0) {
            return scoreCompare;
          } else {
            return Integer.compare(s.id, t.id);
          }
        });
        for (int i = 0; i < Math.min(10, searchResultList.size()); i++) {
          System.out.println(String.format("%d, %s, %f, %f", searchResultList.get(i).id,
              searchResultList.get(i).title, searchResultList.get(i).tfIdfScore,
              pageRankScore.get(searchResultList.get(i).id)));
        }
        countNumDocs.close();
      }
    } catch (Exception e) {
      statement.close();
      connection.close();
      throw e;
    }
  }
}