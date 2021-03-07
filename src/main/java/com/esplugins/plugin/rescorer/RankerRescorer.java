package com.esplugins.plugin.rescorer;

import static java.util.Collections.singletonList;

import com.esplugins.plugin.models.FieldInfo;
import com.esplugins.plugin.models.Fields;
import com.esplugins.plugin.models.RankerContext;
import com.esplugins.plugin.models.Source;
import com.esplugins.plugin.rescorer.utils.CommonUtils;
import com.esplugins.plugin.rescorer.utils.FieldUtils;
import com.google.common.collect.Maps;
import io.appform.functionmetrics.MonitoredFunction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;

public class RankerRescorer extends AbstractLifecycleComponent implements Rescorer {

  private static RankerRescorer INSTANCE;

  private TransportMultiGetAction transportMultiGetAction;
  private static final Logger log = LogManager.getLogger(RankerRescorer.class.getName());


  @Inject
  public RankerRescorer(Settings settings, TransportMultiGetAction multiGetAction) {
    INSTANCE = this;
    transportMultiGetAction = multiGetAction;
  }


  public static Rescorer getInStance() {
    return INSTANCE;
  }

  @MonitoredFunction
  @Override
  public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext)
      throws IOException {
    RankerContext rankerContext = (RankerContext) rescoreContext;
    int windowSize = Math.min(topDocs.scoreDocs.length, rankerContext.getWindowSize());
    if (!rankerContext.isRankingEnable() || windowSize <= 0) {
      return topDocs;
    }
    List<ScoreDoc> scoreDocs = new ArrayList<>();

    for (int index = 0; index < windowSize; index++) {
      scoreDocs.add(topDocs.scoreDocs[index]);
    }
    try {
      Map<String, Map<String, Float>> scoreMap = Maps.newHashMap();
      Map<Integer, String> idToEntityIdMap = getFieldFromPrimaryIndex(rankerContext.getFields(),
          rankerContext.getQueryShardContext(),
          scoreDocs,
          searcher,
          scoreMap);
      List<String> entityIds = new ArrayList<>(idToEntityIdMap.values());

      getFieldFromSecondaryIndex(rankerContext.getFields(), entityIds, scoreMap);
      getFieldScoreFromRequest(rankerContext.getFields(), entityIds, scoreMap,
          rankerContext.getData());
      computeFinalScore(idToEntityIdMap,scoreMap,scoreDocs,rankerContext.getFields().getFieldInfos());
    }catch (Exception e){
      log.error("excption occured while rescoring ",e);
    }

    Arrays.sort(topDocs.scoreDocs, (a, b) -> {
      if (a.score > b.score) {
        return -1;
      }
      if (a.score < b.score) {
        return 1;
      }
      // Safe because doc ids >= 0
      return a.doc - b.doc;
    });
    return topDocs;
  }

  @Override
  protected void doClose() throws IOException {

  }

  @Override
  protected void doStart() {

  }

  @Override
  protected void doStop() {

  }

  @Override
  public Explanation explain(int topLevelDocId, IndexSearcher searcher,
      RescoreContext rescoreContext,
      Explanation sourceExplanation) throws IOException {
    RankerContext context = (RankerContext) rescoreContext;
    return Explanation.match(context.getWindowSize(), "Window size",
        singletonList(sourceExplanation));
  }

  private void computeFinalScore(Map<Integer, String> idToEntityIdMap,
      Map<String, Map<String, Float>> scoreMap, List<ScoreDoc> scoreDocs,
      List<FieldInfo> fieldInfos) {
    for (ScoreDoc scoreDoc : scoreDocs) {
      String entityId = idToEntityIdMap.get(scoreDoc.doc);
      if (entityId != null) {
        float weight = 1000;
        for (FieldInfo fieldInfo : fieldInfos) {
          Map<String, Float> fieldScore = scoreMap.getOrDefault(
              CommonUtils.concat(fieldInfo.getIdIdentifiers(), entityId), Maps.newHashMap());
          weight = weight + fieldInfo.getWeight() * fieldScore.getOrDefault(
              CommonUtils.concat(fieldInfo.getName(), fieldInfo.getSource().name()),
              fieldInfo.getDefaultValue());
        }
      } else {
        log.error("Id does not found for {}", scoreDoc.doc);
      }
    }

  }


  @MonitoredFunction
  private void getFieldScoreFromRequest(Fields fields, List<String> entityIds,
      Map<String, Map<String, Float>> scoreMap, Map<String, Object> data) {
    List<FieldInfo> fieldInfos = FieldUtils
        .filterFieldInfoOnSource(Source.REQUEST, fields.getFieldInfos());
    if (data == null || data.isEmpty() || CommonUtils.isEmpty(fieldInfos)) {
      return;
    }

    Map<String, Map<String, Float>> entityToScores = (Map<String, Map<String, Float>>) data
        .get("scores");
    if (entityToScores == null || entityToScores.isEmpty()) {
      return;
    }
    for (FieldInfo fieldInfo : fieldInfos) {
      for (String entityId : entityIds) {
        String uniqueId = CommonUtils.concat(fieldInfo.getIdIdentifiers(), entityId);
        Map<String, Float> fieldScore = entityToScores.get(uniqueId);
        if (fieldScore != null) {
          Map<String, Float> fieldScoreMap = scoreMap.getOrDefault(uniqueId, Maps.newHashMap());
          fieldScoreMap.put(CommonUtils.concat(fieldInfo.getName(), Source.REQUEST.name()),
              fieldScore.get(fieldInfo.getName()));
          scoreMap.put(uniqueId, fieldScoreMap);
        }
      }
    }

  }

  @MonitoredFunction
  private void getFieldFromSecondaryIndex(Fields fields, List<String> entityIds,
      Map<String, Map<String, Float>> scoreMap) throws Exception {
    List<FieldInfo> fieldInfos = FieldUtils
        .filterFieldInfoOnSource(Source.SECONDARY_INDEX, fields.getFieldInfos());
    if (CommonUtils.isEmpty(fieldInfos)) {
      return;
    }
    MultiGetRequest multiGetRequest = new MultiGetRequest();
    for (String entityId : entityIds) {
      for (FieldInfo fieldInfo : fieldInfos) {
        MultiGetRequest.Item item = new Item(fieldInfo.getIndexName(),
            CommonUtils.concat(fieldInfo.getIdIdentifiers(), entityId));
        item.storedFields(fieldInfo.getName());
        item.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        item.versionType(VersionType.INTERNAL);
        multiGetRequest.add(item);
      }
    }
    multiGetRequest.preference("_local");
    CompletableFuture<MultiGetResponse> completableFuture = new CompletableFuture<>();
    ActionListener<MultiGetResponse> actionListener = ActionListener
        .wrap(completableFuture::complete, completableFuture::completeExceptionally);
    transportMultiGetAction.execute(multiGetRequest, actionListener);
    MultiGetResponse multiGetResponses = completableFuture.get(fields.getRequestTimeout(),
        TimeUnit.MILLISECONDS);
    MultiGetItemResponse[] multiGetItemResponses = multiGetResponses.getResponses();
    for (MultiGetItemResponse multiGetItemResponse : multiGetItemResponses) {
      populateScore(Source.SECONDARY_INDEX,
          multiGetItemResponse.getResponse().getId(),
          multiGetItemResponse.getResponse().getFields(),
          scoreMap);
    }

  }

  @MonitoredFunction
  private Map<Integer, String> getFieldFromPrimaryIndex(Fields fields,
      QueryShardContext queryShardContext,
      List<ScoreDoc> scoreDocs, IndexSearcher searcher, Map<String, Map<String, Float>> scoreMap)
      throws IOException {
    List<LeafReaderContext> leafReaderContexts = new ArrayList<>();
    leafReaderContexts.addAll(searcher.getTopReaderContext().leaves());
    DocValueReader docValueReader = new DocValueReader();
    List<FieldAndFormat> fieldAndFormats = getFieldFormat(FieldUtils.filterFieldInfoOnSource(
        Source.PRIMARY_INDEX,
        fields.getFieldInfos())
    );
    List<SearchHit> searchHits = scoreDocs.stream()
        .map(scoreDoc -> new SearchHit(scoreDoc.doc))
        .collect(Collectors.toList());

    docValueReader.hitsExecute(fieldAndFormats,
        searchHits.toArray(new SearchHit[0]),
        queryShardContext.getMapperService(),
        leafReaderContexts, queryShardContext.lookup().doc()
    );
    Map<Integer, String> idMap = Maps.newHashMap();

    for (int index = 0; index < searchHits.size(); index++) {
      Map<String, DocumentField> maps = searchHits.get(index).getFields();
      String uniqueIdentifier = maps.get("_id").getValue().toString();
      idMap.put(searchHits.get(index).docId(), uniqueIdentifier);
      populateScore(Source.PRIMARY_INDEX, uniqueIdentifier, maps, scoreMap);
    }
    return idMap;
  }

  private void populateScore(Source source, String uniqueIdentifier,
      Map<String, DocumentField> documentMap, Map<String, Map<String, Float>> scoreMap) {
    Map<String, Float> scores = scoreMap.getOrDefault(uniqueIdentifier, Maps.newHashMap());
    for (Map.Entry<String, DocumentField> entrySet : documentMap.entrySet()) {
      if (!"_id".equalsIgnoreCase(entrySet.getKey())) {
        scores.put(CommonUtils.concat(entrySet.getKey(), source.name()),
            entrySet.getValue().getValue());
        scoreMap.put(uniqueIdentifier, scores);
      }
    }
  }


  private List<FieldAndFormat> getFieldFormat(List<FieldInfo> fieldInfos) {
    List<FieldAndFormat> fieldAndFormats = fieldInfos.stream()
        .map(fieldInfo -> new FieldAndFormat(fieldInfo.getName(), null))
        .collect(Collectors.toList());
    fieldAndFormats.add(new FieldAndFormat("_id", null));
    return fieldAndFormats;
  }

}
