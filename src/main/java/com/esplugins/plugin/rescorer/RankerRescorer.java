package com.esplugins.plugin.rescorer;

import static java.util.Collections.singletonList;

import com.esplugins.plugin.models.FieldInfo;
import com.esplugins.plugin.models.Fields;
import com.esplugins.plugin.models.RankerContext;
import com.esplugins.plugin.models.Source;
import com.esplugins.plugin.rescorer.utils.FieldUtils;
import com.esplugins.plugin.rescorer.utils.SecurityUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.functionmetrics.MonitoredFunction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;

public  class RankerRescorer extends AbstractLifecycleComponent implements Rescorer {

  private static RankerRescorer INSTANCE;

  private TransportMultiGetAction transportMultiGetAction;

  @Inject
  public RankerRescorer(Settings settings, TransportMultiGetAction multiGetAction){
    INSTANCE = this;
    transportMultiGetAction = multiGetAction;
  }

  @MonitoredFunction
  private void collectMetric(){
    System.out.println("cominng in collect metric");
  }


  public static Rescorer getInStance(){
    return INSTANCE;
  }

  @Override
  public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {
    RankerContext rankerContext = (RankerContext)rescoreContext;
    int windowSize = Math.min(topDocs.scoreDocs.length,rankerContext.getWindowSize());
    if(!rankerContext.isRankingEnable() || windowSize <= 0){
      return topDocs;
    }
    List<ScoreDoc> scoreDocs = new ArrayList<>();
    for(int index =0;index < windowSize;index++){
      scoreDocs.add(topDocs.scoreDocs[index]);
    }

    getFieldFromPrimaryIndex(rankerContext.getFields(),
        rankerContext.getQueryShardContext(),
        scoreDocs,
        searcher);

//    System.out.println("cjajkjd");
//  //  totalEventRateMeter.mark(101);
//    ScoreDoc[] scoreDocs = topDocs.scoreDocs;
//    List<String> ids = new ArrayList<>();
//    Map<Integer,String> map = new HashMap<>();
//    List<SearchHit> searchHits = new ArrayList<>();
//    for(ScoreDoc scoreDoc : scoreDocs){
//      Set<String> names =  new HashSet<>();
//      names.add("id");
//      Document document = searcher.doc(scoreDoc.doc,names);
//      String id = new String(document.getField("id").binaryValue().bytes);
//      ids.add(id);
//      map.put(scoreDoc.doc,id);
//      SearchHit searchHit = new SearchHit(scoreDoc.doc);
//      searchHits.add(searchHit);
//    }
//    collectMetric();
//
//    MultiGetRequest multiGetRequest = new MultiGetRequest();
//    MultiGetRequest.Item item = new Item("discovery.inapp.appentity","treeboinapp");
//    item.storedFields("lpopularity");
//    item.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
//    item.versionType(VersionType.INTERNAL);
//    multiGetRequest.add(item);
//    multiGetRequest.preference("_local");
//    // CheckedConsumer checkedConsumer = Object::notify;
//    CompletableFuture completableFuture = new CompletableFuture<MultiGetResponse>();
//    ActionListener actionListener = ActionListener.wrap(completableFuture::complete,completableFuture::completeExceptionally);
//    transportMultiGetAction.execute(multiGetRequest, actionListener);
//          new ActionListener<MultiGetResponse>() {
//        @Override
//        public void onResponse(MultiGetResponse multiGetItemResponses) {
//          GetResponse getResponse =  multiGetItemResponses.getResponses()[0].getResponse();
//          System.out.println(getResponse.getField("score"));
//          System.out.println(multiGetItemResponses.getResponses().length);
//        //  Future future = new Result(multiGetItemResponses);
//        }
//
//        @Override
//        public void onFailure(Exception e) {
//           e.printStackTrace();
//        }

    try {
        System.out.println(rankerContext.getWindowSize());
        System.out.println(rankerContext.isRankingEnable());
        System.out.println(rankerContext.getData());
        List<FieldInfo> fieldInfos = rankerContext.getFields().getFieldInfos();
        if(fieldInfos != null && !fieldInfos.isEmpty()){
          System.out.println(fieldInfos.get(0).getDefaultValue());
          System.out.println(fieldInfos.get(0).getSource());
          System.out.println(fieldInfos.get(0).getWeight());
          System.out.println(fieldInfos.get(0).getIdIdentifiers());
          System.out.println(fieldInfos.get(0).getIndexName());

        }
        System.out.println(rankerContext.getFields());
    }catch (Exception e){
      e.printStackTrace();
    }
//
//    try {
//      MultiGetResponse multiGetItemResponses = (MultiGetResponse)completableFuture.get();
//      GetResponse getResponse =  multiGetItemResponses.getResponses()[0].getResponse();
//      ObjectMapper objectMapper = new ObjectMapper();
//      Map<String,Float> map1 = SecurityUtils
//          .doPrivilegedException(()->objectMapper.readValue(((BytesArray)getResponse.getField("lpopularity").getValue()).array(),
//              new TypeReference<Map<String,Float>>(){}));
//      System.out.println(map1.get("56000011213"));
//      System.out.println(multiGetItemResponses.getResponses().length);
//    }catch (Exception e){
//      e.printStackTrace();
//    }
//    RankerContext context = (RankerContext) rescoreContext;
//    QueryShardContext queryShardContext  = context.queryShardContext;
//    MappedFieldType mappedFieldType = queryShardContext.fieldMapper("score");
//    FieldAndFormat fieldAndFormat = new FieldAndFormat("score",null);
//    FieldAndFormat fieldAndFormat1 = new FieldAndFormat("_id",null);
//    try{
//      IndexSearcher.LeafSlice[] leafSlices = searcher.getSlices();
//      List<LeafReaderContext> leafReaderContexts = new ArrayList<>();
//      leafReaderContexts.addAll(searcher.getTopReaderContext().leaves());
//      DocValueReader docValueReader = new DocValueReader();
//      SearchHit[] searchHits1 = searchHits.toArray( new SearchHit[0]);
//      List<FieldAndFormat> fieldAndFormats = new ArrayList<>();
//      fieldAndFormats.add(fieldAndFormat);
//      fieldAndFormats.add(fieldAndFormat1);
//      docValueReader.hitsExecute(fieldAndFormats,
//          searchHits1, queryShardContext.getMapperService(),leafReaderContexts,queryShardContext.lookup().doc());
//      for(int i =0;i< searchHits1.length;i++){
//        System.out.println(searchHits1[i].docId());
//        Map<String, DocumentField> maps = searchHits1[i].getFields();
//        System.out.println(maps.get("_id"));
//        System.out.println(maps.get("score"));
//      }
//    }catch (Exception e){
//      System.out.println("in docValue");
//      e.printStackTrace();
//    }
//      if (mappedFieldType != null){
//        DocValueFieldsContext
////      }
//      Map<String,Map<String,Float>> scores = DiscoveryClient.getScore(ids);
//      for(ScoreDoc scoreDoc: scoreDocs){
//        Map<String,Float> score = scores.getOrDefault(map.getOrDefault(scoreDoc.doc,"MA"),new HashMap<>());
//        scoreDoc.score = scoreDoc.score + score.getOrDefault("t",0f) ;
//      }


//      int end = Math.min(topDocs.scoreDocs.length, rescoreContext.getWindowSize());
//      for (int i = 0; i < end; i++) {
//        topDocs.scoreDocs[i].score *= context.factor;
//      }
//      if (context.factorField != null) {
//        /*
//         * Since this example looks up a single field value it should
//         * access them in docId order because that is the order in
//         * which they are stored on disk and we want reads to be
//         * forwards and close together if possible.
//         *
//         * If accessing multiple fields we'd be better off accessing
//         * them in (reader, field, docId) order because that is the
//         * order they are on disk.
//         */
//        ScoreDoc[] sortedByDocId = new ScoreDoc[topDocs.scoreDocs.length];
//        System.arraycopy(topDocs.scoreDocs, 0, sortedByDocId, 0, topDocs.scoreDocs.length);
//        Arrays.sort(sortedByDocId, (a, b) -> a.doc - b.doc); // Safe because doc ids >= 0
//        Iterator<LeafReaderContext> leaves = searcher.getIndexReader().leaves().iterator();
//        LeafReaderContext leaf = null;
//        SortedNumericDoubleValues data = null;
//        int endDoc = 0;
//        for (int i = 0; i < end; i++) {
//          if (topDocs.scoreDocs[i].doc >= endDoc) {
//            do {
//              leaf = leaves.next();
//              endDoc = leaf.docBase + leaf.reader().maxDoc();
//            } while (topDocs.scoreDocs[i].doc >= endDoc);
//            LeafFieldData fd = context.factorField.load(leaf);
//            if (false == (fd instanceof LeafNumericFieldData)) {
//              throw new IllegalArgumentException("[" + context.factorField.getFieldName() + "] is not a number");
//            }
//            data = ((LeafNumericFieldData) fd).getDoubleValues();
//          }
//          if (false == data.advanceExact(topDocs.scoreDocs[i].doc - leaf.docBase)) {
//            throw new IllegalArgumentException("document [" + topDocs.scoreDocs[i].doc
//                + "] does not have the field [" + context.factorField.getFieldName() + "]");
//          }
//          if (data.docValueCount() > 1) {
//            throw new IllegalArgumentException("document [" + topDocs.scoreDocs[i].doc
//                + "] has more than one value for [" + context.factorField.getFieldName() + "]");
//          }
//          topDocs.scoreDocs[i].score *= data.nextValue();
//        }
//      }
//      // Sort by score descending, then docID ascending, just like lucene's QueryRescorer
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
  public Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext,
      Explanation sourceExplanation) throws IOException {
    RankerContext context = (RankerContext) rescoreContext;
    return Explanation.match(context.getWindowSize(), "Window size",
        singletonList(sourceExplanation));
  }

  @MonitoredFunction
  private void getFieldFromPrimaryIndex(Fields fields,QueryShardContext queryShardContext,
      List<ScoreDoc> scoreDocs, IndexSearcher searcher) throws IOException{
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
          searchHits.toArray( new SearchHit[0]),
          queryShardContext.getMapperService(),
          leafReaderContexts,queryShardContext.lookup().doc()
      );

      for(int i =0;i< searchHits.size();i++){
        System.out.println(searchHits.get(i).docId());
        Map<String, DocumentField> maps = searchHits.get(i).getFields();
        System.out.println(maps.get("_id"));
        System.out.println(maps.get("score"));
      }
  }

  private List<FieldAndFormat> getFieldFormat(List<FieldInfo> fieldInfos){
     List<FieldAndFormat> fieldAndFormats = fieldInfos.stream()
        .map(fieldInfo -> new FieldAndFormat(fieldInfo.getName(), null))
        .collect(Collectors.toList());
     fieldAndFormats.add( new FieldAndFormat("_id",null));
     return fieldAndFormats;
  }

}
