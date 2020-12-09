package com.esplugins.plugin.rescorer;

import com.esplugins.plugin.rescorer.utils.SecurityUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.search.rescore.RescorerBuilder;

import java.io.IOException;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Example rescorer that multiplies the score of the hit by some factor and doesn't resort them.
 */
public class ExampleRescoreBuilder extends RescorerBuilder<ExampleRescoreBuilder> {
  public static final String NAME = "example";

  private final float factor;
  private final String factorField;
  private final Map<String,Object> parameter;
  private static final Log logger = LogFactory.getLog(ExampleRescoreBuilder.class);

  public ExampleRescoreBuilder(float factor, @Nullable String factorField,@Nullable Map<String,Object> parameter) {
    System.out.println("Coming here1");
    this.factor = factor;
    this.factorField = factorField;
    this.parameter = parameter;
  }

  public ExampleRescoreBuilder(StreamInput in) throws IOException {
    super(in);
    factor = in.readFloat();
    factorField = in.readOptionalString();
    parameter = in.readMap();
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeFloat(factor);
    out.writeOptionalString(factorField);
    out.writeMap(parameter);
  }

  @Override
  public String getWriteableName() {
    return NAME;
  }

  @Override
  public RescorerBuilder<ExampleRescoreBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
    return this;
  }

  private static final ParseField FACTOR = new ParseField("factor");
  private static final ParseField FACTOR_FIELD = new ParseField("factor_field");
  private static final ParseField PARAMETER = new ParseField("parameters");
  @Override
  protected void doXContent(XContentBuilder builder, Params params) throws IOException {
    builder.field(FACTOR.getPreferredName(), factor);
    if (factorField != null) {
      builder.field(FACTOR_FIELD.getPreferredName(), factorField);
    }
    if(parameter != null){
      builder.field(PARAMETER.getPreferredName(),parameter);
    }
  }

  private static final ConstructingObjectParser<ExampleRescoreBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME,
      args -> new ExampleRescoreBuilder((float) args[0], (String) args[1],(Map<String,Object>)args[2]));
  static {
    PARSER.declareFloat(constructorArg(), FACTOR);
    PARSER.declareString(optionalConstructorArg(), FACTOR_FIELD);
    PARSER.declareField(optionalConstructorArg(), XContentParser::map, PARAMETER, ObjectParser.ValueType.OBJECT);

    // PARSER.declareField(optionalConstructorArg(),PARAMETER);
  }
  public static ExampleRescoreBuilder fromXContent(XContentParser parser) {
    return PARSER.apply(parser, null);
  }

  @Override
  public RescoreContext innerBuildContext(int windowSize, QueryShardContext context) throws IOException {
//
//    IndexFieldData<?> factorField =
//        this.factorField == null ? null : context.getForField(context.getFieldType(this.factorField));

    return new ExampleRescoreContext(windowSize, factor, null,parameter,context);
  }

  @Override
  public boolean equals(Object obj) {
    if (false == super.equals(obj)) {
      return false;
    }
    ExampleRescoreBuilder other = (ExampleRescoreBuilder) obj;
    return factor == other.factor
        && Objects.equals(factorField, other.factorField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), factor, factorField);
  }

  float factor() {
    return factor;
  }

  @Nullable
  String factorField() {
    return factorField;
  }

  @Nullable
  Map<String,Object> getParameter(){
    return parameter;
  }

  private static class ExampleRescoreContext extends RescoreContext {
    private final float factor;
    @Nullable
    private final IndexFieldData<?> factorField;
    private final Map<String,Object> paramters;
    private QueryShardContext queryShardContext;

    ExampleRescoreContext(int windowSize, float factor, @Nullable IndexFieldData<?> factorField,@Nullable Map<String,Object> paramters,QueryShardContext queryShardContext) {
      super(windowSize, ExampleRescorer.INSTANCE);
      this.factor = factor;
      this.factorField = factorField;
      this.paramters = paramters;
      this.queryShardContext = queryShardContext;
    }
  }

  public static class ExampleRescorer extends AbstractLifecycleComponent implements Rescorer {

    private static  ExampleRescorer INSTANCE;

    private TransportMultiGetAction transportMultiGetAction;

    @Inject
    public ExampleRescorer(Settings settings, TransportMultiGetAction multiGetAction){
      INSTANCE = this;
      transportMultiGetAction = multiGetAction;
      System.out.println("in rescorer construct");
    }


    @Override
    public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {
      logger.error("checking here7");
      ScoreDoc[] scoreDocs = topDocs.scoreDocs;
      List<String> ids = new ArrayList<>();
      Map<Integer,String> map = new HashMap<>();
      List<SearchHit> searchHits = new ArrayList<>();
      for(ScoreDoc scoreDoc : scoreDocs){
        Set<String> names =  new HashSet<>();
        names.add("id");
        Document document = searcher.doc(scoreDoc.doc,names);
        String id = new String(document.getField("id").binaryValue().bytes);
        ids.add(id);
        map.put(scoreDoc.doc,id);
        SearchHit searchHit = new SearchHit(scoreDoc.doc);
        searchHits.add(searchHit);
      }


      MultiGetRequest multiGetRequest = new MultiGetRequest();
      MultiGetRequest.Item item = new Item("discovery.inapp.appentity","treeboinapp");
      item.storedFields("lpopularity");
      item.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
      item.versionType(VersionType.INTERNAL);
      multiGetRequest.add(item);
      multiGetRequest.preference("_local");
     // CheckedConsumer checkedConsumer = Object::notify;
      CompletableFuture completableFuture = new CompletableFuture<MultiGetResponse>();
      ActionListener actionListener = ActionListener.wrap(completableFuture::complete,completableFuture::completeExceptionally);
      transportMultiGetAction.execute(multiGetRequest, actionListener);
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
        MultiGetResponse multiGetItemResponses = (MultiGetResponse)completableFuture.get();
        GetResponse getResponse =  multiGetItemResponses.getResponses()[0].getResponse();
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String,Float> map1 = SecurityUtils
            .doPrivilegedException(()->objectMapper.readValue(((BytesArray)getResponse.getField("lpopularity").getValue()).array(),
                new TypeReference<Map<String,Float>>(){}));
        System.out.println(map1.get("56000011213"));
          System.out.println(multiGetItemResponses.getResponses().length);
      }catch (Exception e){
        e.printStackTrace();
      }
      ExampleRescoreContext context = (ExampleRescoreContext) rescoreContext;
      QueryShardContext queryShardContext  = context.queryShardContext;
      MappedFieldType  mappedFieldType = queryShardContext.fieldMapper("score");
      FieldAndFormat fieldAndFormat = new FieldAndFormat("score",null);
      FieldAndFormat fieldAndFormat1 = new FieldAndFormat("_id",null);
      try{
        IndexSearcher.LeafSlice[] leafSlices = searcher.getSlices();
        List<LeafReaderContext> leafReaderContexts = new ArrayList<>();
        leafReaderContexts.addAll(searcher.getTopReaderContext().leaves());
         DocValueReader docValueReader = new DocValueReader();
         SearchHit[] searchHits1 = searchHits.toArray(new SearchHit[0]);
         List<FieldAndFormat> fieldAndFormats = new ArrayList<>();
         fieldAndFormats.add(fieldAndFormat);
         fieldAndFormats.add(fieldAndFormat1);
         docValueReader.hitsExecute(fieldAndFormats,
             searchHits1, queryShardContext.getMapperService(),leafReaderContexts,queryShardContext.lookup().doc());
         for(int i =0;i< searchHits1.length;i++){
           System.out.println(searchHits1[i].docId());
           Map<String, DocumentField> maps = searchHits1[i].getFields();
           System.out.println(maps.get("_id"));
           System.out.println(maps.get("score"));
         }
      }catch (Exception e){
          System.out.println("in docValue");
          e.printStackTrace();
      }
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
      ExampleRescoreContext context = (ExampleRescoreContext) rescoreContext;
      // Note that this is inaccurate because it ignores factor field
      return Explanation.match(context.factor, "test", singletonList(sourceExplanation));
    }

  }
}