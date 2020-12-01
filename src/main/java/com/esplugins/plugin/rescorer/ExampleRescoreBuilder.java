package com.esplugins.plugin.rescorer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
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

//    IndexFieldData<?> factorField =
//        this.factorField == null ? null : context.getForField(context.getFieldType(this.factorField));
    return new ExampleRescoreContext(windowSize, factor, null,parameter);
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

    ExampleRescoreContext(int windowSize, float factor, @Nullable IndexFieldData<?> factorField,@Nullable Map<String,Object> paramters) {
      super(windowSize, ExampleRescorer.INSTANCE);
      this.factor = factor;
      this.factorField = factorField;
      this.paramters = paramters;
    }
  }

  private static class ExampleRescorer implements Rescorer {

    private static final ExampleRescorer INSTANCE = new ExampleRescorer();

    @Override
      public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {
      logger.error("checking here7");
      ScoreDoc[] scoreDocs = topDocs.scoreDocs;
      List<String> ids = new ArrayList<>();
      Map<Integer,String> map = new HashMap<>();
      for(ScoreDoc scoreDoc : scoreDocs){
        Set<String> names =  new HashSet<>();
        names.add("id");
        Document document = searcher.doc(scoreDoc.doc,names);
        System.out.println(scoreDoc.score);
        String id = new String(document.getField("id").binaryValue().bytes);
        ids.add(id);
        map.put(scoreDoc.doc,id);
        System.out.println(new String(document.getField("id").binaryValue().bytes));
      }

      ExampleRescoreContext context = (ExampleRescoreContext) rescoreContext;
      System.out.println(context.paramters);
      Map<String,Map<String,Float>> scores = DiscoveryClient.getScore(ids);
      for(ScoreDoc scoreDoc: scoreDocs){
        Map<String,Float> score = scores.getOrDefault(map.getOrDefault(scoreDoc.doc,"MA"),new HashMap<>());
        scoreDoc.score = scoreDoc.score + score.getOrDefault("t",0f) ;
      }

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
    public Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext,
        Explanation sourceExplanation) throws IOException {
      ExampleRescoreContext context = (ExampleRescoreContext) rescoreContext;
      // Note that this is inaccurate because it ignores factor field
      return Explanation.match(context.factor, "test", singletonList(sourceExplanation));
    }

  }
}