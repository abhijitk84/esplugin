package com.esplugins.plugin.rescorer;

import static org.elasticsearch.search.DocValueFormat.withNanosecondResolution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.plain.SortedNumericDVIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext.FieldAndFormat;
import org.elasticsearch.search.lookup.DocLookup;

public class DocValueReader {
  private static final String USE_DEFAULT_FORMAT = "use_field_mapping";
  private final Logger logger = LogManager.getLogger(getClass());


  public void hitsExecute(List<FieldAndFormat> fields, SearchHit[] hits,
      MapperService mapperService,
      List<LeafReaderContext> leafReaderContexts,
      DocLookup docLookup) throws IOException {

    if (fields == null) {
      return;
    }

    hits = hits.clone(); // don't modify the incoming hits
    Arrays.sort(hits, Comparator.comparingInt(SearchHit::docId));

    for (FieldAndFormat fieldAndFormat : fields) {
      String field = fieldAndFormat.field;
      MappedFieldType fieldType = mapperService.fullName(field);
      if (fieldType != null) {
        final IndexFieldData<?> indexFieldData = docLookup.getForField(fieldType);
        final boolean isNanosecond;
        if (indexFieldData instanceof IndexNumericFieldData) {
          isNanosecond = ((IndexNumericFieldData) indexFieldData).getNumericType() == NumericType.DATE_NANOSECONDS;
        } else {
          isNanosecond = false;
        }
        final DocValueFormat format;
        String formatDesc = fieldAndFormat.format;
        if (Objects.equals(formatDesc, USE_DEFAULT_FORMAT)) {
          formatDesc = null;
        }
        if (isNanosecond) {
          format = withNanosecondResolution(fieldType.docValueFormat(formatDesc, null));
        } else {
          format = fieldType.docValueFormat(formatDesc, null);
        }
        LeafReaderContext subReaderContext = null;
        AtomicFieldData data = null;
        SortedBinaryDocValues binaryValues = null; // binary / string / ip fields
        SortedNumericDocValues longValues = null; // int / date fields
        SortedNumericDoubleValues doubleValues = null; // floating-point fields
        for (SearchHit hit : hits) {
          // if the reader index has changed we need to get a new doc values reader instance
          if (subReaderContext == null || hit.docId() >= subReaderContext.docBase + subReaderContext.reader().maxDoc()) {
            int readerIndex = ReaderUtil
                .subIndex(hit.docId(), leafReaderContexts);
            subReaderContext = leafReaderContexts.get(readerIndex);
            data = indexFieldData.load(subReaderContext);
            if (indexFieldData instanceof IndexNumericFieldData) {
              NumericType numericType = ((IndexNumericFieldData) indexFieldData).getNumericType();
              if (numericType.isFloatingPoint()) {
                doubleValues = ((AtomicNumericFieldData) data).getDoubleValues();
              } else {
                // by default nanoseconds are cut to milliseconds within aggregations
                // however for doc value fields we need the original nanosecond longs
                if (isNanosecond) {
                  longValues = ((SortedNumericDVIndexFieldData.NanoSecondFieldData) data).getLongValuesAsNanos();
                } else {
                  longValues = ((AtomicNumericFieldData) data).getLongValues();
                }
              }
            } else {
              data = indexFieldData.load(subReaderContext);
              binaryValues = data.getBytesValues();
            }
          }
          if (hit.fieldsOrNull() == null) {
            hit.fields(new HashMap<>(2));
          }
          DocumentField hitField = hit.getFields().get(field);
          if (hitField == null) {
            hitField = new DocumentField(field, new ArrayList<>(2));
            hit.getFields().put(field, hitField);
          }
          final List<Object> values = hitField.getValues();

          int subDocId = hit.docId() - subReaderContext.docBase;
          if (binaryValues != null) {
            if (binaryValues.advanceExact(subDocId)) {
              for (int i = 0, count = binaryValues.docValueCount(); i < count; ++i) {
                values.add(format.format(binaryValues.nextValue()));
              }
            }
          } else if (longValues != null) {
            if (longValues.advanceExact(subDocId)) {
              for (int i = 0, count = longValues.docValueCount(); i < count; ++i) {
                values.add(format.format(longValues.nextValue()));
              }
            }
          } else if (doubleValues != null) {
            if (doubleValues.advanceExact(subDocId)) {
              for (int i = 0, count = doubleValues.docValueCount(); i < count; ++i) {
                values.add(format.format(doubleValues.nextValue()));
              }
            }
          } else {
            throw new AssertionError("Unreachable code");
          }
        }
      }
    }
  }
}
