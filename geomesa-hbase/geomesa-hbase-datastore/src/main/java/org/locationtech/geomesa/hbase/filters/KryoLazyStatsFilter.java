package org.locationtech.geomesa.hbase.filters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.features.interop.SerializationOptions;
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.stats.KryoStatSerializer;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;

public class KryoLazyStatsFilter extends FilterBase {
    private String sftString = "stats:String,geom:Geometry";
    private SimpleFeatureType sft;
    private KryoStatSerializer serializer;

    private org.opengis.filter.Filter filter;
    private String filterString;

    public KryoLazyStatsFilter() {
//        System.out.println("JSFF init sft: " + sftString + " : filter: " + filterString);
//        this.sftString = sftString;
        configureSFT();
//        this.filterString = filterString;
//        configureFilter();

    }

    public KryoLazyStatsFilter(SimpleFeatureType sft, org.opengis.filter.Filter filter) {
//        System.out.println("Adding HBase Feature Filter with");
//        System.out.println("\tSFT: " + sft.toString());
//        System.out.println("\tFilter: " + ECQL.toCQL(filter));

        this.sft = sft;
        this.filter = filter;
        this.sftString = SimpleFeatureTypes.encodeType(sft, true);
        this.filterString = ECQL.toCQL(filter);
    }

    private void configureSFT() {
        sft = SimpleFeatureTypes.createType("stats:stats", sftString);
        serializer = new KryoStatSerializer(sft);
    }

//    private void configureFilter() {
//        if (filterString != null && filterString != "") {
//            try {
//                this.filter = ECQL.toFilter(this.filterString);
//            } catch (CQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        return null;
    }

    public static org.apache.hadoop.hbase.filter.Filter parseFrom(final byte [] pbBytes) throws DeserializationException {
        System.out.println("Creating Stats Filter with parseFrom!");

//        int sftLen =  Bytes.readAsInt(pbBytes, 0, 4);
//        String sftString = new String(Bytes.copy(pbBytes, 4, sftLen));
//
//        int filterLen = Bytes.readAsInt(pbBytes, sftLen + 4, 4);
//        String filterString = new String(Bytes.copy(pbBytes, sftLen + 8, filterLen));
//
//        return new JSimpleFeatureFilter(sftString, filterString);
        return new KryoLazyStatsFilter();
    }


}
