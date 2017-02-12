+    val scan = new Scan(range._1, range._2)
+    val flist2 = if (filter != Filter.INCLUDE) {
+      println(s"Configuring the JSFF with ${ECQL.toCQL(filter)} for range ${new String(range._1)} - ${new String(range._2)}.")
+      val sftString = SimpleFeatureTypes.encodeType(encoder.sft)
+      val sff = new JSimpleFeatureFilter(sftString, ECQL.toCQL(filter))
+      scan.setFilter(sff)
+    }
