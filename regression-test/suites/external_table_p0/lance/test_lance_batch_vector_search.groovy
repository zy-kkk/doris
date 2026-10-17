// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_lance_batch_vector_search", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String catalog = "test_lance_batch_vector_search"
    String ip = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("iceberg_minio_port")
    sql "DROP CATALOG IF EXISTS `${catalog}`"
    try {
        sql """CREATE CATALOG `${catalog}` PROPERTIES (
            "type"="lance", "lance.catalog.type"="filesystem", "warehouse"="s3://warehouse/lance",
            "s3.endpoint"="http://${ip}:${port}", "s3.access_key"="admin", "s3.secret_key"="password",
            "s3.region"="us-east-1", "use_path_style"="true")"""
        sql "SET enable_file_scanner_v2=true"
        sql "SET enable_sql_cache=false"
        sql "SET enable_query_cache=false"
        // Resolve each carried fixture once, then pin every scan and TVF comparison to it.
        // The batch fixtures keep version 11: two index segments, an append and three deletes.
        def versions = [:]
        def versionOf = { String table ->
            if (!versions.containsKey(table)) {
                String plan = (sql "EXPLAIN SELECT row_id FROM ${table}").collect { it[0] }.join("\n")
                def matches = (plan =~ /\blanceVersion=(\d+)\b/).collect { it[1] }.unique()
                assertEquals(1, matches.size(), "${table}: expected one snapshot version in EXPLAIN")
                versions[table] = matches[0] as long
            }
            versions[table]
        }
        // A complete, unfiltered single-query ranking supplies both rank boundaries and
        // ID-to-distance ownership. Equal scores may exchange IDs only at tied boundaries.
        def assertWindow = { List ranking, List rows, int offset, int k ->
            assertEquals(ranking.size(), ranking.collect { it[0] }.unique().size())
            assertEquals(Math.min(k, Math.max(0, ranking.size() - offset)), rows.size())
            assertEquals(rows.size(), rows.collect { it[0] }.unique().size())
            def distances = ranking.collectEntries { [(it[0]): it[1]] }
            assertEquals(ranking.drop(offset).take(k).collect { it[1] }.sort(),
                    rows.collect { it[1] }.sort())
            rows.each { row ->
                assertTrue(distances.containsKey(row[0]), "unknown candidate ID ${row[0]}")
                assertEquals(distances[row[0]], row[1], "distance belongs to another ID: ${row}")
            }
        }
        // The old distance-only check accepts this swapped-ID result. The ownership check must fail.
        boolean rejectedWrongDistance = false
        try {
            assertWindow([[1L, 0d], [2L, 1d]], [[1L, 1d]], 1, 1)
        } catch (AssertionError expected) {
            rejectedWrongDistance = true
        }
        assertTrue(rejectedWrongDistance)
        // A boundary tie remains legal, including when OFFSET cuts through the tie group.
        assertWindow([[1L, 0d], [2L, 0d], [3L, 1d]], [[1L, 0d]], 1, 1)
        def vector = { double start -> "[" + (0..<8).collect { start + it }.join(",") + "]" }
        def vectors = [vector(42.125), vector(1042.125), vector(2200.125), vector(42.125)]
        def search = { String table, String key, String value, int k, int offset, String options ->
            """vector_search('table'='${catalog}.`default`.${table}', 'column'='emb', 'version'='11',
                '${key}'='${value}', 'top_k'='${k}', 'offset'='${offset}' ${options})"""
        }
        def sqIds = (sql "SELECT id FROM ${catalog}.`default`.batch_search_hnsw FOR VERSION AS OF 11")
                .collect { it[0] }.sort()
        assertEquals(2301, sqIds.size())
        String sqOptions = ", 'nprobes'='4', 'ef'='2304'"
        def sqRankings = [:]
        vectors.unique(false).each { query ->
            def complete = search("batch_search_hnsw", "query_vector", query, 2301, 0, sqOptions)
            def ranking = sql "SELECT id,_distance FROM ${complete} ORDER BY _distance,id"
            assertEquals(sqIds, ranking.collect { it[0] }.sort())
            assertEquals(2301, ranking.size())
            sqRankings[query] = ranking
        }
        def compare = { String table, List queries, int offset, String options, String where ->
            String batch = search(table, "query_vectors", "[" + queries.join(",") + "]", 3, offset, options)
            def actual = sql "SELECT query_index,id,_distance,payload FROM ${batch} ${where} ORDER BY query_index,_distance,id"
            def expected = []
            queries.eachWithIndex { query, index ->
                def single = search(table, "query_vector", query, 3, offset, options)
                def rows = sql "SELECT id,_distance,payload FROM ${single} ${where} ORDER BY _distance,id"
                expected.addAll(rows.collect { [index] + it })
            }
            assertEquals(expected, actual)
            assertEquals(actual.collect { it[1] }.sort(),
                    (sql "SELECT id FROM ${batch} ${where}").collect { it[0] }.sort())
            assertEquals(actual.size(), (sql "SELECT COUNT(*) FROM ${batch} ${where}")[0][0] as int)
        }
        // Both materialization paths, exact IVF, flat, default probing and per-query fallbacks.
        for (boolean lazy : [false, true]) {
            sql "SET enable_lance_lazy_materialization=${lazy}"
            for (String table : ["batch_search", "batch_search_hnsw_flat"]) {
                for (String options : [", 'use_index'='false'", ", 'nprobes'='4'",
                        "", ", 'nprobes'='4', 'refine_factor'='2'"]) {
                    for (int offset : [0, 1]) {
                        compare(table, vectors, offset, options, "")
                    }
                    // Nine flat splits * two queries * 2304 candidates * refine=2 fit 100000.
                    compare(table, vectors.take(2), 2301, options, "")
                    compare(table, vectors, 1, options, "WHERE category=1")
                    for (boolean prefilter : [false, true]) {
                        compare(table, vectors, 1,
                                options + ", 'filter'='category=1', 'prefilter'='${prefilter}'", "")
                    }
                    compare(table, vectors, 0, options + ", 'filter'='id<0'", "")
                }
            }
            // SQ permits arbitrary tie order, but every returned ID must own its score in
            // the complete same-index ranking. A filter would change ANN candidates and
            // cannot supply this reference. Explicit ef covers every graph candidate here.
            for (int offset : [0, 1, 12]) {
                def batch = search("batch_search_hnsw", "query_vectors", "[" + vectors.join(",") + "]",
                        3, offset, sqOptions)
                def actual = sql "SELECT query_index,id,_distance,payload FROM ${batch}"
                assertEquals(12, actual.size())
                assertEquals(12, (sql "SELECT COUNT(*) FROM ${batch}")[0][0] as int)
                assertEquals((0..<vectors.size()).toList(), actual.collect { it[0] }.unique().sort())
                vectors.eachWithIndex { query, index ->
                    def single = search("batch_search_hnsw", "query_vector", query, 3, offset, sqOptions)
                    def expected = sql "SELECT id,_distance FROM ${single} ORDER BY _distance,id"
                    def group = actual.findAll { it[0] == index }
                    assertWindow(sqRankings[query], expected, offset, 3)
                    assertWindow(sqRankings[query], group.collect { [it[1], it[2]] }, offset, 3)
                    assertEquals(expected.collect { it[1] }, group.collect { it[2] }.sort())
                    group.each { row ->
                        assertFalse(row[1] in [42L, 1042L, 2200L])
                        assertEquals(("row-${row[1]}:" + "payload " * 128).toString(), row[3])
                    }
                }
            }
        }
        // An outer query and JOIN ON consume the completed window, including its OFFSET.
        // Filtering one retained ID before the window would discard it again at OFFSET 1.
        String boundaryBatch = search("batch_search", "query_vectors",
                "[" + vectors.take(2).join(",") + "]", 3, 1, ", 'use_index'='false'")
        for (boolean lazy : [false, true]) {
            sql "SET enable_lance_lazy_materialization=${lazy}"
            def window = sql "SELECT query_index,id,_distance FROM ${boundaryBatch} ORDER BY query_index,_distance,id"
            assertEquals(6, window.size())
            long retainedId = window[0][1] as long
            def retained = window.findAll { it[1] == retainedId }
            assertFalse(retained.isEmpty())
            assertEquals(retained, sql("""SELECT query_index,id,_distance FROM
                (SELECT * FROM ${boundaryBatch}) hits WHERE id=${retainedId}
                ORDER BY query_index,_distance,id"""))
            for (boolean materialize : [false, true]) {
                sql "SET enable_cte_materialize=${materialize}"
                assertEquals(retained, sql("""WITH hits AS (SELECT * FROM ${boundaryBatch})
                    SELECT query_index,id,_distance FROM hits WHERE id=${retainedId}
                    ORDER BY query_index,_distance,id"""))
                assertEquals(retained, sql("""WITH hits AS (SELECT * FROM ${boundaryBatch})
                    SELECT query_index,id,_distance FROM hits WHERE id=${retainedId}
                    UNION ALL SELECT query_index,id,_distance FROM hits WHERE id=-1
                    ORDER BY query_index,_distance,id"""))
            }
            sql "SET enable_cte_materialize=true"
            for (String join : ["LEFT JOIN", "INNER JOIN"]) {
                assertEquals(retained, sql("""SELECT hits.query_index,hits.id,hits._distance
                    FROM numbers('number'='1') n ${join} ${boundaryBatch} hits ON hits.id=${retainedId}
                    ORDER BY hits.query_index,hits._distance,hits.id"""))
                assertEquals(retained, sql("""SELECT hits.query_index,hits.id,hits._distance
                    FROM numbers('number'='1') n ${join} ${boundaryBatch} hits ON n.number=0
                    WHERE hits.id=${retainedId} ORDER BY hits.query_index,hits._distance,hits.id"""))
            }
            assertEquals([[null, null]], sql("""SELECT hits.query_index,hits.id
                FROM numbers('number'='1') n LEFT JOIN ${boundaryBatch} hits ON hits.id=-1"""))
            // Aliasing a direct TVF does not introduce a query block boundary.
            assertEquals([], sql("""SELECT query_index,id,_distance FROM ${boundaryBatch}
                WHERE id=${retainedId} ORDER BY query_index"""))
            assertEquals([], sql("""SELECT hits.query_index,hits.id,hits._distance FROM ${boundaryBatch} hits
                WHERE hits.id=${retainedId} ORDER BY hits.query_index"""))
        }
        test {
            sql """EXPLAIN SELECT n.number FROM numbers('number'='2') n WHERE EXISTS
                (SELECT 1 FROM ${boundaryBatch} hits WHERE hits.id=n.number)"""
            exception "Correlated predicates in a direct batch vector_search WHERE are not supported"
        }
        test {
            sql """EXPLAIN SELECT query_index,id FROM ${search("batch_search", "query_vectors",
                    "[" + vectors.join(",") + "]", 3, 2301, ", 'use_index'='false', 'refine_factor'='2'")}"""
            exception "100000 total candidate budget"
        }
        // Exact matches provide an independent oracle: deleted ID 42 leaves query 1 empty.
        for (String options : [", 'use_index'='false'", ", 'nprobes'='4'"]) {
            String sparseBatch = search("batch_search", "query_vectors",
                    "[" + [43d, 42d, 2201d, 43d].collect { vector(it) }.join(",") + "]", 3, 0, options)
            assertEquals([[0, 43L, 0f], [2, 2201L, 0f], [3, 43L, 0f]],
                    sql("SELECT query_index,id,_distance FROM ${sparseBatch} WHERE _distance=0 ORDER BY query_index"))
            if (options.contains("nprobes")) {
                explain {
                    sql "SELECT query_index,id FROM ${sparseBatch}"
                    contains "lanceSearchFragments=9"
                    contains "lanceSearchIndexSegments=2"
                    contains "lanceSearchIndexFragments=8"
                    contains "lanceSearchUnindexedFragments=1"
                }
            }
        }
        compare("batch_search", [vectors[0]], 1, ", 'nprobes'='4'", "")
        compare("batch_search", (0..<128).collect { vector(it + 0.125) }, 0, ", 'nprobes'='4'", "")
        // Exercise each encoded element type through BE, including nullable dataset vectors.
        long allTypesVersion = versionOf("${catalog}.`default`.all_types")
        for (def typeCase : [["float16", "[0.1,0.2,0.3,0.4]", "l2"],
                ["float32", "[0.1,0.2,0.3,0.4]", "l2"], ["float64", "[0.1,0.2,0.3,0.4]", "l2"],
                ["uint8", "[0,1,2,3]", "hamming"], ["int8", "[-1,0,1,2]", "l2"]]) {
            String properties = """'table'='${catalog}.`default`.all_types', 'version'='${allTypesVersion}',
                'column'='fixed_size_list_${typeCase[0]}_col', 'top_k'='3',
                'metric'='${typeCase[2]}', 'use_index'='false'"""
            def single = sql """SELECT row_id,_distance FROM vector_search(${properties},
                'query_vector'='${typeCase[1]}') ORDER BY _distance,row_id"""
            assertFalse(single.isEmpty())
            def actual = sql """SELECT query_index,row_id,_distance FROM vector_search(${properties},
                'query_vectors'='[${typeCase[1]},${typeCase[1]}]') ORDER BY query_index,_distance,row_id"""
            assertEquals([0, 1].collectMany { index -> single.collect { [index] + it } }, actual)
        }
        // Reuse seven columns of the existing 44-cell single-query fixture. This is a
        // representative batch matrix, not another type x metric x algorithm cross product.
        String matrixTable = "${catalog}.doris.vs_index_matrix"
        long matrixVersion = versionOf(matrixTable)
        def matrixIds = (sql "SELECT row_id FROM ${matrixTable} FOR VERSION AS OF ${matrixVersion}")
                .collect { it[0] }.sort()
        assertEquals((1L..64L).toList(), matrixIds)
        def columns = (sql "DESC ${matrixTable}").collect { it[0] }
        def cells = [
                [column: "emb__f32__cosine__ivf_hnsw_flat", quantized: false],
                [column: "emb__f32__dot__ivf_flat", quantized: false],
                [column: "emb__f32__cosine__ivf_sq", quantized: true],
                [column: "emb__f32__dot__ivf_hnsw_pq", quantized: true],
                [column: "emb__f16__l2__ivf_flat", quantized: false],
                [column: "emb__f64__cosine__ivf_flat", quantized: false],
                [column: "emb__u8__hamming__ivf_hnsw_flat", quantized: false],
        ]
        def matrixSearch = { Map cell, String key, String value, int k, int offset, String options ->
            """vector_search('table'='${matrixTable}', 'version'='${matrixVersion}',
                'column'='${cell.column}', 'metric'='${cell.metric}', 'nprobes'='4', 'use_index'='true',
                '${key}'='${value}', 'top_k'='${k}', 'offset'='${offset}'${options})"""
        }
        cells.each { cell ->
            assertTrue(columns.contains(cell.column), "missing existing matrix column ${cell.column}")
            def parts = cell.column.split("__")
            cell.metric = parts[2]
            boolean graph = parts[3].contains("hnsw")
            // Endpoints give the uint8 thermometer distinct distances rather than symmetric ties.
            def queries = [1, 64].collect { row ->
                def stored = sql """SELECT `${cell.column}` FROM ${matrixTable}
                    FOR VERSION AS OF ${matrixVersion} WHERE row_id=${row}"""
                assertEquals(1, stored.size())
                stored[0][0].toString().replaceAll("\\s+", "")
            }
            queries.add(queries[0])
            String matrix = "[" + queries.join(",") + "]"
            String options = graph ? ", 'ef'='64'" : ""
            if (parts[1] == "f64") {
                options += ", 'query_parallelism'='2'"
            }
            def variants = [options]
            if (cell.quantized) {
                // (top_k + offset) * 16 covers all 64 rows and forces per-query refinement.
                variants.add(", 'refine_factor'='16'" + (graph ? ", 'ef'='1024'" : ""))
            }
            variants.each { variant ->
                def rankings = queries.take(2).collect { query ->
                    def complete = matrixSearch(cell, "query_vector", query, 64, 0, variant)
                    def ranking = sql "SELECT row_id,_distance FROM ${complete} ORDER BY _distance,row_id"
                    assertEquals(matrixIds, ranking.collect { it[0] }.sort())
                    assertEquals(64, ranking.size())
                    ranking
                }
                rankings.add(rankings[0])
                String batchSearch = matrixSearch(cell, "query_vectors", matrix, 3, 1, variant)
                explain {
                    sql "SELECT query_index,row_id,_distance FROM ${batchSearch}"
                    contains "lanceBatchQueries=3"
                    contains "lanceVersion=${matrixVersion}"
                    contains "lanceMetric=${cell.metric}"
                    contains "lanceSearchUnindexedFragments=0"
                    contains "lanceSearchIndexSegments=1"
                }
                for (boolean lazy : [false, true]) {
                    sql "SET enable_lance_lazy_materialization=${lazy}"
                    def actual = sql """SELECT query_index,row_id,_distance FROM ${batchSearch}
                        ORDER BY query_index,_distance,row_id"""
                    assertEquals(9, actual.size(), "${cell.column}: expected three rows per query")
                    assertEquals(9, (sql "SELECT COUNT(*) FROM ${batchSearch}")[0][0] as int)
                    assertEquals([0, 1, 2], actual.collect { it[0] }.unique().sort())
                    queries.eachWithIndex { query, index ->
                        def singleSearch = matrixSearch(cell, "query_vector", query, 3, 1, variant)
                        def single = sql """SELECT row_id,_distance FROM ${singleSearch}
                            ORDER BY _distance,row_id"""
                        def group = actual.findAll { it[0] == index }.collect { [it[1], it[2]] }
                        assertWindow(rankings[index], single, 1, 3)
                        assertWindow(rankings[index], group, 1, 3)
                        // Quantized columns retain the same tie-aware contract after refinement.
                        if (!cell.quantized) {
                            assertEquals(single, group, "${cell.column}: batch differs from single")
                        }
                    }
                }
            }
            if (parts[1] == "u8") {
                test {
                    sql """SELECT row_id FROM ${matrixSearch(cell + [metric: "l2"],
                            "query_vectors", matrix, 3, 0, options)}"""
                    exception "does not support UInt8"
                }
            }
            if (parts[3] == "ivf_hnsw_pq") {
                test {
                    sql """SELECT row_id FROM ${matrixSearch(cell, "query_vectors", matrix, 3, 1,
                            ", 'refine_factor'='16', 'ef'='4'")}"""
                    exception "ef must be greater than or equal to k"
                }
            }
        }
        // Force the local prefilter to pass through. The global group window must remain exact.
        sql "SET partition_topn_max_partitions=1"
        sql "SET partition_topn_pre_partition_rows=100000"
        compare("batch_search", vectors, 1, ", 'nprobes'='4'", "")
        String batch = search("batch_search", "query_vectors", "[" + vectors.join(",") + "]", 3, 1, ", 'nprobes'='4'")
        explain {
            sql "SELECT id,payload FROM ${batch} WHERE category=1"
            contains "lanceBatchQueries=4"
            contains "partition offset: 1"
            contains "TWO_PHASE_GLOBAL_PTOPN"
            contains "TWO_PHASE_LOCAL_PTOPN"
            contains "query_index"
        }
        def full = sql "SELECT query_index,id,_distance FROM ${batch} ORDER BY id,query_index"
        assertEquals(full.take(2), sql("SELECT query_index,id,_distance FROM ${batch} ORDER BY id,query_index LIMIT 2"))
        // Exercise optimizer boundaries with both materialization paths. A LIMIT without
        // ORDER BY may select any subset, but must not reintroduce rows skipped by offset.
        for (boolean lazy : [false, true]) {
            sql "SET enable_lance_lazy_materialization=${lazy}"
            def complete = sql "SELECT query_index,id,_distance,payload FROM ${batch} ORDER BY query_index,_distance,id"
            assertEquals(12, complete.size())
            def limited = sql "SELECT query_index,id,_distance,payload FROM ${batch} LIMIT 2"
            assertEquals(2, limited.size())
            limited.each { assertTrue(complete.contains(it)) }
            assertEquals(complete, sql("""WITH candidates AS (
                SELECT query_index AS qi,id AS source_id,_distance AS distance,payload AS body FROM ${batch})
                SELECT qi,source_id,distance,body FROM candidates ORDER BY qi,distance,source_id"""))
            // Reusing a CTE exercises expression-id copying as well as generated-column preservation.
            assertEquals(complete, sql("""WITH candidates AS (
                SELECT query_index,id,_distance,payload FROM ${batch})
                SELECT a.query_index,a.id,a._distance,a.payload FROM candidates a JOIN candidates b
                ON a.query_index=b.query_index AND a.id=b.id ORDER BY a.query_index,a._distance,a.id"""))
            long selectedId = complete[0][1] as long
            def selected = complete.findAll { (it[1] as long) == selectedId }
            assertEquals(selected, sql("""SELECT a.query_index,a.id,a._distance,a.payload
                FROM ${batch} a JOIN (SELECT ${selectedId} AS id) b ON a.id=b.id
                ORDER BY a.query_index,a._distance,a.id"""))
        }
        String preparedUrl = getServerPrepareJdbcUrl(context.config.jdbcUrl, context.dbName) +
                "&emulateUnsupportedPstmts=false"
        connect(context.config.jdbcUser, context.config.jdbcPassword, preparedUrl) {
            sql "SET enable_file_scanner_v2=true"
            sql "SET enable_lance_lazy_materialization=true"
            sql "SET enable_sql_cache=false"
            sql "SET enable_query_cache=false"
            def prepared = prepareStatement("""SELECT query_index,id,_distance FROM vector_search(
                'table'='${catalog}.`default`.batch_search', 'column'='emb', 'version'='11', 'nprobes'='4',
                'query_vectors'=?, 'top_k'=?, 'offset'=?) ORDER BY query_index,_distance,id""")
            try {
                assertTrue(prepared instanceof com.mysql.cj.jdbc.ServerPreparedStatement)
                def checkPrepared = { List queries, int k, int offset ->
                    String matrix = "[" + queries.join(",") + "]"
                    prepared.setString(1, matrix)
                    prepared.setInt(2, k)
                    prepared.setInt(3, offset)
                    def literal = search("batch_search", "query_vectors", matrix, k, offset, ", 'nprobes'='4'")
                    assertEquals(sql("SELECT query_index,id,_distance FROM ${literal} ORDER BY query_index,_distance,id"),
                            exec(prepared))
                }
                checkPrepared([vectors[0]], 3, 1)
                checkPrepared(vectors, 3, 1)
                checkPrepared([vectors[1], vectors[0]], 2, 0)
                // Keep N unchanged while replacing the payload and result window.
                checkPrepared([vectors[2], vectors[1]], 4, 2)
                prepared.setString(1, "[[1]]")
                try {
                    exec(prepared)
                    assertTrue(false, "Expected a dimension error for the bound query matrix")
                } catch (java.sql.SQLException expected) {
                    assertTrue(expected.message.contains("dimension"), expected.message)
                }
                checkPrepared([vectors[0]], 3, 1)
            } finally {
                prepared.close()
            }
        }
        for (String options : [", 'distance_lower_bound'='0'", ", 'query_vector'='${vectors[0]}'"]) {
            test {
                sql "SELECT * FROM ${search('batch_search', 'query_vectors', '[' + vectors[0] + ']', 3, 0, options)}"
                exception (options.contains("distance") ? "Distance bounds" : "mutually exclusive")
            }
        }
        test {
            sql "SELECT * FROM ${search('batch_search', 'query_vectors', '[]', 3, 0, '')}"
            exception "1..128"
        }
        test {
            sql "SELECT * FROM ${search('batch_search', 'query_vectors', '[' + vectors.join(',') + ']', 30000, 0, '')}"
            exception "candidate budget"
        }
    } finally {
        sql "DROP CATALOG IF EXISTS `${catalog}`"
    }
}
