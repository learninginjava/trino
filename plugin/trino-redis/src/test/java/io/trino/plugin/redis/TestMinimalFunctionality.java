/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.redis;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.plugin.redis.util.JsonEncoder;
import io.trino.plugin.redis.util.RedisServer;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.testing.MaterializedResult;
import io.trino.testing.StandaloneQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.trino.plugin.redis.util.RedisTestUtils.createTableDefinitions;
import static io.trino.plugin.redis.util.RedisTestUtils.installRedisPlugin;
import static io.trino.plugin.redis.util.RedisTestUtils.loadSimpleTableDescription;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("redis")
            .setSchema("default")
            .build();

    private RedisServer redisServer;
    private String tableName;
    private String stringValueTableName;
    private String hashValueTableName;
    private StandaloneQueryRunner queryRunner;

    @BeforeClass
    public void startRedis()
            throws Exception
    {
        redisServer = new RedisServer();
        tableName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");

        queryRunner = new StandaloneQueryRunner(SESSION);

        RedisTableDescription stringValueTableDesc = loadSimpleTableDescription(queryRunner, "string");
        RedisTableDescription hashValueTableDesc = loadSimpleTableDescription(queryRunner, "hash");
        stringValueTableName = stringValueTableDesc.getTableName();
        hashValueTableName = hashValueTableDesc.getTableName();

        installRedisPlugin(redisServer, queryRunner,
                ImmutableMap.<SchemaTableName, RedisTableDescription>builder()
                        .put(createTableDefinitions(new SchemaTableName("default", tableName), null, null))
                        .put(createTableDefinitions(new SchemaTableName("default", stringValueTableName), stringValueTableDesc.getKey(), stringValueTableDesc.getValue()))
                        .put(createTableDefinitions(new SchemaTableName("default", hashValueTableName), hashValueTableDesc.getKey(), hashValueTableDesc.getValue()))
                        .buildOrThrow(),
                "redis", "true");

        populateData(1000);
    }

    @AfterClass(alwaysRun = true)
    public void stopRedis()
    {
        clearData();

        queryRunner.close();
        queryRunner = null;

        redisServer.close();
        redisServer = null;
    }

    private void populateData(int count)
    {
        JsonEncoder jsonEncoder = new JsonEncoder();
        for (long i = 0; i < count; i++) {
            Object value = ImmutableMap.of("id", Long.toString(i), "value", UUID.randomUUID().toString());
            try (Jedis jedis = redisServer.getJedisPool().getResource()) {
                jedis.set(tableName + ":" + i, jsonEncoder.toString(value));
                jedis.set(stringValueTableName + ":" + i, jsonEncoder.toString(value));
                jedis.hmset(hashValueTableName + ":" + i, (Map<String, String>) value);
            }
        }
    }

    private void clearData()
    {
        try (Jedis jedis = redisServer.getJedisPool().getResource()) {
            jedis.flushAll();
        }
    }

    @Test
    public void testTableExists()
    {
        QualifiedObjectName name = new QualifiedObjectName("redis", "default", tableName);
        transaction(queryRunner.getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, session -> {
                    Optional<TableHandle> handle = queryRunner.getServer().getMetadata().getTableHandle(session, name);
                    assertTrue(handle.isPresent());
                });
    }

    @Test
    public void testTableHasData()
    {
        clearData();

        MaterializedResult result = queryRunner.execute("SELECT count(1) from " + tableName);
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();
        assertEquals(result, expected);

        int count = 1000;
        populateData(count);

        result = queryRunner.execute("SELECT count(1) from " + tableName);
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row((long) count)
                .build();
        assertEquals(result, expected);
    }

    @Test
    public void testStringValueWhereClauseHasData()
    {
        MaterializedResult result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key = '%s:999'", stringValueTableName, stringValueTableName));
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key in ('%s:0', '%s:999')", stringValueTableName, stringValueTableName, stringValueTableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(2L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key in ('%s:0', '%s:999')", stringValueTableName, stringValueTableName, tableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();
        assertEquals(result, expected);
    }

    @Test
    public void testHashValueWhereClauseHasData()
    {
        MaterializedResult result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key = '%s:999'", hashValueTableName, hashValueTableName));
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key in ('%s:0', '%s:999')", hashValueTableName, hashValueTableName, hashValueTableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(2L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key in ('%s:0', '%s:999')", hashValueTableName, hashValueTableName, tableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();
        assertEquals(result, expected);
    }

    @Test
    public void testStringValueWhereClauseHasNoData()
    {
        MaterializedResult result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key = '%s:999'", stringValueTableName, tableName));
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key in ('%s:0', '%s:999')", stringValueTableName, tableName, tableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();
        assertEquals(result, expected);
    }

    @Test
    public void testHashValueWhereClauseHasNoData()
    {
        MaterializedResult result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key = '%s:999'", hashValueTableName, tableName));
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key in ('%s:0', '%s:999')", hashValueTableName, tableName, tableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();
        assertEquals(result, expected);
    }
}
