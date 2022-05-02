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
import com.google.common.io.ByteStreams;
import io.airlift.json.JsonCodec;
import io.trino.Session;
import io.trino.plugin.redis.util.CodecSupplier;
import io.trino.plugin.redis.util.RedisServer;
import io.trino.plugin.redis.util.RedisTestUtils;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.testing.MaterializedResult;
import io.trino.testing.StandaloneQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.UUID;

import static io.trino.plugin.redis.util.RedisTestUtils.createTableDescription;
import static io.trino.plugin.redis.util.RedisTestUtils.installRedisPlugin;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class TestMinimalFunctionalityWithoutKeyPrefix
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("redis_without_key_prefix")
            .setSchema("default")
            .build();

    private RedisServer redisServer;
    private String tableName;
    private String minimalTableName;
    private StandaloneQueryRunner queryRunner;

    @BeforeClass
    public void startRedis()
    {
        redisServer = new RedisServer();
    }

    @AfterClass(alwaysRun = true)
    public void stopRedis()
    {
        redisServer.close();
        redisServer = null;
    }

    @BeforeMethod
    public void spinUp()
            throws Exception
    {
        this.tableName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");

        this.queryRunner = new StandaloneQueryRunner(SESSION);

        RedisTableDescription description;
        JsonCodec<RedisTableDescription> tableDescriptionJsonCodec = new CodecSupplier<>(RedisTableDescription.class, this.queryRunner.getTypeManager()).get();
        try (InputStream data = RedisTestUtils.class.getResourceAsStream("/read_test/minimal_table_desc.json")) {
            description = tableDescriptionJsonCodec.fromJson(ByteStreams.toByteArray(data));
        }
        this.minimalTableName = description.getTableName();

        installRedisPlugin(redisServer, queryRunner,
                ImmutableMap.<SchemaTableName, RedisTableDescription>builder()
                        .put(createTableDescription(new SchemaTableName("default", tableName), null, null))
                        .put(createTableDescription(new SchemaTableName(description.getSchemaName(), description.getTableName()), description.getKey(), description.getValue()))
                        .buildOrThrow(),
                "redis_without_key_prefix", "false");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testWhereClauseHasData()
    {
        MaterializedResult result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key in ('%s:0', '%s:999')", minimalTableName, minimalTableName, tableName));

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(2L)
                .build();

        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key = '%s:999'", minimalTableName, tableName));

        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();

        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key in ('%s:0', '%s:999')", minimalTableName, tableName, tableName));

        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(2L)
                .build();

        assertEquals(result, expected);
    }
}
