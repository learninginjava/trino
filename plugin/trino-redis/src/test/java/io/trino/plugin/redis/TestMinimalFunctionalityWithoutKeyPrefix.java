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
import io.trino.plugin.redis.util.RedisServer;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.testing.MaterializedResult;
import io.trino.testing.StandaloneQueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.UUID;

import static io.trino.plugin.redis.util.RedisTestUtils.createTableDescription;
import static io.trino.plugin.redis.util.RedisTestUtils.installRedisPlugin;
import static io.trino.plugin.redis.util.RedisTestUtils.loadSimpleTableDescription;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestMinimalFunctionalityWithoutKeyPrefix
        extends AbstractTestMinimalFunctionality
{
    @Override
    @BeforeClass
    public void startRedis()
            throws Exception
    {
        redisServer = new RedisServer();

        queryRunner = new StandaloneQueryRunner(SESSION);

        tableName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");
        RedisTableDescription stringValueTableDescription = loadSimpleTableDescription(queryRunner, "string");
        RedisTableDescription hashValueTableDescription = loadSimpleTableDescription(queryRunner, "hash");
        stringValueTableName = stringValueTableDescription.getTableName();
        hashValueTableName = hashValueTableDescription.getTableName();

        installRedisPlugin(redisServer, queryRunner,
                ImmutableMap.<SchemaTableName, RedisTableDescription>builder()
                        .put(createTableDescription(new RedisTableDescription(tableName, "default", null, null)))
                        .put(createTableDescription(stringValueTableDescription))
                        .put(createTableDescription(hashValueTableDescription))
                        .buildOrThrow(),
                ImmutableMap.of("redis.key-prefix-schema-table", "false"));

        populateData(1000);
    }

    @Test
    public void testStringValueWhereClauseHasData()
    {
        MaterializedResult result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key in ('%s:0', '%s:999')", stringValueTableName, stringValueTableName, tableName));
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(2L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key = '%s:999'", stringValueTableName, tableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key in ('%s:0', '%s:999')", stringValueTableName, tableName, tableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(2L)
                .build();
        assertEquals(result, expected);
    }

    @Test
    public void testHashValueWhereClauseHasData()
    {
        MaterializedResult result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key in ('%s:0', '%s:999')", hashValueTableName, hashValueTableName, hashValueTableName));
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(2L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key = '%s:999'", hashValueTableName, hashValueTableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();
        assertEquals(result, expected);
    }

    @Test
    public void testHashValueWhereClauseHasNoData()
    {
        MaterializedResult result = queryRunner.execute(format("SELECT count(1) from %s WHERE redis_key = 'does_not_exist'", hashValueTableName));
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();
        assertEquals(result, expected);
    }

    @Test
    public void testHashValueWhereClauseError()
    {
        assertThatThrownBy(() -> queryRunner.execute(
                format("SELECT count(1) from %s WHERE redis_key = '%s:999'", hashValueTableName, stringValueTableName)))
                .hasMessageContaining("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
