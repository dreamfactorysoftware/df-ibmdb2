<?php

namespace DreamFactory\Core\IbmDb2\Tests\Security;

use PHPUnit\Framework\TestCase;

/**
 * Security: IbmSchema must parameterize all schema/routine name lookups.
 *
 * Phase 2 audit found 4 SQL interpolation sites across 3 DB2 series
 * (ISERIES, LUW, ZOS) — getTableConstraints had `IN ('{\$schema}')`
 * variants on different catalog tables, and loadParameters had
 * routine name + schema interpolation in 3 case branches.
 *
 * Fix: parameterized ?-placeholder list for the IN clause and
 * :name/:schema named bindings for the routine lookups.
 */
class SchemaInterpolationTest extends TestCase
{
    private string $contents;

    protected function setUp(): void
    {
        $sourcePath = __DIR__ . '/../../src/Database/Schema/IbmSchema.php';
        $this->assertFileExists($sourcePath);
        $this->contents = file_get_contents($sourcePath);
    }

    public function testNoInClauseSchemaInterpolation(): void
    {
        $this->assertDoesNotMatchRegularExpression(
            "/IN\s*\(\s*'\{\\\$schema\}'\s*\)/",
            $this->contents,
            'No \$schema must be interpolated single-quoted into IN clauses'
        );
    }

    public function testNoRoutineNameInterpolation(): void
    {
        $this->assertDoesNotMatchRegularExpression(
            "/'\{?\\\$holder->resourceName\}?'/",
            $this->contents,
            '\$holder->resourceName must not be interpolated into SQL'
        );
        $this->assertDoesNotMatchRegularExpression(
            "/'\{?\\\$holder->schemaName\}?'/",
            $this->contents,
            '\$holder->schemaName must not be interpolated into SQL'
        );
    }

    public function testInClauseUsesPlaceholderArray(): void
    {
        $this->assertMatchesRegularExpression(
            '/array_fill\s*\(\s*0\s*,\s*count\s*\(\s*\$schemas\s*\)/',
            $this->contents,
            'IN clause must use array_fill ?-placeholder list'
        );
    }

    public function testRoutineLookupUsesNamedBindings(): void
    {
        $this->assertMatchesRegularExpression(
            '/:name\b/',
            $this->contents,
            ':name named placeholder must appear'
        );
        $this->assertMatchesRegularExpression(
            '/:schema\b/',
            $this->contents,
            ':schema named placeholder must appear'
        );
    }
}
