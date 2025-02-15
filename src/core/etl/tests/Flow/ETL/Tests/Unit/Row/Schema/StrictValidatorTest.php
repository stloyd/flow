<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Row;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\StrictValidator;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class StrictValidatorTest extends TestCase
{
    public function test_rows_with_a_missing_entry() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::string('name'),
        );

        $this->assertFalse(
            (new StrictValidator())->isValid(
                new Rows(Row::create(int_entry('id', 1), str_entry('name', 'test'), bool_entry('active', true))),
                $schema
            )
        );
    }

    public function test_rows_with_all_entries_valid() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::string('name'),
            Schema\Definition::boolean('active'),
        );

        $this->assertTrue(
            (new StrictValidator())->isValid(
                new Rows(Row::create(int_entry('id', 1), str_entry('name', 'test'), bool_entry('active', true))),
                $schema
            )
        );
    }

    public function test_rows_with_an_extra_entry() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::string('name'),
            Schema\Definition::boolean('active'),
            Schema\Definition::array('tags'),
        );

        $this->assertFalse(
            (new StrictValidator())->isValid(
                new Rows(Row::create(int_entry('id', 1), str_entry('name', 'test'), bool_entry('active', true))),
                $schema
            )
        );
    }

    public function test_rows_with_single_invalid_entry() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::boolean('name'),
            Schema\Definition::boolean('active'),
        );

        $this->assertFalse(
            (new StrictValidator())->isValid(
                new Rows(Row::create(int_entry('id', 1), str_entry('name', 'test'), bool_entry('active', true))),
                $schema
            )
        );
    }

    public function test_rows_with_single_invalid_row() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::string('name'),
            Schema\Definition::boolean('active'),
        );

        $this->assertFalse(
            (new StrictValidator())->isValid(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('name', 'test'), bool_entry('active', true)),
                    Row::create(int_entry('id', 1), bool_entry('active', true))
                ),
                $schema
            )
        );
    }
}
