<?php

declare(strict_types=1);

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\array_expand;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../bootstrap.php';

$df = df()
    ->read(from_rows(rows(
        row(int_entry('id', 1), array_entry('array', ['a' => 1, 'b' => 2, 'c' => 3])),
    )))
    ->write(to_output(false))
    ->withEntry('expanded', array_expand(ref('array')))
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

$df->run();
