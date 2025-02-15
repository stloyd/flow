<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class PregMatch implements ScalarFunction
{
    public function __construct(
        private readonly ScalarFunction $pattern,
        private readonly ScalarFunction $subject
    ) {
    }

    public function eval(Row $row) : ?bool
    {
        /** @var array<array-key, non-empty-string>|non-empty-string $pattern */
        $pattern = $this->pattern->eval($row);
        $subject = $this->subject->eval($row);

        if (!\is_string($pattern) || !\is_string($subject)) {
            return null;
        }

        return \preg_match($pattern, $subject) === 1;
    }
}
