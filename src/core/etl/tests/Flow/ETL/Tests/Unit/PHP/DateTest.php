<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Value\Date;
use Flow\ETL\PHP\Value\Time\Precision;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class DateTest extends TestCase
{
    #[TestWith(['2023-10-33 12:00:00 Europe/Warsaw'])]
    #[TestWith(['not a date'])]
    public function test_creat_from_invalid_date_string(string $input) : void
    {
        $this->expectException(InvalidArgumentException::class);
        Date::fromString($input);
    }

    #[TestWith(['2023-10-31', '2023-10-31'])]
    #[TestWith(['2024-01-01 12:00:14 Europe/Warsaw', '2024-01-01'])]
    public function test_create_from_date_string(string $input, string $output) : void
    {
        $date = Date::fromString($input);

        self::assertEquals($output, (string) $date);
    }

    public function test_create_from_datetime() : void
    {
        $date = Date::fromDateTime(new \DateTimeImmutable('2023-10-31'));

        self::assertEquals('2023-10-31', (string) $date);
    }

    public function test_create_from_datetime_with_time() : void
    {
        $date = Date::fromDateTime(new \DateTimeImmutable('2023-10-31 12:00:00'));

        self::assertEquals('2023-10-31', (string) $date);
    }

    #[TestWith(['P1Y1M1D', '1971-02-02'])]
    #[TestWith(['P1Y1M1DT1H1M1S', '1971-02-02'])]
    #[TestWith(['P55Y', '2025-01-01'])]
    public function test_create_from_interval(string $interval, string $output) : void
    {
        $date = Date::fromInterval(new \DateInterval($interval));

        self::assertEquals($output, (string) $date);
    }

    public function test_create_from_microseconds_timestamp() : void
    {
        $date = Date::fromTimestamp(1674431200 * 1_000_000, Precision::MICROSECONDS);

        self::assertEquals('2023-01-22', (string) $date);
    }

    public function test_create_from_milliseconds_timestamp() : void
    {
        $date = Date::fromTimestamp(1674431200 * 1000, Precision::MILLISECONDS);

        self::assertEquals('2023-01-22', (string) $date);
    }

    public function test_create_from_timestamp() : void
    {
        $date = Date::fromTimestamp(1674431200);

        self::assertEquals('2023-01-22', (string) $date);
    }

    public function test_date_creation_from_components() : void
    {
        $date = Date::fromComponents(2021, 1, 1);

        self::assertEquals(18628, $date->days());
        self::assertEquals(2021, $date->year());
        self::assertEquals(1, $date->month());
        self::assertEquals(1, $date->day());
    }

    public function test_date_creation_from_days_since_epoch() : void
    {
        $date = new Date(18628);

        self::assertEquals(2021, $date->year());
        self::assertEquals(1, $date->month());
        self::assertEquals(1, $date->day());
    }

    public function test_day_retrieval() : void
    {
        $date = new Date(365); // "1971-01-01"

        self::assertEquals(1, $date->day());
    }

    public function test_days_since_epoch_retrieval() : void
    {
        $date = Date::fromComponents(2000, 1, 1);

        self::assertEquals(10957, $date->days()); // Days since epoch for "2000-01-01"
    }

    public function test_from_time_interval() : void
    {
        $date = Date::fromInterval(new \DateInterval('PT1H1M1S'));

        self::assertEquals('1970-01-01', (string) $date);
    }

    public function test_invalid_date_creation_throws_exception() : void
    {
        $this->expectException(InvalidArgumentException::class);
        Date::fromComponents(2021, 2, 30);
    }

    public function test_leap_year_date() : void
    {
        $date = Date::fromComponents(2020, 2, 29);

        self::assertEquals(2020, $date->year());
        self::assertEquals(2, $date->month());
        self::assertEquals(29, $date->day());
        self::assertEquals(18321, $date->days()); // Days since epoch for "2020-02-29"
    }

    public function test_month_retrieval() : void
    {
        $date = new Date(365); // One year after epoch, "1971-01-01"

        self::assertEquals(1, $date->month());
    }

    public function test_to_string_format() : void
    {
        $date = Date::fromComponents(2023, 10, 31);

        self::assertEquals('2023-10-31', (string) $date);
    }

    public function test_year_retrieval() : void
    {
        $date = new Date(0);

        self::assertEquals(1970, $date->year());
    }
}
